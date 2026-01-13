"""Service for managing Cloud Scheduler jobs."""
import json
from typing import Optional, Dict, Any
from google.cloud import scheduler_v1
from google.protobuf import field_mask_pb2
from app.config import settings
import logging

logger = logging.getLogger(__name__)

class SchedulerService:
    """Service to handle Cloud Scheduler job operations."""

    def __init__(self):
        self.client = scheduler_v1.CloudSchedulerClient()
        self.project = settings.google_cloud_project
        self.location = settings.scheduler_location
        self.parent = f"projects/{self.project}/locations/{self.location}"

    def _get_job_name(self, config_id: str) -> str:
        """Generate a unique job name for a configuration."""
        # Sanitize config_id to be a valid job name (letters, numbers, hyphens)
        sanitized_id = config_id.replace("_", "-").lower()
        return f"{self.parent}/jobs/qa-agent-{sanitized_id}"

    async def upsert_job(
        self,
        config_id: str,
        cron_schedule: str,
        target_dataset: str,
        target_table: str,
        config_dataset: str,
        config_table: str
    ) -> tuple[bool, str]:
        """Create or update a Cloud Scheduler job for the given config."""
        if not settings.cloud_run_url:
            msg = "CLOUD_RUN_URL not set. Cannot upsert scheduler job."
            logger.warning(msg)
            return False, msg

        job_name = self._get_job_name(config_id)
        
        # Define the HTTP target (Cloud Run endpoint)
        http_target = scheduler_v1.HttpTarget(
            uri=f"{settings.cloud_run_url}/api/run-scheduled-tests",
            http_method=scheduler_v1.HttpMethod.POST,
            body=json.dumps({
                "config_id": config_id,
                "project_id": self.project,
                "config_dataset": config_dataset,
                "config_table": config_table,
                "target_dataset": target_dataset,
                "target_table": target_table,
                "cron_schedule": cron_schedule
            }).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            oidc_token=scheduler_v1.OidcToken(
                service_account_email=f"{self.project}@appspot.gserviceaccount.com" # Default service account
            )
        )

        job = scheduler_v1.Job(
            name=job_name,
            schedule=cron_schedule,
            time_zone="UTC",
            http_target=http_target
        )

        try:
            # Check if job exists
            try:
                self.client.get_job(name=job_name)
                # If exists, update
                update_mask = field_mask_pb2.FieldMask(paths=["schedule", "http_target"])
                self.client.update_job(job=job, update_mask=update_mask)
                logger.info(f"Updated Cloud Scheduler job: {job_name}")
                return True, "Updated successfully"
            except Exception:
                # If not exists, create
                self.client.create_job(parent=self.parent, job=job)
                logger.info(f"Created Cloud Scheduler job: {job_name}")
                return True, "Created successfully"
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Failed to upsert Cloud Scheduler job {job_name}: {error_msg}")
            return False, error_msg

    async def delete_job(self, config_id: str) -> bool:
        """Delete a Cloud Scheduler job."""
        job_name = self._get_job_name(config_id)
        try:
            self.client.delete_job(name=job_name)
            logger.info(f"Deleted Cloud Scheduler job: {job_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete Cloud Scheduler job {job_name}: {str(e)}")
            return False

    async def sync_all_from_config(self) -> Dict[str, Any]:
        """
        Read all configurations from BigQuery and ensure Cloud Scheduler jobs exist.
        Returns a summary of the sync operation.
        """
        # Lazy import to avoid circular dependency
        from app.services.bigquery_service import bigquery_service
        
        summary = {"total": 0, "synced": 0, "failed": 0, "skipped": 0, "details": []}
        
        try:
            logger.info(f"Starting Cloud Scheduler sync for project {self.project}")
            
            # 1. Sync SCD Configs
            try:
                scd_configs = await bigquery_service.read_scd_config_table(
                    self.project, "transform_config", "scd_validation_config"
                )
                logger.info(f"Found {len(scd_configs)} SCD configurations")
                
                for config in scd_configs:
                    summary["total"] += 1
                    config_id = config.get('config_id')
                    cron = config.get('cron_schedule')
                    
                    if not cron:
                        summary["skipped"] += 1
                        continue
                    
                    success, message = await self.upsert_job(
                        config_id=config_id,
                        cron_schedule=cron,
                        target_dataset=config.get('target_dataset', ''),
                        target_table=config.get('target_table', ''),
                        config_dataset="transform_config",
                        config_table="scd_validation_config"
                    )
                    
                    if success:
                        summary["synced"] += 1
                        summary["details"].append(f"Synced SCD {config_id}: {message}")
                    else:
                        summary["failed"] += 1
                        summary["details"].append(f"Failed SCD {config_id}: {message}")
            except Exception as e:
                logger.error(f"Failed to read SCD configs: {e}")
                summary["details"].append(f"Error reading SCD configs: {str(e)}")

            # 2. Sync GCS Configs
            try:
                gcs_configs = await bigquery_service.read_config_table(
                    self.project, "transform_config", "data_load_config"
                )
                logger.info(f"Found {len(gcs_configs)} GCS configurations")
                
                for config in gcs_configs:
                    summary["total"] += 1
                    config_id = config.get('mapping_id') # GCS uses mapping_id
                    cron = config.get('cron_schedule')
                    
                    if not cron:
                        summary["skipped"] += 1
                        continue
                    
                    success, message = await self.upsert_job(
                        config_id=config_id,
                        cron_schedule=cron,
                        target_dataset=config.get('target_dataset', ''),
                        target_table=config.get('target_table', ''),
                        config_dataset="transform_config",
                        config_table="data_load_config"
                    )
                    
                    if success:
                        summary["synced"] += 1
                        summary["details"].append(f"Synced GCS {config_id}: {message}")
                    else:
                        summary["failed"] += 1
                        summary["details"].append(f"Failed GCS {config_id}: {message}")
            except Exception as e:
                logger.error(f"Failed to read GCS configs: {e}")
                summary["details"].append(f"Error reading GCS configs: {str(e)}")
                    
            logger.info(f"Scheduler sync complete. Summary: {summary}")
            return summary
        except Exception as e:
            logger.error(f"Fatal error during scheduler sync: {str(e)}")
            raise

scheduler_service = SchedulerService()
