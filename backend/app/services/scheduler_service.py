"""Service for managing Cloud Scheduler jobs."""
import json
from typing import Optional, Dict, Any
from google.cloud import scheduler_v1
from google.protobuf import field_mask_pb2
from google.api_core import exceptions
from app.config import settings
import logging

logger = logging.getLogger(__name__)

class SchedulerService:
    """Service to handle Cloud Scheduler job operations."""

    def __init__(self):
        self._client = None
        self.project = settings.google_cloud_project
        self.location = settings.scheduler_location
        self.parent = f"projects/{self.project}/locations/{self.location}"

    @property
    def client(self):
        """Lazy load Cloud Scheduler client."""
        if not self._client:
            try:
                self._client = scheduler_v1.CloudSchedulerClient()
            except Exception as e:
                logger.warning(f"Failed to initialize SchedulerService (will retry on usage if needed): {e}")
                return None
        return self._client

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
            time_zone=settings.scheduler_timezone,
            http_target=http_target
        )

        try:
            # Check if job exists
            try:
                self.client.get_job(name=job_name)
                # If exists, update
                update_mask = field_mask_pb2.FieldMask(paths=["schedule", "time_zone", "http_target"])
                self.client.update_job(job=job, update_mask=update_mask)
                logger.info(f"Updated Cloud Scheduler job: {job_name}")
                return True, "Updated successfully"
            except exceptions.NotFound:
                # If job doesn't exist, create it
                try:
                    self.client.create_job(parent=self.parent, job=job)
                    logger.info(f"Created Cloud Scheduler job: {job_name}")
                    return True, "Created successfully"
                except exceptions.NotFound as e:
                    # This usually means the parent location doesn't exist
                    error_msg = f"Parent location not found. Make sure Cloud Scheduler is initialized in {self.location}. Original error: {str(e)}"
                    logger.error(f"Failed to create job {job_name}: {error_msg}")
                    return False, error_msg
            except Exception as e:
                # Other errors during get or update
                raise e
        except Exception as e:
            error_msg = str(e)
            if "404 Requested entity was not found" in error_msg:
                error_msg = f"Parent location not found. TIP: You must initialize a region by running 'gcloud app create --region=us-central' in your project even if not using App Engine. Original error: {error_msg}"
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

    async def ensure_master_sync_job(self) -> tuple[bool, str]:
        """Ensures the master-sync job exists. Self-healing."""
        if not settings.cloud_run_url:
            return False, "CLOUD_RUN_URL not set"

        job_id = "master-sync"
        job_name = self._get_job_name(job_id)
        cron = "0 * * * *" # Hourly
        
        # Target: Calls /api/sync-scheduler
        http_target = scheduler_v1.HttpTarget(
            uri=f"{settings.cloud_run_url}/api/sync-scheduler",
            http_method=scheduler_v1.HttpMethod.POST,
            body=json.dumps({"project_id": self.project}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            oidc_token=scheduler_v1.OidcToken(
                service_account_email=f"{self.project}@appspot.gserviceaccount.com",
                audience=f"{settings.cloud_run_url}/api/sync-scheduler"
            )
        )

        job = scheduler_v1.Job(
            name=job_name,
            schedule=cron,
            time_zone=settings.scheduler_timezone,
            http_target=http_target
        )

        try:
            try:
                self.client.get_job(name=job_name)
                # Exist: Update it to ensure settings are correct
                update_mask = field_mask_pb2.FieldMask(paths=["schedule", "time_zone", "http_target"])
                self.client.update_job(job=job, update_mask=update_mask)
                return True, "Master sync job updated"
            except exceptions.NotFound:
                # Missing: Create it
                self.client.create_job(parent=self.parent, job=job)
                return True, "Master sync job created"
        except Exception as e:
            logger.error(f"Failed to ensure master sync job: {e}")
            return False, str(e)

    async def sync_all_from_config(self) -> Dict[str, Any]:
        """
        Read all configurations from BigQuery and ensure Cloud Scheduler jobs exist.
        Also deletes jobs that are no longer in the configuration (Source of Truth).
        Returns a summary of the sync operation.
        """
        # Ensure the master sync job itself exists (Self-healing)
        await self.ensure_master_sync_job()

        # Lazy import to avoid circular dependency
        from app.services.bigquery_service import bigquery_service
        
        summary = {"total": 0, "synced": 0, "failed": 0, "skipped": 0, "deleted": 0, "details": []}
        
        try:
            logger.info(f"Starting Cloud Scheduler sync for project {self.project}")
            
            # 1. Fetch Active Configs from BigQuery
            valid_job_names = set()
            try:
                scd_configs = await bigquery_service.read_scd_config_table(
                    self.project, "config", "scd_validation_config"
                )
                logger.info(f"Found {len(scd_configs)} SCD configurations")
                
                # Sync Upserts
                for config in scd_configs:
                    summary["total"] += 1
                    config_id = config.get('config_id')
                    cron = config.get('cron_schedule')
                    
                    if not cron:
                        summary["skipped"] += 1
                        continue
                    
                    # Track valid job names for cleanup phase
                    valid_job_names.add(self._get_job_name(config_id))

                    success, message = await self.upsert_job(
                        config_id=config_id,
                        cron_schedule=cron,
                        target_dataset=config.get('target_dataset', ''),
                        target_table=config.get('target_table', ''),
                        config_dataset="config",
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

            # 2. Cleanup Obsolete Jobs
            try:
                # List all jobs in the location
                # valid_job_names contains full resource names: projects/.../locations/.../jobs/name
                all_jobs = self.client.list_jobs(parent=self.parent)
                
                for job in all_jobs:
                    # Check if it's a QA Agent job (managed by us)
                    if "/jobs/qa-agent-" in job.name:
                        # EXEMPTION: Do not delete the master sync job itself!
                        if "qa-agent-master-sync" in job.name:
                            continue
                            
                        if job.name not in valid_job_names:
                            logger.info(f"Found obsolete job: {job.name}. Deleting...")
                            try:
                                self.client.delete_job(name=job.name)
                                summary["deleted"] += 1
                                summary["details"].append(f"Deleted obsolete job: {job.name.split('/')[-1]}")
                            except Exception as del_err:
                                logger.error(f"Failed to delete {job.name}: {del_err}")
                                summary["details"].append(f"Failed to delete {job.name}: {str(del_err)}")
                                
            except Exception as cleanup_err:
                logger.error(f"Error during job cleanup: {cleanup_err}")
                summary["details"].append(f"Cleanup error: {str(cleanup_err)}")

            logger.info(f"Scheduler sync complete. Summary: {summary}")
            return summary
        except Exception as e:
            logger.error(f"Fatal error during scheduler sync: {str(e)}")
            raise

scheduler_service = SchedulerService()
