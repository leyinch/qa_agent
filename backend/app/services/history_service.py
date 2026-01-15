"""
Test Results History Service

This module handles saving and retrieving test execution results 
from BigQuery for historical tracking and future alerting.
"""

from google.cloud import bigquery
from datetime import datetime, timedelta
import uuid
import json
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta, date
import pytz

from app.config import settings

logger = logging.getLogger(__name__)

# Configuration
HISTORY_PROJECT_ID = settings.google_cloud_project
HISTORY_DATASET = "qa_results"
HISTORY_TABLE = "scd_test_history"
HISTORY_TABLE_FQN = f"{HISTORY_PROJECT_ID}.{HISTORY_DATASET}.{HISTORY_TABLE}"


class TestHistoryService:
    """Service for managing test execution history in BigQuery"""
    
    
    def __init__(self):
        self._client = None
        self._table_checked = False

    @property
    def client(self):
        """Lazy load BigQuery client."""
        if not self._client:
            try:
                self._client = bigquery.Client(project=HISTORY_PROJECT_ID)
            except Exception as e:
                logger.error(f"Failed to initialize TestHistoryService client: {e}")
                raise
        return self._client

    def _ensure_table_exists(self):
        """Ensure the history table exists in BigQuery."""
        try:
            # Check dataset
            dataset_ref = f"{HISTORY_PROJECT_ID}.{HISTORY_DATASET}"
            try:
                self._client.get_dataset(dataset_ref)
            except Exception:
                logger.info(f"Creating dataset {dataset_ref}")
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = "US"
                self._client.create_dataset(dataset)
            
            # Check table
            try:
                self._client.get_table(HISTORY_TABLE_FQN)
            except Exception:
                logger.info(f"Creating table {HISTORY_TABLE_FQN}")
                # Schema definition matching backend/create_history_table.sql
                schema = [
                    bigquery.SchemaField("execution_id", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("execution_timestamp", "DATETIME", mode="REQUIRED"),
                    bigquery.SchemaField("project_id", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("comparison_mode", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("target_dataset", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("target_table", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("mapping_id", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("total_tests", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("passed_tests", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("failed_tests", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("cron_schedule", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("test_results", "JSON", mode="NULLABLE"),
                    bigquery.SchemaField("executed_by", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("metadata", "JSON", mode="NULLABLE")
                ]
                table = bigquery.Table(HISTORY_TABLE_FQN, schema=schema)
                table.partitioning_type = "DAY"
                table.time_partitioning = bigquery.TimePartitioning(field="execution_timestamp")
                table.clustering_fields = ["project_id", "target_table", "status"]
                
                self._client.create_table(table)
        except Exception as e:
            logger.warning(f"Failed to ensure history table exists: {e}")
    
    def save_test_results(
        self,
        project_id: str,
        comparison_mode: str,
        test_results: Any,
        target_dataset: Optional[str] = None,
        target_table: Optional[str] = None,
        mapping_id: Optional[str] = None,
        cron_schedule: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        executed_by: Optional[str] = None
    ) -> str:
        """
        Save test execution results to BigQuery history table (one row per table).
        """
        # Ensure table exists before writing
        self._ensure_table_exists()

        execution_id = str(uuid.uuid4())
        
        # Get execution timestamp in Melbourne time (wall-clock time)
        # We store as DATETIME (local time) so it appears correct in BigQuery
        tz = pytz.timezone('Australia/Melbourne')
        execution_timestamp = datetime.now(tz).replace(tzinfo=None)
        
        # Aggregate stats
        if isinstance(test_results, list):
            total_tests = len(test_results)
            passed_tests = len([t for t in test_results if t.get("status") == "PASS"])
            failed_tests = len([t for t in test_results if t.get("status") == "FAIL"])
            error_message = next((t.get("error_message") for t in test_results if t.get("error_message")), None)
            
            # Determine overall status
            status = "PASS"
            if any(t.get("status") == "ERROR" for t in test_results):
                status = "ERROR"
            elif failed_tests > 0:
                status = "FAIL"
        else:
            # Handle non-list results (e.g. schema validation dict)
            total_tests = 1
            status = metadata.get("status") if metadata else "PASS" # Default or from metadata
            passed_tests = 1 if status == "PASS" else 0
            failed_tests = 1 if status == "FAIL" else 0
            error_message = None

        def json_serial(obj):
            """JSON serializer for objects not serializable by default json code"""
            if isinstance(obj, (datetime, date)):
                return obj.isoformat()
            return str(obj)

        row = {
            "execution_id": execution_id,
            "execution_timestamp": execution_timestamp.isoformat(),
            "project_id": project_id,
            "comparison_mode": comparison_mode,
            "target_dataset": target_dataset,
            "target_table": target_table,
            "mapping_id": mapping_id,
            "status": status,
            "total_tests": total_tests,
            "passed_tests": passed_tests,
            "failed_tests": failed_tests,
            "error_message": error_message,
            "cron_schedule": cron_schedule,
            "test_results": json.dumps(test_results, default=json_serial),
            "executed_by": executed_by or "System",
            "metadata": json.dumps(metadata, default=json_serial) if metadata else None
        }
        
        # Insert into BigQuery
        errors = self.client.insert_rows_json(HISTORY_TABLE_FQN, [row])
        
        if errors:
            raise Exception(f"Failed to insert row into BigQuery: {errors}")
        
        return execution_id
    
    def get_test_history(
        self,
        project_id: Optional[str] = None,
        target_table: Optional[str] = None,
        execution_id: Optional[str] = None,
        test_status: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Query test execution history with filters.
        
        Args:
            project_id: Filter by project ID (optional)
            target_table: Filter by table name (optional)
            execution_id: Filter by execution ID (optional)
            test_status: Filter by status (PASS/FAIL/ERROR) (optional)
            start_date: Filter results after this date (optional)
            end_date: Filter results before this date (optional)
            limit: Maximum number of results to return
        """
        # Select columns based on whether we need full details (execution_id provided) or just summary
        select_columns = [
            "execution_id", "execution_timestamp", "project_id", "comparison_mode",
            "target_dataset", "target_table", "mapping_id", "status",
            "total_tests", "passed_tests", "failed_tests", "error_message",
            "cron_schedule", "executed_by", "metadata"
        ]
        
        if execution_id:
            select_columns.append("test_results")
            
        columns_str = ", ".join(select_columns)
        
        query_parts = [f"SELECT {columns_str} FROM `{HISTORY_TABLE_FQN}` WHERE 1=1"]
        params = []
        
        if project_id:
            query_parts.append("AND project_id = @project_id")
            params.append(bigquery.ScalarQueryParameter("project_id", "STRING", project_id))
        
        if target_table:
            query_parts.append("AND target_table = @target_table")
            params.append(bigquery.ScalarQueryParameter("target_table", "STRING", target_table))
            
        if execution_id:
            query_parts.append("AND execution_id = @execution_id")
            params.append(bigquery.ScalarQueryParameter("execution_id", "STRING", execution_id))
        
        if test_status:
            query_parts.append("AND test_status = @test_status")
            params.append(bigquery.ScalarQueryParameter("test_status", "STRING", test_status))
        
        if start_date:
            query_parts.append("AND execution_timestamp >= @start_date")
            params.append(bigquery.ScalarQueryParameter("start_date", "DATETIME", start_date))
        
        if end_date:
            query_parts.append("AND execution_timestamp <= @end_date")
            params.append(bigquery.ScalarQueryParameter("end_date", "DATETIME", end_date))
        
        query_parts.append("ORDER BY execution_timestamp DESC")
        query_parts.append(f"LIMIT {limit}")
        
        query = " ".join(query_parts)
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        
        try:
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()
            return [dict(row) for row in results]
        except Exception as e:
            if "Not found" in str(e):
                logger.info("History table not found, returning empty list")
                return []
            logger.error(f"Error querying history: {e}")
            raise
    
    def get_table_timeline(
        self,
        project_id: str,
        target_dataset: str,
        target_table: str,
        days_back: int = 30
    ) -> List[Dict[str, Any]]:
        """
        Get chronological test history for a specific table.
        
        Args:
            project_id: GCP project ID
            target_dataset: Dataset name
            target_table: Table name
            days_back: Number of days to look back
        
        Returns:
            Chronological list of test executions for the table
        """
        tz = pytz.timezone('Australia/Melbourne')
        start_date = datetime.now(tz).replace(tzinfo=None) - timedelta(days=days_back)
        
        query = f"""
        SELECT 
            execution_id,
            execution_timestamp,
            status,
            total_tests,
            passed_tests,
            failed_tests,
            error_message
        FROM `{HISTORY_TABLE_FQN}`
        WHERE project_id = @project_id
          AND target_dataset = @target_dataset
          AND target_table = @target_table
          AND execution_timestamp >= @start_date
        ORDER BY execution_timestamp DESC
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("project_id", "STRING", project_id),
                bigquery.ScalarQueryParameter("target_dataset", "STRING", target_dataset),
                bigquery.ScalarQueryParameter("target_table", "STRING", target_table),
                bigquery.ScalarQueryParameter("start_date", "DATETIME", start_date)
            ]
        )
        
        query_job = self.client.query(query, job_config=job_config)
        results = query_job.result()
        
        return [dict(row) for row in results]

    def clear_history(self, project_id: str) -> None:
        """
        Delete all execution history for a specific project.
        
        Args:
            project_id: Project ID to clear history for
        """
        try:
            # Use TRUNCATE TABLE as requested
            query = f"TRUNCATE TABLE `{HISTORY_TABLE_FQN}`"
            job_config = bigquery.QueryJobConfig() # No params needed
            query_job = self.client.query(query, job_config=job_config)
            query_job.result()  # Wait for completion
            logger.info(f"Cleared history for project {project_id} by truncating table")
        except Exception as e:
            logger.error(f"Failed to clear history: {e}")
            raise

    def migrate_to_datetime(self) -> Dict[str, Any]:
        """
        Migrate the history table schema from TIMESTAMP to DATETIME to support local time.
        """
        try:
            table = self.client.get_table(HISTORY_TABLE_FQN)
            ts_field = next((f for f in table.schema if f.name == "execution_timestamp"), None)
            
            if not ts_field:
                return {"status": "skipped", "reason": "execution_timestamp field not found"}
            
            if ts_field.field_type == 'DATETIME':
                return {"status": "skipped", "reason": "Already using DATETIME"}

            logger.info(f"Field is {ts_field.field_type}. Starting migration to DATETIME...")
            
            # 1. Create temp table with new schema
            new_schema = []
            for field in table.schema:
                if field.name == "execution_timestamp":
                    new_schema.append(bigquery.SchemaField("execution_timestamp", "DATETIME", mode=field.mode))
                else:
                    new_schema.append(field)
            
            new_table_ref = f"{HISTORY_TABLE_FQN}_new"
            new_table = bigquery.Table(new_table_ref, schema=new_schema)
            new_table.partitioning_type = "DAY"
            new_table.time_partitioning = bigquery.TimePartitioning(field="execution_timestamp")
            
            self.client.delete_table(new_table_ref, not_found_ok=True)
            self.client.create_table(new_table)
            
            # 2. Copy data with conversion
            cols = [f.name for f in new_schema]
            select_cols = []
            for col in cols:
                if col == "execution_timestamp":
                    # Convert UTC TIMESTAMP to Melbourne DATETIME
                    select_cols.append("DATETIME(execution_timestamp, 'Australia/Melbourne') as execution_timestamp")
                else:
                    select_cols.append(col)
                    
            query = f"""
            INSERT INTO `{new_table_ref}` ({', '.join(cols)})
            SELECT {', '.join(select_cols)}
            FROM `{HISTORY_TABLE_FQN}`
            """
            self.client.query(query).result()
            
            # Verify row counts
            old_count = list(self.client.query(f"SELECT count(*) as c FROM `{HISTORY_TABLE_FQN}`").result())[0].get('c')
            new_count = list(self.client.query(f"SELECT count(*) as c FROM `{new_table_ref}`").result())[0].get('c')
            
            if old_count != new_count:
                raise Exception(f"Migration mismatch: Old={old_count}, New={new_count}")

            # 3. Swap tables
            backup_table_ref = f"{HISTORY_TABLE_FQN}_backup"
            self.client.delete_table(backup_table_ref, not_found_ok=True)
            
            self.client.query(f"ALTER TABLE `{HISTORY_TABLE_FQN}` RENAME TO `{HISTORY_TABLE}_backup`").result()
            self.client.query(f"ALTER TABLE `{new_table_ref}` RENAME TO `{HISTORY_TABLE}`").result()
            
            return {"status": "success", "migrated_rows": new_count}
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            raise
