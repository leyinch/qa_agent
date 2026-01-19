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
def get_history_project_id():
    return settings.google_cloud_project

def get_history_table_fqn():
    return f"{get_history_project_id()}.qa_results.scd_test_history"


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
                self._client = bigquery.Client(project=get_history_project_id())
            except Exception as e:
                logger.error(f"Failed to initialize TestHistoryService client: {e}")
                raise
        return self._client

    def _ensure_table_exists(self):
        """Ensure the history table exists in BigQuery."""
        try:
            # Check dataset
            dataset_ref = f"{get_history_project_id()}.qa_results"
            try:
                self.client.get_dataset(dataset_ref)
            except Exception:
                logger.info(f"Creating dataset {dataset_ref}")
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = "US"
                self.client.create_dataset(dataset)
            
            # Check table
            try:
                self.client.get_table(get_history_table_fqn())
            except Exception:
                logger.info(f"Creating table {get_history_table_fqn()}")
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
                    bigquery.SchemaField("test_results", "JSON", mode="NULLABLE"),
                    bigquery.SchemaField("executed_by", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("metadata", "JSON", mode="NULLABLE")
                ]
                table = bigquery.Table(get_history_table_fqn(), schema=schema)
                table.partitioning_type = "DAY"
                table.time_partitioning = bigquery.TimePartitioning(field="execution_timestamp")
                table.clustering_fields = ["project_id", "target_table", "status"]
                
                self.client.create_table(table)
        except Exception as e:
            logger.warning(f"Failed to ensure history table exists: {e}")
    
    def _prepare_json_for_bq(self, data: Any) -> Any:
        """
        Recursively convert objects to BigQuery-compatible JSON formats.
        BigQuery JSON columns expect Python dicts/lists, but nested
        datetime/date objects must be converted to strings.
        """
        if isinstance(data, (datetime, date)):
            return data.isoformat()
        elif isinstance(data, dict):
            return {k: self._prepare_json_for_bq(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._prepare_json_for_bq(i) for i in data]
        return data

    def save_test_results(
        self,
        project_id: str,
        comparison_mode: str,
        test_results: Any,
        target_dataset: Optional[str] = None,
        target_table: Optional[str] = None,
        mapping_id: Optional[str] = None,
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

        row = {
            "execution_id": execution_id,
            "execution_timestamp": execution_timestamp,
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
            "test_results": self._prepare_json_for_bq(test_results),
            "executed_by": executed_by or "System",
            "metadata": self._prepare_json_for_bq(metadata) if metadata else None
        }
        
        # Insert into BigQuery
        errors = self.client.insert_rows_json(get_history_table_fqn(), [row])
        
        if errors:
            logger.error(f"BigQuery insertion errors: {errors}")
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
            "executed_by", "metadata"
        ]
        
        if execution_id:
            select_columns.append("test_results")
            
        columns_str = ", ".join(select_columns)
        
        query_parts = [f"SELECT {columns_str} FROM `{get_history_table_fqn()}` WHERE 1=1"]
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
        FROM `{get_history_table_fqn()}`
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
            query = f"TRUNCATE TABLE `{get_history_table_fqn()}`"
            job_config = bigquery.QueryJobConfig() # No params needed
            query_job = self.client.query(query, job_config=job_config)
            query_job.result()  # Wait for completion
            logger.info(f"Cleared history for project {project_id} by truncating table")
        except Exception as e:
            logger.error(f"Failed to clear history: {e}")
            raise
