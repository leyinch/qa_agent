"""
Test Results History Service

This module handles saving and retrieving test execution results 
from BigQuery for historical tracking and future alerting.
"""

from google.cloud import bigquery
from datetime import datetime, timedelta
import uuid
import json
from typing import List, Dict, Optional, Any

# Configuration - TODO: Move to environment variables
HISTORY_PROJECT_ID = "leyin-sandpit"
HISTORY_DATASET = "qa_agent_metadata"
HISTORY_TABLE = "test_results_history"
HISTORY_TABLE_FQN = f"{HISTORY_PROJECT_ID}.{HISTORY_DATASET}.{HISTORY_TABLE}"


class TestHistoryService:
    """Service for managing test execution history in BigQuery"""
    
    def __init__(self):
        self.client = bigquery.Client(project=HISTORY_PROJECT_ID)
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        """Ensure the history table exists in BigQuery."""
        try:
            # Check dataset
            dataset_ref = f"{HISTORY_PROJECT_ID}.{HISTORY_DATASET}"
            try:
                self.client.get_dataset(dataset_ref)
            except Exception:
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = "US"
                self.client.create_dataset(dataset)
            
            # Check table
            try:
                self.client.get_table(HISTORY_TABLE_FQN)
            except Exception:
                # Schema definition matching backend/create_history_table.sql
                schema = [
                    bigquery.SchemaField("execution_id", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("execution_timestamp", "TIMESTAMP", mode="REQUIRED"),
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
                
                self.client.create_table(table)
                print(f"Created history table {HISTORY_TABLE_FQN}")
        except Exception as e:
            print(f"Warning: Failed to ensure history table exists: {e}")
    
    def save_test_results(
        self,
        project_id: str,
        comparison_mode: str,
        test_results: List[Dict[str, Any]],
        target_dataset: Optional[str] = None,
        target_table: Optional[str] = None,
        mapping_id: Optional[str] = None,
        cron_schedule: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Save test execution results to BigQuery history table (one row per table).
        """
        execution_id = str(uuid.uuid4())
        execution_timestamp = datetime.utcnow()
        
        # Aggregate stats
        total_tests = len(test_results)
        passed_tests = len([t for t in test_results if t.get("status") == "PASS"])
        failed_tests = len([t for t in test_results if t.get("status") == "FAIL"])
        
        # Determine overall status
        status = "PASS"
        if any(t.get("status") == "ERROR" for t in test_results):
            status = "ERROR"
        elif failed_tests > 0:
            status = "FAIL"
            
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
            "error_message": next((t.get("error_message") for t in test_results if t.get("error_message")), None),
            "cron_schedule": cron_schedule,
            "test_results": json.dumps(test_results),
            "executed_by": None,  # TODO: Add auth
            "metadata": json.dumps(metadata) if metadata else None
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
        
        Returns:
            List of test execution records
        """
        query_parts = [f"SELECT * FROM `{HISTORY_TABLE_FQN}` WHERE 1=1"]
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
            params.append(bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", start_date))
        
        if end_date:
            query_parts.append("AND execution_timestamp <= @end_date")
            params.append(bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", end_date))
        
        query_parts.append("ORDER BY execution_timestamp DESC")
        query_parts.append(f"LIMIT {limit}")
        
        query = " ".join(query_parts)
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        
        query_job = self.client.query(query, job_config=job_config)
        results = query_job.result()
        
        return [dict(row) for row in results]
    
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
        start_date = datetime.utcnow() - timedelta(days=days_back)
        
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
                bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", start_date)
            ]
        )
        
        query_job = self.client.query(query, job_config=job_config)
        results = query_job.result()
        
        return [dict(row) for row in results]
