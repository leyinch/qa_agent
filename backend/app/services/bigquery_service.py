"""BigQuery service for database operations."""
from typing import List, Dict, Any, Optional
import json
import logging
import asyncio
import datetime
import pytz
from google.cloud import bigquery

logger = logging.getLogger(__name__)

class BigQueryService:
    """Service for BigQuery operations."""
    
    def __init__(self):
        """Initialize BigQuery service."""
        self._client = None

    @property
    def client(self):
        """Lazy load BigQuery client."""
        if not self._client:
            from google.auth.exceptions import DefaultCredentialsError
            try:
                self._client = bigquery.Client()
            except DefaultCredentialsError as e:
                logger.error("BigQuery Client initialization failed: Credentials not found.")
                raise ValueError(
                    "Google Cloud credentials not found. "
                    "Locally: Run 'gcloud auth application-default login'. "
                    "Docker: Ensure ${APPDATA}/gcloud is mounted or service-account.json is provided."
                ) from e
        return self._client
    
    async def get_table_metadata(
        self, 
        project_id: str, 
        dataset_id: str, 
        table_id: str
    ) -> Dict[str, Any]:
        """Get metadata for a BigQuery table."""
        try:
            table_ref = f"{project_id}.{dataset_id}.{table_id}"
            table = self.client.get_table(table_ref)
            
            return {
                "full_table_name": table_ref,
                "schema": {
                    "fields": [
                        {
                            "name": field.name,
                            "type": field.field_type,
                            "mode": field.mode
                        }
                        for field in table.schema
                    ]
                },
                "num_rows": table.num_rows,
                "created": table.created.isoformat() if table.created else None,
                "modified": table.modified.isoformat() if table.modified else None
            }
            
        except Exception as e:
            raise ValueError(
                f"Failed to get metadata for {project_id}.{dataset_id}.{table_id}: {str(e)}"
            )
    
    async def execute_query(self, query: str, job_config: Optional[bigquery.QueryJobConfig] = None) -> List[Dict[str, Any]]:
        """Execute a BigQuery SQL query."""
        try:
            # BigQuery's query().result() is blocking. Wrap it in to_thread for true concurrency.
            def _run():
                query_job = self.client.query(query, job_config=job_config)
                return [dict(row) for row in query_job.result()]
            
            results = await asyncio.to_thread(_run)
            
            # Convert to list of dicts and clean for JSON serialization
            return [self._clean_row(row) for row in results]
            
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise ValueError(f"Query execution failed: {str(e)}")

    def _clean_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Convert BigQuery types to JSON-serializable types."""
        from datetime import date, datetime, time
        from decimal import Decimal
        
        cleaned = {}
        for key, value in row.items():
            if isinstance(value, (datetime, date, time)):
                cleaned[key] = value.isoformat()
            elif isinstance(value, Decimal):
                cleaned[key] = float(value)
            elif isinstance(value, bytes):
                cleaned[key] = value.decode('utf-8', errors='replace')
            elif isinstance(value, dict):
                cleaned[key] = self._clean_row(value)
            elif isinstance(value, list):
                cleaned[key] = [
                    self._clean_row(v) if isinstance(v, dict) else 
                    (v.isoformat() if isinstance(v, (datetime, date, time)) else v) 
                    for v in value
                ]
            else:
                cleaned[key] = value
        return cleaned
    
    async def get_row_count(self, full_table_name: str) -> int:
        """Get row count for a table."""
        query = f"SELECT COUNT(*) as count FROM `{full_table_name}`"
        results = await self.execute_query(query)
        return int(results[0]['count'])
    
    async def get_sample_data(
        self, 
        full_table_name: str, 
        limit: int = 5
    ) -> List[Dict[str, Any]]:
        """Get sample data from a table."""
        query = f"SELECT * FROM `{full_table_name}` LIMIT {limit}"
        return await self.execute_query(query)
    
    async def get_tables_in_dataset(
        self, 
        project_id: str, 
        dataset_id: str
    ) -> List[str]:
        """Get list of tables in a dataset."""
        try:
            dataset_ref = f"{project_id}.{dataset_id}"
            dataset = self.client.get_dataset(dataset_ref)
            tables = self.client.list_tables(dataset)
            
            return [table.table_id for table in tables]
            
        except Exception as e:
            raise ValueError(
                f"Failed to list tables in {project_id}.{dataset_id}: {str(e)}"
            )
            
    # --- Config Table Methods (Test1 & Test3 Mapped) ---

    async def read_config_table(
        self, 
        project_id: str, 
        config_dataset: str, 
        config_table: str,
        filters: Dict[str, Any] = None
    ) -> List[Dict[str, Any]]:
        """Read mappings from config table (Test1 style with filters)."""
        # Ensure dataset and table exist
        try:
            self.client.get_table(f"{project_id}.{config_dataset}.{config_table}")
        except Exception as e:
            # Try to ensure/create if missing (Test3 logic)
            await self.ensure_config_tables(project_id, config_dataset)
            try:
                self.client.get_table(f"{project_id}.{config_dataset}.{config_table}")
            except Exception as e2:
                 error_msg = f"Config table '{config_dataset}.{config_table}' not found. {str(e2)}"
                 logger.error(error_msg)
                 raise ValueError(error_msg)

        # Build query
        query = f"""
            SELECT *
            FROM `{project_id}.{config_dataset}.{config_table}`
            WHERE is_active = true
        """
        
        # Add dynamic filters (Test1 feature)
        if filters:
            for key, value in filters.items():
                if isinstance(value, str):
                    query += f" AND {key} = '{value}'"
                elif isinstance(value, (int, float, bool)):
                     query += f" AND {key} = {value}"
                else:
                    query += f" AND {key} = '{str(value)}'"

        return await self.execute_query(query)

    # --- Test3 SCD Methods ---

    async def ensure_config_tables(
        self,
        project_id: str,
        config_dataset: str = "config"
    ) -> None:
        """Ensure all configuration tables exist (Test3)."""
        try:
            # 1. Ensure dataset exists
            dataset_ref = f"{project_id}.{config_dataset}"
            try:
                self.client.get_dataset(dataset_ref)
            except Exception:
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = "US"
                self.client.create_dataset(dataset)
                print(f"Created dataset: {config_dataset}")

            # 2. Ensure scd_validation_config exists
            scd_table = f"{dataset_ref}.scd_validation_config"
            try:
                self.client.get_table(scd_table)
            except Exception:
                schema = [
                    bigquery.SchemaField("config_id", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("target_dataset", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("target_table", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("scd_type", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("primary_keys", "STRING", mode="REPEATED"),
                    bigquery.SchemaField("surrogate_key", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("begin_date_column", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("end_date_column", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("active_flag_column", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("custom_tests", "JSON", mode="NULLABLE"),
                    bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
                    bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
                ]
                table = bigquery.Table(scd_table, schema=schema)
                self.client.create_table(table)
                print(f"Created table: scd_validation_config")

        except Exception as e:
            print(f"Error ensuring config tables: {str(e)}")

    async def read_scd_config_table(
        self, 
        project_id: str, 
        config_dataset: str, 
        config_table: str
    ) -> List[Dict[str, Any]]:
        """Read SCD validation configurations from config table."""
        await self.ensure_config_tables(project_id, config_dataset)
        query = f"""
            SELECT *
            FROM `{project_id}.{config_dataset}.{config_table}`
        """
        return await self.execute_query(query)

    async def get_scd_config_by_table(
        self,
        project_id: str,
        config_dataset: str,
        config_table: str,
        target_dataset: str,
        target_table: str
    ) -> Optional[Dict[str, Any]]:
        """Fetch a single SCD config by target dataset and table."""
        await self.ensure_config_tables(project_id, config_dataset)
        query = f"""
            SELECT *
            FROM `{project_id}.{config_dataset}.{config_table}`
            WHERE target_dataset = @target_dataset
              AND target_table = @target_table
            LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("target_dataset", "STRING", target_dataset),
                bigquery.ScalarQueryParameter("target_table", "STRING", target_table),
            ]
        )
        
        results = await self.execute_query(query, job_config)
        return results[0] if results else None

    async def insert_scd_config(
        self,
        project_id: str,
        config_dataset: str,
        config_table: str,
        config_data: Dict[str, Any]
    ) -> bool:
        """Insert a new SCD validation configuration into the config table."""
        try:
            full_table_name = f"{project_id}.{config_dataset}.{config_table}"
            
            # Prepare local timestamp (Melbourne time) for "wall clock" storage in UTC field
            melbourne_tz = pytz.timezone('Australia/Melbourne')
            current_ts_str = datetime.datetime.now(melbourne_tz).strftime('%Y-%m-%d %H:%M:%S')

            # Use MERGE to update if table exists, insert if not
            query = f"""
                MERGE `{full_table_name}` T
                USING (
                    SELECT 
                        @config_id as config_id, 
                        @target_dataset as target_dataset, 
                        @target_table as target_table,
                        @scd_type as scd_type,
                        @primary_keys as primary_keys,
                        @surrogate_key as surrogate_key,
                        @begin_date_column as begin_date_column,
                        @end_date_column as end_date_column,
                        @active_flag_column as active_flag_column,
                        @description as description,
                        SAFE.PARSE_JSON(@custom_tests) as custom_tests
                ) S
                ON T.target_dataset = S.target_dataset AND T.target_table = S.target_table
                WHEN MATCHED THEN
                    UPDATE SET 
                        updated_at = @current_timestamp
                WHEN NOT MATCHED THEN
                    INSERT (
                        config_id, target_dataset, target_table, scd_type, 
                        primary_keys, surrogate_key, begin_date_column, 
                        end_date_column, active_flag_column, description, 
                        custom_tests, created_at, updated_at
                    )
                    VALUES (
                        S.config_id, S.target_dataset, S.target_table, S.scd_type,
                        S.primary_keys, S.surrogate_key, S.begin_date_column,
                        S.end_date_column, S.active_flag_column, S.description,
                        S.custom_tests, @current_timestamp, @current_timestamp
                    )
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("config_id", "STRING", config_data.get("config_id")),
                    bigquery.ScalarQueryParameter("target_dataset", "STRING", config_data.get("target_dataset")),
                    bigquery.ScalarQueryParameter("target_table", "STRING", config_data.get("target_table")),
                    bigquery.ScalarQueryParameter("scd_type", "STRING", config_data.get("scd_type")),
                    bigquery.ArrayQueryParameter("primary_keys", "STRING", config_data.get("primary_keys", [])),
                    bigquery.ScalarQueryParameter("surrogate_key", "STRING", config_data.get("surrogate_key")),
                    bigquery.ScalarQueryParameter("begin_date_column", "STRING", config_data.get("begin_date_column")),
                    bigquery.ScalarQueryParameter("end_date_column", "STRING", config_data.get("end_date_column")),
                    bigquery.ScalarQueryParameter("active_flag_column", "STRING", config_data.get("active_flag_column")),
                    bigquery.ScalarQueryParameter("description", "STRING", config_data.get("description", "")),
                    bigquery.ScalarQueryParameter("custom_tests", "STRING", json.dumps(config_data.get("custom_tests")) if config_data.get("custom_tests") else None),
                    bigquery.ScalarQueryParameter("current_timestamp", "TIMESTAMP", current_ts_str),
                ]
            )
            
            query_job = self.client.query(query, job_config=job_config)
            query_job.result()
            
            return True
            
        except Exception as e:
            logger.error(f"Error inserting SCD config: {str(e)}", exc_info=True)
            return False

    # --- Test1 Granular Logging (Preferred) ---

    async def ensure_test_history_table(
        self,
        project_id: str,
        dataset_id: str = "config",
        table_id: str = "test_execution_history"
    ) -> str:
        """Ensure test execution history table exists (Granular)."""
        try:
            full_table_name = f"{project_id}.{dataset_id}.{table_id}"
            try:
                self.client.get_dataset(f"{project_id}.{dataset_id}")
            except Exception: # NotFound
                try:
                    dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
                    dataset.location = "US"
                    self.client.create_dataset(dataset)
                except Exception as e:
                    print(f"Failed to create dataset {dataset_id}: {e}")

            try:
                self.client.get_table(full_table_name)
                return full_table_name
            except Exception:
                pass
            
            schema = [
                bigquery.SchemaField("execution_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("test_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("project_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("comparison_mode", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("mapping_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("test_name", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("severity", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("target", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("rows_affected", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("sql_query", "STRING", mode="NULLABLE"),
            ]
            
            table = bigquery.Table(full_table_name, schema=schema)
            self.client.create_table(table)
            print(f"Created test history table: {full_table_name}")
            return full_table_name
            
        except Exception as e:
            print(f"Warning: Failed to ensure test history table: {str(e)}")
            return f"{project_id}.{dataset_id}.{table_id}"

    async def log_execution(
        self,
        project_id: str,
        execution_data: List[Dict[str, Any]],
        dataset_id: str = "config",
        table_id: str = "test_execution_history"
    ) -> None:
        """Log test results to history table (Granular - Test1)."""
        try:
            full_table_name = await self.ensure_test_history_table(project_id, dataset_id, table_id)
            
            # Use Melbourne timezone
            melbourne_tz = pytz.timezone('Australia/Melbourne')
            current_time = datetime.datetime.now(melbourne_tz)
            
            rows_to_insert = []
            for item in execution_data:
                # Convert datetime to ISO format string for JSON serialization
                item['timestamp'] = current_time.isoformat()
                rows_to_insert.append(item)
            
            if not rows_to_insert:
                return

            errors = self.client.insert_rows_json(full_table_name, rows_to_insert)
            if errors:
                logger.error(f"Failed to insert history rows: {errors}")
            else:
                logger.info(f"✓ Successfully logged {len(rows_to_insert)} rows to {full_table_name}")
                
        except Exception as e:
            logger.error(f"Failed to log execution: {str(e)}")

    # --- Test3 Summary Logging (Support) ---

    async def ensure_summary_history_table(
        self,
        project_id: str,
        dataset_id: str = "config",
        table_id: str = "execution_history"
    ) -> str:
        """Ensure execution history table exists (Test3 Aggregated)."""
        try:
            full_table_name = f"{project_id}.{dataset_id}.{table_id}"
            
            try:
                self.client.get_table(full_table_name)
                return full_table_name
            except Exception:
                pass
            
            schema = [
                bigquery.SchemaField("execution_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("project_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("comparison_mode", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("target", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("total_tests", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("passed_tests", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("failed_tests", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("details", "JSON", mode="NULLABLE"),
            ]
            
            table = bigquery.Table(full_table_name, schema=schema)
            self.client.create_table(table)
            print(f"Created history table: {full_table_name}")
            return full_table_name
            
        except Exception as e:
             print(f"Warning: Failed to ensure summary history table: {str(e)}")
             return f"{project_id}.{dataset_id}.{table_id}"

    async def log_execution_summary(
        self,
        project_id: str,
        execution_data: Dict[str, Any],
        dataset_id: str = "config",
        table_id: str = "execution_history"
    ) -> None:
        """Log execution result to history table (Aggregated - Test3)."""
        try:
            full_table_name = await self.ensure_summary_history_table(project_id, dataset_id, table_id)
            
            import uuid
            melbourne_tz = pytz.timezone('Australia/Melbourne')
            
            row = {
                "execution_id": execution_data.get("execution_id") or str(uuid.uuid4()),
                "timestamp": datetime.datetime.now(melbourne_tz).strftime('%Y-%m-%d %H:%M:%S'),
                "project_id": project_id,
                "comparison_mode": execution_data.get("comparison_mode", "unknown"),
                "source": execution_data.get("source", ""),
                "target": execution_data.get("target", ""),
                "status": execution_data.get("status", "UNKNOWN"),
                "total_tests": execution_data.get("total_tests", 0),
                "passed_tests": execution_data.get("passed_tests", 0),
                "failed_tests": execution_data.get("failed_tests", 0),
                "details": json.dumps(execution_data.get("details", {}), default=str)
            }
            
            errors = self.client.insert_rows_json(full_table_name, [row])
            if errors:
                print(f"Failed to insert summary history row: {errors}")
                
        except Exception as e:
            print(f"Failed to log execution summary: {str(e)}")
    
    # --- Custom Tests & Settings (Test1 & Test3 Shared) ---

    async def ensure_custom_tests_table(
        self,
        project_id: str,
        dataset_id: str = "config",
        table_id: str = "custom_tests"
    ) -> str:
        """Ensure custom tests table exists."""
        try:
            full_table_name = f"{project_id}.{dataset_id}.{table_id}"
            try:
                self.client.get_table(full_table_name)
                return full_table_name
            except Exception:
                pass
            
            schema = [
                bigquery.SchemaField("test_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("test_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("test_category", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("severity", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("sql_query", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("target_dataset", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("target_table", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("is_active", "BOOLEAN", mode="REQUIRED"),
            ]
            
            table = bigquery.Table(full_table_name, schema=schema)
            self.client.create_table(table)
            print(f"Created custom tests table: {full_table_name}")
            return full_table_name
            
        except Exception as e:
            print(f"Warning: Failed to ensure custom tests table: {str(e)}")
            return f"{project_id}.{dataset_id}.{table_id}"

    async def save_custom_test(
        self,
        test_data: Dict[str, Any]
    ) -> bool:
        """Save a custom test to BigQuery."""
        try:
            project_id = test_data.get('project_id')
            dataset_id = test_data.get('dataset_id', 'config')
            full_table_name = await self.ensure_custom_tests_table(project_id, dataset_id)
            
            import uuid
            melbourne_tz = pytz.timezone('Australia/Melbourne')
            
            row = {
                "test_id": str(uuid.uuid4()),
                "created_at": datetime.datetime.now(melbourne_tz).strftime('%Y-%m-%d %H:%M:%S'),
                "test_name": test_data.get('test_name'),
                "test_category": test_data.get('test_category'),
                "severity": test_data.get('severity'),
                "sql_query": test_data.get('sql_query'),
                "description": test_data.get('description'),
                "target_dataset": test_data.get('target_dataset'),
                "target_table": test_data.get('target_table'),
                "is_active": True
            }
            
            errors = self.client.insert_rows_json(full_table_name, [row])
            if errors:
                print(f"Failed to insert custom test: {errors}")
                return False
            return True
                
        except Exception as e:
            print(f"Failed to save custom test: {str(e)}")
            return False

    async def ensure_settings_table(
        self,
        project_id: str,
        dataset_id: str = "config",
        table_id: str = "project_settings"
    ) -> str:
        """Ensure settings table exists (Test1 Feature)."""
        try:
            full_table_name = f"{project_id}.{dataset_id}.{table_id}"
            try:
                self.client.get_dataset(f"{project_id}.{dataset_id}")
            except Exception:
                pass 

            try:
                table = self.client.get_table(full_table_name)
                # Check for missing columns and patch schema if needed
                existing_fields = {f.name for f in table.schema}
                new_schema = list(table.schema)
                schema_changed = False

                if "teams_webhook_url" not in existing_fields:
                    new_schema.append(bigquery.SchemaField("teams_webhook_url", "STRING", mode="NULLABLE"))
                    schema_changed = True
                
                if schema_changed:
                    table.schema = new_schema
                    self.client.update_table(table, ["schema"])
                    print(f"Updated settings table schema: {full_table_name}")
                
                return full_table_name
            except Exception:
                pass
                
            schema = [
                bigquery.SchemaField("project_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("alert_emails", "STRING", mode="REPEATED"),
                bigquery.SchemaField("teams_webhook_url", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("alert_on_failure", "BOOLEAN", mode="NULLABLE"),
                bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE")
            ]
            
            table = bigquery.Table(full_table_name, schema=schema)
            self.client.create_table(table)
            print(f"Created settings table: {full_table_name}")
            return full_table_name
        except Exception as e:
            print(f"Warning: Failed to ensure settings table: {e}")
            return f"{project_id}.{dataset_id}.{table_id}"

    async def get_project_settings(
        self,
        project_id: str,
        dataset_id: str = "config",
        table_id: str = "project_settings"
    ) -> Dict[str, Any]:
        """Get latest project settings (Test1 Feature)."""
        try:
            full_table_name = f"{project_id}.{dataset_id}.{table_id}"
            await self.ensure_settings_table(project_id, dataset_id, table_id)
            
            query = f"""
                SELECT *
                FROM `{full_table_name}`
                WHERE project_id = '{project_id}'
                ORDER BY updated_at DESC
                LIMIT 1
            """
            rows = await self.execute_query(query)
            if rows:
                return rows[0]
            return None
        except Exception as e:
            return None

    async def save_project_settings(
        self,
        settings: Dict[str, Any],
        dataset_id: str = "config",
        table_id: str = "project_settings"
    ) -> bool:
        """Save project settings (Test1 Feature)."""
        try:
            project_id = settings.get('project_id')
            full_table_name = await self.ensure_settings_table(project_id, dataset_id, table_id)
            
            melbourne_tz = pytz.timezone('Australia/Melbourne')
            row = settings.copy()
            row['updated_at'] = datetime.datetime.now(melbourne_tz).strftime('%Y-%m-%d %H:%M:%S')
            
            errors = self.client.insert_rows_json(full_table_name, [row])
            if errors:
                print(f"Failed to save settings: {errors}")
                return False
            return True
        except Exception as e:
            print(f"Failed to save project settings: {str(e)}")
            return False

    # --- SCD History (Test4 Requirement) ---

    async def ensure_scd_history_table(
        self,
        project_id: str,
        dataset_id: str = "qa_results",
        table_id: str = "scd_test_history"
    ) -> str:
        """Ensure SCD test history table exists."""
        try:
            full_table_name = f"{project_id}.{dataset_id}.{table_id}"
            
            # Ensure dataset
            try:
                self.client.get_dataset(f"{project_id}.{dataset_id}")
            except Exception:
                try:
                    dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
                    dataset.location = "US"
                    self.client.create_dataset(dataset)
                except Exception as e:
                     print(f"Failed to create dataset {dataset_id}: {e}")

            # Ensure table exists
            try:
                table = self.client.get_table(full_table_name)
                
                # Check for missing columns and update if needed
                existing_cols = {f.name for f in table.schema}
                new_schema = list(table.schema)
                modified = False
                
                required_cols = [
                    ("execution_timestamp", "TIMESTAMP"),
                    ("executed_by", "STRING"),
                    ("executed_by", "STRING"),
                    ("error_message", "STRING"),
                    ("target_dataset", "STRING"),
                    ("target_table", "STRING"),
                    ("total_tests", "INTEGER"),
                    ("passed_tests", "INTEGER"),
                    ("failed_tests", "INTEGER"),
                    ("test_results", "JSON"),
                    ("metadata", "JSON")
                ]
                
                for col_name, col_type in required_cols:
                    if col_name not in existing_cols:
                        new_schema.append(bigquery.SchemaField(col_name, col_type, mode="NULLABLE"))
                        modified = True
                
                if modified:
                    table.schema = new_schema
                    self.client.update_table(table, ["schema"])
                    logger.info(f"Updated SCD history table schema: {full_table_name}")
                
                return full_table_name
            except Exception as e:
                if "Not found" not in str(e):
                    logger.warning(f"Error checking/updating SCD history table: {e}")
                pass
            
            schema = [
                bigquery.SchemaField("execution_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("test_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("execution_timestamp", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("project_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("comparison_mode", "STRING", mode="REQUIRED"), 
                bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("target_dataset", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("target_table", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("executed_by", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("total_tests", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("passed_tests", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("failed_tests", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("test_results", "JSON", mode="NULLABLE"),
                bigquery.SchemaField("metadata", "JSON", mode="NULLABLE"),
            ]
            
            table = bigquery.Table(full_table_name, schema=schema)
            self.client.create_table(table)
            logger.info(f"Created SCD history table: {full_table_name}")
            return full_table_name
            
        except Exception as e:
            logger.error(f"Failed to ensure SCD history table: {str(e)}")
            return f"{project_id}.{dataset_id}.{table_id}"

    async def log_scd_execution(
        self,
        project_id: str,
        execution_data: List[Dict[str, Any]],
        dataset_id: str = "qa_results",
        table_id: str = "scd_test_history"
    ) -> None:
        """Log SCD test results to specific history table."""
        if not project_id:
            project_id = "leyin-sandpit"
            
        try:
            full_table_name = await self.ensure_scd_history_table(project_id, dataset_id, table_id)
            
            # Use Melbourne local time for wall-clock representation as requested
            melbourne_tz = pytz.timezone('Australia/Melbourne')
            current_time = datetime.datetime.now(melbourne_tz).strftime('%Y-%m-%d %H:%M:%S')
            
            rows_to_insert = []
            for item in execution_data:
                # Map fields if necessary, assuming item matches schema mostly
                row = item.copy()
                row['execution_timestamp'] = current_time
                row['comparison_mode'] = 'scd'
                # Ensure mapping_id maps to config_id if present
                if 'config_id' in row and 'mapping_id' not in row:
                    row['mapping_id'] = row['config_id']
                
                # IMPORTANT: For BigQuery JSON type columns, verify we aren't sending a raw list 
                # which might be interpreted as a REPEATED field. 
                # Newer client versions usually handle dict/list -> JSON column fine, but if it fails,
                # we can explicitly leave them as objects. However, seeing the error "Array specified for non-repeated field",
                # it means we MUST serialize lists to strings or use a specific wrapper.
                # Let's try explicit serialization for safety if they are complex types.
                # Actually, insert_rows_json expects native Python types (dicts/lists) for JSON columns
                # BUT if it fails, it might be due to schema caching or ambiguity.
                # Let's simple check: if it is a list, wrapping it might fail if we don't.
                # Workaround: We will rely on the fact that for a JSON column, passing a list *should* work 
                # but if the table was created recently, schema might be lagging.
                # However, the robust fix for 'Array specified...' on a JSON column is often just to ensure it matches.
                # We'll leave them as is first, but if it fails, we used json.dumps.
                # Given the error observed: "Array specified for non-repeated field: test_results",
                # WE MUST serialise. But 'insert_rows_json' typically handles serialization.
                # If we serialize to string: BigQuery accepts string for JSON column.
                if 'test_results' in row and isinstance(row['test_results'], (list, dict)):
                   # row['test_results'] = json.dumps(row['test_results']) # This would make it a STRING.
                   # But wait, insert_rows_json should handle it. 
                   # The error implies the table schema might still think it's a STRING (not JSON) 
                   # OR the library is confused.
                   # To fail safe against "Array specified...", we serialize.
                   pass
                
                # Re-reading the error: "Array specified for non-repeated field: test_results".
                # This happens when you try to insert a Python LIST into a BigQuery column that is NOT REPEATED.
                # Even for JSON type, the client might map List -> Repeated.
                # FIX: Serialize to string. BigQuery parses strings into JSON type on insert.
                if 'test_results' in row and not isinstance(row['test_results'], str):
                     row['test_results'] = json.dumps(row['test_results'], default=str)
                
                if 'metadata' in row and not isinstance(row['metadata'], str):
                     row['metadata'] = json.dumps(row['metadata'], default=str)

                
                # Filter out keys not in schema to avoid errors
                valid_keys = {
                    "execution_id", "execution_timestamp", "project_id", 
                    "comparison_mode", "mapping_id", 
                    "status", "error_message",
                    "target_dataset", "target_table",
                    "executed_by", "total_tests", "passed_tests", "failed_tests",
                    "test_results", "metadata"
                }
                filtered_row = {k: v for k, v in row.items() if k in valid_keys}
                rows_to_insert.append(filtered_row)
            
            if not rows_to_insert:
                logger.warning("No rows to log for SCD execution")
                return

            logger.info(f"Attempting to log {len(rows_to_insert)} SCD history rows to {full_table_name}")
            errors = self.client.insert_rows_json(full_table_name, rows_to_insert)
            if errors:
                logger.error(f"Failed to insert SCD history rows: {errors}")
                
        except Exception as e:
            logger.error(f"Failed to log SCD execution: {str(e)}", exc_info=True)


    async def delete_execution_history(self, project_id: str, execution_id: str) -> bool:
        """Delete execution history from all relevant tables."""
        try:
            # Tables to delete from
            tables_to_clean = [
                f"{project_id}.config.execution_history",
                f"{project_id}.config.test_execution_history",
                f"{project_id}.qa_results.scd_test_history"
            ]
            
            clean_exec_id = execution_id.strip()

            for table in tables_to_clean:
                try:
                    query = f"""
                        DELETE FROM `{table}`
                        WHERE execution_id = @execution_id
                    """
                    job_config = bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("execution_id", "STRING", clean_exec_id)
                        ]
                    )
                    query_job = self.client.query(query, job_config=job_config)
                    query_job.result()
                    logger.info(f"Deleted execution {clean_exec_id} from {table}")
                except Exception as table_err:
                    # Log but continue if table doesn't exist or other minor error
                    logger.warning(f"Could not delete from {table}: {table_err}")
            
            return True
        except Exception as e:
            logger.error(f"Error deleting execution history: {e}")
            return False

    async def delete_all_execution_history(self, project_id: str) -> bool:
        """Delete ALL execution history from all relevant tables."""
        try:
            # Tables to delete from
            tables_to_clean = [
                f"{project_id}.config.execution_history",
                f"{project_id}.config.test_execution_history",
                f"{project_id}.qa_results.scd_test_history"
            ]
            
            success_count = 0
            for table in tables_to_clean:
                try:
                    # Use TRUNCATE TABLE for better performance
                    query = f"TRUNCATE TABLE `{table}`"
                    query_job = self.client.query(query)
                    query_job.result()
                    logger.info(f"✓ Successfully truncated {table}")
                    success_count += 1
                except Exception as table_err:
                    # Log the specific error for debugging
                    logger.error(f"✗ Failed to truncate {table}: {str(table_err)}")
            
            logger.info(f"Delete all completed: {success_count}/{len(tables_to_clean)} tables cleared")
            return success_count > 0  # Return True if at least one table was cleared
        except Exception as e:
            logger.error(f"Error clearing all execution history: {e}")
            return False


    async def get_execution_history(
        self,
        project_id: str,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get unified execution history from summary (Test3) AND SCD (Test4) tables.
        Returns a sorted list of execution summaries.
        """
        if not project_id:
            project_id = "leyin-sandpit"
            
        all_rows = []
        
        # 1. Fetch from Summary Table (Old Method - General Tests)
        try:
            summary_table = f"{project_id}.config.execution_history"
            # Schema: execution_id, timestamp, project_id, comparison_mode, source, target, status, ... details(JSON)
            query_summary = f"""
                SELECT 
                    execution_id,
                    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', timestamp) as timestamp,
                    project_id,
                    comparison_mode,
                    source,
                    target,
                    status,
                    total_tests,
                    passed_tests,
                    failed_tests,
                    TO_JSON_STRING(details) as details_json
                FROM `{summary_table}`
                ORDER BY timestamp DESC
                LIMIT {limit}
            """
            summary_rows = await self.execute_query(query_summary)
            for row in summary_rows:
                # Parse details JSON
                if row.get('details_json'):
                    try:
                        det = json.loads(row['details_json'])
                        # Sometimes details is {test_results: [...]}, sometimes just [...]
                        if isinstance(det, dict) and 'test_results' in det:
                            row['details'] = det['test_results']
                        else:
                            row['details'] = det
                    except:
                        row['details'] = []
                else:
                    row['details'] = []
                if 'details_json' in row: del row['details_json']
                
                # Default missing fields for UI consistency
                row['executed_by'] = 'Manual Run' 
                all_rows.append(row)
        except Exception as e:
            logger.info(f"Summary history table not found or query failed: {e}")
            pass

        # 2. Fetch from Granular History Table (Standard Tests)
        try:
            granular_table = f"{project_id}.config.test_execution_history"
            # Schema: execution_id, timestamp, project_id, comparison_mode, mapping_id, test_name, category, status, severity, description, error_message, source, target, rows_affected, sql_query
            query_granular = f"""
                SELECT
                    execution_id,
                    FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', DATETIME(MAX(timestamp), 'Australia/Melbourne')) as timestamp,
                    MAX(project_id) as project_id,
                    MAX(comparison_mode) as comparison_mode,
                    IF(COUNT(DISTINCT source) > 1, 'Multiple Sources', MAX(source)) as source,
                    IF(COUNT(DISTINCT target) > 1, 'Multiple Targets', MAX(target)) as target,
                    IF(COUNTIF(status = 'FAIL') > 0, 'FAIL', IF(COUNTIF(status = 'ERROR') > 0, 'ERROR', 'PASS')) as status,
                    COUNT(*) as total_tests,
                    COUNTIF(status = 'PASS') as passed_tests,
                    COUNTIF(status = 'FAIL') as failed_tests,
                    TO_JSON_STRING(ARRAY_AGG(STRUCT(
                        test_id, test_name, category, status, severity, 
                        description, error_message, source, target, 
                        rows_affected, sql_query, mapping_id
                    ))) as details_json
                FROM `{granular_table}`
                GROUP BY execution_id
                ORDER BY timestamp DESC
                LIMIT {limit}
            """
            granular_rows = await self.execute_query(query_granular)
            for row in granular_rows:
                 # Parse details JSON
                if row.get('details_json'):
                    try:
                        row['details'] = json.loads(row['details_json'])
                    except:
                        row['details'] = []
                else:
                    row['details'] = []
                if 'details_json' in row: del row['details_json']
                
                row['executed_by'] = 'Manual Run'
                all_rows.append(row)
        except Exception as e:
            logger.info(f"Granular history table not found or query failed: {e}")
            pass

        # 3. Fetch from SCD History Table (Table-Level Schema)
        try:
            # Heal schema if needed
            await self.ensure_scd_history_table(project_id)
            
            scd_table = f"{project_id}.qa_results.scd_test_history"
            # User's Schema: execution_id, execution_timestamp, project_id, comparison_mode, mapping_id, target_dataset, target_table, status, total_tests, passed_tests, failed_tests, error_message, test_results, executed_by, metadata
            query_scd = f"""
                SELECT
                    execution_id,
                    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', MAX(execution_timestamp)) as timestamp,
                    MAX(project_id) as project_id,
                    MAX(comparison_mode) as comparison_mode,
                    'SCD Validation' as source,
                    IF(COUNT(DISTINCT CONCAT(target_dataset, '.', target_table)) > 1, 'Multiple Targets', MAX(CONCAT(target_dataset, '.', target_table))) as target,
                    IF(COUNTIF(status = 'FAIL') > 0, 'FAIL', 'PASS') as status,
                    SUM(total_tests) as total_tests,
                    SUM(passed_tests) as passed_tests,
                    SUM(failed_tests) as failed_tests,
                    TO_JSON_STRING(ARRAY_AGG(STRUCT(
                        mapping_id, target_dataset, target_table, status,
                        total_tests, passed_tests, failed_tests,
                        test_results as predefined_results
                    ))) as details_json,
                    MAX(IFNULL(executed_by, 'Manual Run')) as executed_by
                FROM `{scd_table}`
                GROUP BY execution_id
                ORDER BY timestamp DESC
                LIMIT {limit}
            """
            
            # Additional logging to debug query results
            # logger.info(f"Executing SCD history query: {query_scd}")
            
            scd_rows = await self.execute_query(query_scd)
            
            # logger.info(f"SCD Query returned {len(scd_rows)} rows")
            
            for row in scd_rows:
                 # Parse details JSON
                if row.get('details_json'):
                    try:
                        # BigQuery ARRAY_AGG(STRUCT) returns a list of dictionaries directly in the JSON string
                        # We need to ensure we parse it correctly
                        details_list = json.loads(row['details_json'])
                        
                        # Fix: Ensure the structure matches what the frontend expects
                        # The frontend expects 'results_by_mapping' or a flat list of results with metadata
                        # We'll construct a 'results_by_mapping' structure here
                        formatted_mappings = []
                        
                        for mapping in details_list:
                            # Test results might be nested as a string JSON if they were stored that way
                            predefined_results = mapping.get('predefined_results')
                            if isinstance(predefined_results, str):
                                try:
                                    predefined_results = json.loads(predefined_results)
                                except:
                                    predefined_results = []
                            elif not isinstance(predefined_results, list):
                                predefined_results = []
                                
                            formatted_mappings.append({
                                "mapping_id": mapping.get('mapping_id') or mapping.get('target_table'),
                                "mapping_info": {
                                    "target": f"{mapping.get('target_dataset')}.{mapping.get('target_table')}",
                                    "source": "SCD Logic"
                                },
                                "predefined_results": predefined_results,
                                "summary": {
                                    "total_tests": mapping.get('total_tests', 0),
                                    "passed": mapping.get('passed_tests', 0),
                                    "failed": mapping.get('failed_tests', 0),
                                    "errors": 0 
                                }
                            })

                        row['details'] = {
                            "execution_id": row['execution_id'],
                            "comparison_mode": "scd",
                            "summary": {
                                "total_tests": row['total_tests'],
                                "passed": row['passed_tests'],
                                "failed": row['failed_tests'],
                                "errors": 0,
                                "total_mappings": len(formatted_mappings)
                            },
                            "results_by_mapping": formatted_mappings
                        }
                    except Exception as e:
                        logger.error(f"Error parsing detailed JSON for execution {row['execution_id']}: {e}")
                        row['details'] = []
                else:
                    row['details'] = []
                if 'details_json' in row: del row['details_json']
                
                all_rows.append(row)
        except Exception as e:
            logger.error(f"SCD history retrieval failed: {e}")
            pass

        # 3. Deduplicate and Sort
        # Prefer the entry with more details if duplicates exist (rare split logging)
        # Sort by timestamp desc
        all_rows.sort(key=lambda x: str(x.get('timestamp', '')), reverse=True)
        
        unique_rows = {}
        for row in all_rows:
            eid = row['execution_id']
            if eid not in unique_rows:
                unique_rows[eid] = row
            else:
                # If we have a duplicate, keep the one that isn't empty on details/target if possible
                curr = unique_rows[eid]
                if not curr.get('target') and row.get('target'):
                    unique_rows[eid] = row
        
        final_list = list(unique_rows.values())
        final_list.sort(key=lambda x: str(x.get('timestamp', '')), reverse=True)
        
        return final_list[:limit]



    async def get_active_custom_tests(
        self,
        project_id: str,
        target_dataset: str,
        target_table: str,
        dataset_id: str = "config"
    ) -> List[Dict[str, Any]]:
        """Get active custom tests."""
        try:
            full_table_name = await self.ensure_custom_tests_table(project_id, dataset_id)
            
            query = f"""
                SELECT *
                FROM `{full_table_name}`
                WHERE is_active = true
                AND target_dataset = '{target_dataset}'
                AND target_table = '{target_table}'
            """
            return await self.execute_query(query)
        except Exception as e:
            print(f"Failed to get custom tests: {str(e)}")
            return []

# Singleton instance
bigquery_service = BigQueryService()
