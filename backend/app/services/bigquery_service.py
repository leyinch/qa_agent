from typing import List, Dict, Any, Optional
import json
import logging
from google.cloud import bigquery

# Configure logger
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
            self._client = bigquery.Client()
        return self._client
    
    async def get_table_metadata(
        self, 
        project_id: str, 
        dataset_id: str, 
        table_id: str
    ) -> Dict[str, Any]:
        """
        Get metadata for a BigQuery table.
        """
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
        """
        Execute a BigQuery SQL query.
        """
        try:
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()
            
            # Convert to list of dicts
            return [dict(row) for row in results]
            
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise ValueError(f"Query execution failed: {str(e)}")
    
    async def get_row_count(self, full_table_name: str) -> int:
        """
        Get row count for a table.
        """
        query = f"SELECT COUNT(*) as count FROM `{full_table_name}`"
        results = await self.execute_query(query)
        return int(results[0]['count'])
    
    async def get_sample_data(
        self, 
        full_table_name: str, 
        limit: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Get sample data from a table.
        """
        query = f"SELECT * FROM `{full_table_name}` LIMIT {limit}"
        return await self.execute_query(query)
    
    async def get_tables_in_dataset(
        self, 
        project_id: str, 
        dataset_id: str
    ) -> List[str]:
        """
        Get list of tables in a dataset.
        """
        try:
            dataset_ref = f"{project_id}.{dataset_id}"
            dataset = self.client.get_dataset(dataset_ref)
            tables = self.client.list_tables(dataset)
            
            return [table.table_id for table in tables]
            
        except Exception as e:
            raise ValueError(
                f"Failed to list tables in {project_id}.{dataset_id}: {str(e)}"
            )
    
    async def read_config_table(
        self, 
        project_id: str, 
        config_dataset: str, 
        config_table: str,
        filters: Dict[str, Any] = None
    ) -> List[Dict[str, Any]]:
        """
        Read mappings from config table with optional filtering.
        """
        # Ensure dataset and table exist (SCD specific check, but good for all)
        try:
            # We call this to ensure our SCD table is there, 
            # though standard gcs-config might use a different table.
            if config_table == "scd_validation_config":
                await self.ensure_config_tables(project_id, config_dataset)
                
            self.client.get_table(f"{project_id}.{config_dataset}.{config_table}")
        except Exception as e:
            error_msg = f"Config table '{config_dataset}.{config_table}' not found or inaccessible in project '{project_id}'. Details: {str(e)}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Build query
        query = f"""
            SELECT *
            FROM `{project_id}.{config_dataset}.{config_table}`
            WHERE is_active = true
        """
        
        # Add dynamic filters (from colleague's changes)
        if filters:
            for key, value in filters.items():
                if isinstance(value, str):
                    query += f" AND {key} = '{value}'"
                elif isinstance(value, (int, float, bool)):
                     query += f" AND {key} = {value}"
                else:
                    query += f" AND {key} = '{str(value)}'"

        return await self.execute_query(query)

    async def ensure_config_tables(
        self,
        project_id: str,
        config_dataset: str = "config"
    ) -> None:
        """Ensure all configuration tables exist (SCD specific)."""
        try:
            dataset_ref = f"{project_id}.{config_dataset}"
            try:
                self.client.get_dataset(dataset_ref)
            except Exception:
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = "US"
                self.client.create_dataset(dataset)
                logger.info(f"Created dataset: {config_dataset}")

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
                logger.info(f"Created table: scd_validation_config")

        except Exception as e:
            logger.error(f"Error ensuring config tables: {str(e)}")

    async def read_scd_config_table(
        self, 
        project_id: str, 
        config_dataset: str, 
        config_table: str
    ) -> List[Dict[str, Any]]:
        """Read SCD mappings."""
        await self.ensure_config_tables(project_id, config_dataset)
        query = f"SELECT * FROM `{project_id}.{config_dataset}.{config_table}`"
        return await self.execute_query(query)

    async def get_scd_config_by_table(
        self,
        project_id: str,
        config_dataset: str,
        config_table: str,
        target_dataset: str,
        target_table: str
    ) -> Optional[Dict[str, Any]]:
        """Fetch SCD config by table."""
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
        """Upsert SCD config."""
        try:
            full_table_name = f"{project_id}.{config_dataset}.{config_table}"
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
                        config_id = S.config_id,
                        scd_type = S.scd_type,
                        primary_keys = S.primary_keys,
                        surrogate_key = S.surrogate_key,
                        begin_date_column = S.begin_date_column,
                        end_date_column = S.end_date_column,
                        active_flag_column = S.active_flag_column,
                        description = S.description,
                        custom_tests = S.custom_tests,
                        updated_at = CURRENT_TIMESTAMP()
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
                        S.custom_tests, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
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
                ]
            )
            query_job = self.client.query(query, job_config=job_config)
            query_job.result()
            return True
        except Exception as e:
            logger.error(f"Error inserting SCD config: {str(e)}", exc_info=True)
            return False

    async def ensure_test_history_table(
        self,
        project_id: str,
        dataset_id: str = "config",
        table_id: str = "test_execution_history"
    ) -> str:
        """Ensure test execution history table exists (Colleague change)."""
        try:
            full_table_name = f"{project_id}.{dataset_id}.{table_id}"
            try:
                self.client.get_dataset(f"{project_id}.{dataset_id}")
            except Exception:
                dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
                dataset.location = "US"
                self.client.create_dataset(dataset)
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
            return full_table_name
        except Exception as e:
            logger.warning(f"Failed to ensure test history table: {str(e)}")
            return f"{project_id}.{dataset_id}.{table_id}"

    async def log_execution(
        self,
        project_id: str,
        execution_data: List[Dict[str, Any]],
        dataset_id: str = "config",
        table_id: str = "test_execution_history"
    ) -> None:
        """Log test results (Colleague change)."""
        try:
            full_table_name = await self.ensure_test_history_table(project_id, dataset_id, table_id)
            import datetime
            current_time = datetime.datetime.now().isoformat()
            rows_to_insert = []
            for item in execution_data:
                item['timestamp'] = current_time
                rows_to_insert.append(item)
            if rows_to_insert:
                errors = self.client.insert_rows_json(full_table_name, rows_to_insert)
                if errors:
                    logger.error(f"Failed to insert history rows: {errors}")
        except Exception as e:
            logger.error(f"Failed to log execution: {str(e)}")

    async def get_execution_history(
        self,
        project_id: str,
        dataset_id: str = "config",
        table_id: str = "test_execution_history",
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get history records (Colleague change)."""
        await self.ensure_test_history_table(project_id, dataset_id, table_id)
        query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}` ORDER BY timestamp DESC LIMIT {limit}"
        return await self.execute_query(query)

    async def ensure_custom_tests_table(
        self,
        project_id: str,
        dataset_id: str = "config",
        table_id: str = "custom_tests"
    ) -> str:
        """Ensure custom tests table (Colleague change)."""
        try:
            full_table_name = f"{project_id}.{dataset_id}.{table_id}"
            try:
                self.client.get_dataset(f"{project_id}.{dataset_id}")
            except Exception:
                dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
                dataset.location = "US"
                self.client.create_dataset(dataset)
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
            return full_table_name
        except Exception as e:
            logger.warning(f"Failed to ensure custom tests table: {str(e)}")
            return f"{project_id}.{dataset_id}.{table_id}"

    async def save_custom_test(self, test_data: Dict[str, Any]) -> bool:
        """Save custom test (Colleague change)."""
        try:
            project_id = test_data.get('project_id')
            dataset_id = test_data.get('dataset_id', 'config')
            full_table_name = await self.ensure_custom_tests_table(project_id, dataset_id)
            import datetime, uuid
            row = {
                "test_id": str(uuid.uuid4()),
                "created_at": datetime.datetime.now().isoformat(),
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
            return not bool(errors)
        except Exception as e:
            logger.error(f"Failed to save custom test: {str(e)}")
            return False

    async def ensure_settings_table(
        self,
        project_id: str,
        dataset_id: str = "config",
        table_id: str = "project_settings"
    ) -> str:
        """Ensure settings table (Colleague change)."""
        try:
            full_table_name = f"{project_id}.{dataset_id}.{table_id}"
            try:
                table = self.client.get_table(full_table_name)
                existing_fields = {f.name for f in table.schema}
                new_schema = list(table.schema)
                schema_changed = False
                if "teams_webhook_url" not in existing_fields:
                    new_schema.append(bigquery.SchemaField("teams_webhook_url", "STRING", mode="NULLABLE"))
                    schema_changed = True
                if schema_changed:
                    table.schema = new_schema
                    self.client.update_table(table, ["schema"])
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
            return full_table_name
        except Exception as e:
            logger.warning(f"Failed to ensure settings table: {e}")
            return f"{project_id}.{dataset_id}.{table_id}"

    async def get_project_settings(self, project_id: str) -> Dict[str, Any]:
        """Get settings (Colleague change)."""
        full_table_name = await self.ensure_settings_table(project_id)
        query = f"SELECT * FROM `{full_table_name}` WHERE project_id = '{project_id}' ORDER BY updated_at DESC LIMIT 1"
        rows = await self.execute_query(query)
        return rows[0] if rows else None

    async def save_project_settings(self, settings: Dict[str, Any]) -> bool:
        """Save settings (Colleague change)."""
        try:
            project_id = settings.get('project_id')
            full_table_name = await self.ensure_settings_table(project_id)
            import datetime
            row = settings.copy()
            row['updated_at'] = datetime.datetime.now().isoformat()
            errors = self.client.insert_rows_json(full_table_name, [row])
            return not bool(errors)
        except Exception as e:
            logger.error(f"Failed to save settings: {str(e)}")
            return False

    async def get_active_custom_tests(self, project_id: str, target_dataset: str, target_table: str) -> List[Dict[str, Any]]:
        """Get custom tests (Colleague change)."""
        try:
            full_table_name = await self.ensure_custom_tests_table(project_id)
            query = f"SELECT * FROM `{full_table_name}` WHERE is_active = true AND target_dataset = '{target_dataset}' AND target_table = '{target_table}'"
            return await self.execute_query(query)
        except Exception:
            return []

# Singleton instance
bigquery_service = BigQueryService()
