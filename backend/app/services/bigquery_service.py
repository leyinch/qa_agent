"""BigQuery service for database operations."""
from typing import List, Dict, Any
import json
from google.cloud import bigquery


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
        
        Args:
            project_id: Google Cloud project ID
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID
            
        Returns:
            Dictionary containing table metadata
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
    
    async def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute a BigQuery SQL query.
        
        Args:
            query: SQL query string
            
        Returns:
            List of dictionaries representing rows
        """
        try:
            query_job = self.client.query(query)
            results = query_job.result()
            
            # Convert to list of dicts
            return [dict(row) for row in results]
            
        except Exception as e:
            raise ValueError(f"Query execution failed: {str(e)}")
    
    async def get_row_count(self, full_table_name: str) -> int:
        """
        Get row count for a table.
        
        Args:
            full_table_name: Fully qualified table name (project.dataset.table)
            
        Returns:
            Number of rows
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
        
        Args:
            full_table_name: Fully qualified table name
            limit: Maximum number of rows
            
        Returns:
            List of dictionaries representing rows
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
        
        Args:
            project_id: Google Cloud project ID
            dataset_id: BigQuery dataset ID
            
        Returns:
            List of table IDs
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
        config_table: str
    ) -> List[Dict[str, Any]]:
        """
        Read mappings from config table.
        
        Args:
            project_id: Google Cloud project ID
            config_dataset: Config table dataset
            config_table: Config table name
            
        Returns:
            List of mapping configurations
        """
        query = f"""
            SELECT *
            FROM `{project_id}.{config_dataset}.{config_table}`
            WHERE is_active = true
        """
        return await self.execute_query(query)

    async def ensure_config_tables(
        self,
        project_id: str,
        config_dataset: str = "config"
    ) -> None:
        """Ensure all configuration tables exist."""
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

            # data_load_config removed as it is not required at this stage

        except Exception as e:
            print(f"Error ensuring config tables: {str(e)}")

    async def read_config_table(
        self, 
        project_id: str, 
        config_dataset: str, 
        config_table: str
    ) -> List[Dict[str, Any]]:
        """
        Read mappings from config table.
        """
        await self.ensure_config_tables(project_id, config_dataset)
        query = f"""
            SELECT *
            FROM `{project_id}.{config_dataset}.{config_table}`
            WHERE is_active = true
        """
        return await self.execute_query(query)

    async def read_scd_config_table(
        self, 
        project_id: str, 
        config_dataset: str, 
        config_table: str
    ) -> List[Dict[str, Any]]:
        """
        Read SCD validation configurations from config table.
        """
        await self.ensure_config_tables(project_id, config_dataset)
        query = f"""
            SELECT *
            FROM `{project_id}.{config_dataset}.{config_table}`
        """
        return await self.execute_query(query)

    async def insert_scd_config(
        self,
        project_id: str,
        config_dataset: str,
        config_table: str,
        config_data: Dict[str, Any]
    ) -> bool:
        """
        Insert a new SCD validation configuration into the config table.
        
        Args:
            project_id: Google Cloud project ID
            config_dataset: Config table dataset
            config_table: Config table name
            config_data: Configuration data to insert
            
        Returns:
            True if successful, False otherwise
        """
        try:
            full_table_name = f"{project_id}.{config_dataset}.{config_table}"
            
            # Use MERGE to update if table exists, insert if not
            # Join key is (target_dataset, target_table) as requested
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


    async def ensure_history_table(
        self,
        project_id: str,
        dataset_id: str = "config",
        table_id: str = "execution_history"
    ) -> str:
        """
        Ensure execution history table exists.
        
        Returns:
            Full table name
        """
        try:
            full_table_name = f"{project_id}.{dataset_id}.{table_id}"
            
            # 1. Ensure dataset exists
            try:
                self.client.get_dataset(f"{project_id}.{dataset_id}")
            except Exception: # NotFound
                print(f"Dataset {dataset_id} not found, creating...")
                try:
                    dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
                    dataset.location = "US" # Default to US or make configurable
                    self.client.create_dataset(dataset)
                    print(f"Created dataset: {dataset_id}")
                except Exception as e:
                    print(f"Failed to create dataset {dataset_id}: {e}")
                    # Allow to proceed, maybe it exists but permission issue

            # 2. Check if table exists
            try:
                self.client.get_table(full_table_name)
                return full_table_name
            except Exception:
                # Table doesn't exist, create it
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
            print(f"Warning: Failed to ensure history table: {str(e)}")
            return f"{project_id}.{dataset_id}.{table_id}"

    async def log_execution(
        self,
        project_id: str,
        execution_data: Dict[str, Any],
        dataset_id: str = "config",
        table_id: str = "execution_history"
    ) -> None:
        """
        Log execution result to history table.
        """
        try:
            full_table_name = await self.ensure_history_table(project_id, dataset_id, table_id)
            
            # Prepare row
            row = {
                "execution_id": execution_data.get("execution_id"),
                "timestamp": "Generic::current_timestamp", # BQ handles this if we use SQL, but for insert_rows we need value. 
                # Better to use INSERT statement or current_timestamp() in SQL
            }
            # Actually, let's use insert_rows_json
            
            import datetime
            import uuid
            
            row = {
                "execution_id": execution_data.get("execution_id") or str(uuid.uuid4()),
                "timestamp": datetime.datetime.now().isoformat(),
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
                print(f"Failed to insert history row: {errors}")
                
        except Exception as e:
            print(f"Failed to log execution: {str(e)}")

    async def get_execution_history(
        self,
        project_id: str,
        dataset_id: str = "config",
        table_id: str = "execution_history",
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Get recent execution history."""
        try:
            # Ensure table exists before querying to avoid NotFound errors
            await self.ensure_history_table(project_id, dataset_id, table_id)
            
            query = f"""
                SELECT *
                FROM `{project_id}.{dataset_id}.{table_id}`
                ORDER BY timestamp DESC
                LIMIT {limit}
            """
        except Exception as e:
            print(f"Failed to fetch history: {str(e)}")
            return []

    async def ensure_custom_tests_table(
        self,
        project_id: str,
        dataset_id: str = "config",
        table_id: str = "custom_tests"
    ) -> str:
        """
        Ensure custom tests table exists.
        
        Returns:
            Full table name
        """
        try:
            full_table_name = f"{project_id}.{dataset_id}.{table_id}"
            
            # 1. Ensure dataset exists (reuse logic or rely on history table check having done it, but safer to check)
            try:
                self.client.get_dataset(f"{project_id}.{dataset_id}")
            except Exception: # NotFound
                try:
                    dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
                    dataset.location = "US"
                    self.client.create_dataset(dataset)
                except Exception as e:
                    print(f"Failed to create dataset {dataset_id}: {e}")

            # 2. Check if table exists
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
        """
        Save a custom test to BigQuery.
        """
        try:
            project_id = test_data.get('project_id')
            dataset_id = test_data.get('dataset_id', 'config')
            full_table_name = await self.ensure_custom_tests_table(project_id, dataset_id)
            
            import datetime
            import uuid
            
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
            if errors:
                print(f"Failed to insert custom test: {errors}")
                return False
            return True
                
        except Exception as e:
            print(f"Failed to save custom test: {str(e)}")
            return False


# Singleton instance
bigquery_service = BigQueryService()
