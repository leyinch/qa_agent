"""BigQuery service for database operations."""
from typing import List, Dict, Any
import json
import logging
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
        config_table: str,
        filters: Dict[str, Any] = None
    ) -> List[Dict[str, Any]]:
        """
        Read mappings from config table.
        
        Args:
            project_id: Google Cloud project ID
            config_dataset: Config table dataset
            config_table: Config table name
            filters: Optional dictionary of filters
            
        Returns:
            List of mapping configurations
        """
        # Ensure dataset and table exist
        try:
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
        
        # Add dynamic filters
        if filters:
            for key, value in filters.items():
                if isinstance(value, str):
                    query += f" AND {key} = '{value}'"
                elif isinstance(value, (int, float, bool)):
                     query += f" AND {key} = {value}"
                else:
                    # Fallback for complex types, maybe cast to string
                    query += f" AND {key} = '{str(value)}'"

        return await self.execute_query(query)



    async def ensure_test_history_table(
        self,
        project_id: str,
        dataset_id: str = "config",
        table_id: str = "test_execution_history"
    ) -> str:
        """
        Ensure test execution history table exists.
        
        Returns:
            Full table name
        """
        try:
            full_table_name = f"{project_id}.{dataset_id}.{table_id}"
            
            # 1. Ensure dataset exists
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
        """
        Log test results to history table.
        Args:
            execution_data: List of test result dictionaries
        """
        try:
            full_table_name = await self.ensure_test_history_table(project_id, dataset_id, table_id)
            
            import datetime
            current_time = datetime.datetime.now().isoformat()
            
            rows_to_insert = []
            for item in execution_data:
                item['timestamp'] = current_time
                rows_to_insert.append(item)
            
            if not rows_to_insert:
                return

            errors = self.client.insert_rows_json(full_table_name, rows_to_insert)
            if errors:
                print(f"Failed to insert history rows: {errors}")
                
        except Exception as e:
            print(f"Failed to log execution: {str(e)}")

    async def get_execution_history(
        self,
        project_id: str,
        dataset_id: str = "config",
        table_id: str = "test_execution_history",
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get recent test execution history."""
        # Ensure table exists before querying
        await self.ensure_test_history_table(project_id, dataset_id, table_id)
        
        query = f"""
            SELECT *
            FROM `{project_id}.{dataset_id}.{table_id}`
            ORDER BY timestamp DESC
            LIMIT {limit}
        """
        return await self.execute_query(query)

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


    async def ensure_settings_table(
        self,
        project_id: str,
        dataset_id: str = "config",
        table_id: str = "project_settings"
    ) -> str:
        """Ensure settings table exists."""
        try:
            full_table_name = f"{project_id}.{dataset_id}.{table_id}"
            
            # Helper to ensure dataset exists
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
        """Get latest project settings."""
        try:
            full_table_name = f"{project_id}.{dataset_id}.{table_id}"
            # Ensure table exists
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
            # Table might not exist yet
            return None

    async def save_project_settings(
        self,
        settings: Dict[str, Any],
        dataset_id: str = "config",
        table_id: str = "project_settings"
    ) -> bool:
        """Save project settings (appends new version)."""
        try:
            project_id = settings.get('project_id')
            full_table_name = await self.ensure_settings_table(project_id, dataset_id, table_id)
            
            import datetime
            row = settings.copy()
            row['updated_at'] = datetime.datetime.now().isoformat()
            
            errors = self.client.insert_rows_json(full_table_name, [row])
            if errors:
                print(f"Failed to save settings: {errors}")
                return False
            return True
        except Exception as e:
            print(f"Failed to save project settings: {str(e)}")
            return False


    async def get_active_custom_tests(
        self,
        project_id: str,
        target_dataset: str,
        target_table: str,
        dataset_id: str = "config"
    ) -> List[Dict[str, Any]]:
        """Get active custom tests for a target table."""
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
