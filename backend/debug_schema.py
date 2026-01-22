from google.cloud import bigquery
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "leyin-sandpit")
logger.info(f"Using project: {project_id}")

try:
    client = bigquery.Client(project=project_id)
except Exception as e:
    logger.error(f"Failed to create BigQuery client: {e}")
    exit(1)

def print_table_schema(table_id):
    full_table_id = f"{project_id}.{table_id}"
    try:
        table = client.get_table(full_table_id)
        print(f"\n--- Schema for {full_table_id} ---")
        for schema_field in table.schema:
            print(f" - {schema_field.name} ({schema_field.field_type})")
    except Exception as e:
        print(f"\nError getting {full_table_id}: {e}")

if __name__ == "__main__":
    print_table_schema("config.execution_history")
    print_table_schema("qa_results.scd_test_history")
    print_table_schema("config.test_execution_history")
