from google.cloud import bigquery
from google.api_core.exceptions import NotFound

client = bigquery.Client(project="leyin-sandpit")
dataset_id = "qa_agent_metadata"
dataset_ref = f"leyin-sandpit.{dataset_id}"

# Create dataset if not exists
try:
    client.get_dataset(dataset_ref)
    print(f"Dataset {dataset_ref} already exists.")
except NotFound:
    print(f"Dataset {dataset_ref} not found. Creating...")
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    dataset = client.create_dataset(dataset)
    print(f"Created dataset {dataset_ref}")

# Read and execute SQL
with open("backend/create_history_table.sql", "r") as f:
    sql = f.read()

print("Executing schema SQL (Table-Level Granularity)...")
query_job = client.query(sql)
query_job.result()
print("Successfully created table-level history table and view.")
