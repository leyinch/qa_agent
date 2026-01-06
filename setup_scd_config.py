from google.cloud import bigquery

client = bigquery.Client(project='leyin-sandpit')

# Create Dataset
dataset_id = "transform_config"
dataset_ref = client.dataset(dataset_id)
dataset = bigquery.Dataset(dataset_ref)
dataset.location = "US"
client.create_dataset(dataset, exists_ok=True)
print(f"Dataset {dataset_id} created or already exists.")

# Create SCD Config Table
table_id = f"{dataset_id}.scd_validation_config"
schema = [
    bigquery.SchemaField("config_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("target_dataset", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("target_table", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("scd_type", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("primary_keys", "STRING", mode="REPEATED"),
    bigquery.SchemaField("surrogate_key", "STRING"),
    bigquery.SchemaField("begin_date_column", "STRING"),
    bigquery.SchemaField("end_date_column", "STRING"),
    bigquery.SchemaField("active_flag_column", "STRING"),
    bigquery.SchemaField("description", "STRING")
]

table = bigquery.Table(f"leyin-sandpit.{table_id}", schema=schema)
client.delete_table(table, not_found_ok=True)
table = client.create_table(table)
print(f"Table {table_id} created.")

# Insert Config Data
rows_to_insert = [
    {
        "config_id": "seat_scd1",
        "target_dataset": "crown_scd_mock",
        "target_table": "D_Seat_WD",
        "scd_type": "scd1",
        "primary_keys": ["TableId", "PositionIDX"],
        "surrogate_key": "DWSeatID",
        "description": "SCD1 Mock for Gaming Seats (Test Data)"
    },
    {
        "config_id": "employee_scd2",
        "target_dataset": "crown_scd_mock",
        "target_table": "D_Employee_WD",
        "scd_type": "scd2",
        "primary_keys": ["UserId"],
        "surrogate_key": "DWEmployeeID",
        "begin_date_column": "DWBeginEffDateTime",
        "end_date_column": "DWEndEffDateTime",
        "active_flag_column": "DWCurrentRowFlag",
        "description": "SCD2 Mock for Employees (Test Data)"
    },

]

errors = client.insert_rows_json(table, rows_to_insert)
if not errors:
    print("Config data inserted successfully.")
else:
    print(f"Errors inserting config data: {errors}")
