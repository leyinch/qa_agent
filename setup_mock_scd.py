import os
from google.cloud import bigquery

# Initialize BigQuery client
client = bigquery.Client(project='leyin-sandpit')

# 1. Create Mock Dataset
dataset_id = "crown_scd_mock"
dataset_ref = client.dataset(dataset_id)
dataset = bigquery.Dataset(dataset_ref)
dataset.location = "US"
client.create_dataset(dataset, exists_ok=True)
print(f"Dataset {dataset_id} created or already exists.")

# 2. Create SCD1 Mock Table (with intentional errors)
scd1_table_id = f"{dataset_id}.D_Seat_WD"
scd1_schema = [
    bigquery.SchemaField("TableId", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("PositionIDX", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("PositionCode", "STRING"),
    bigquery.SchemaField("PositionLabel", "STRING"),
    bigquery.SchemaField("DWSeatID", "INTEGER"),
    bigquery.SchemaField("UpdateTimestamp", "TIMESTAMP")
]

# Data contains:
# - Normal row
# - Duplicate Primary Keys (TableId, PositionIDX) -> Error
# - Null Primary Key -> Error
scd1_data = [
    {"TableId": 101, "PositionIDX": 1, "PositionCode": "P1", "PositionLabel": "Label 1", "DWSeatID": 1001, "UpdateTimestamp": "2024-01-01T00:00:00Z"},
    {"TableId": 101, "PositionIDX": 1, "PositionCode": "P1_DUPE", "PositionLabel": "Label 1 Dupe", "DWSeatID": 1002, "UpdateTimestamp": "2024-01-02T00:00:00Z"},
    {"TableId": 102, "PositionIDX": 2, "PositionCode": "P2", "PositionLabel": "Label 2", "DWSeatID": 1003, "UpdateTimestamp": "2024-01-01T00:00:00Z"},
    {"TableId": 103, "PositionIDX": None, "PositionCode": "P3", "PositionLabel": "Label 3", "DWSeatID": 1004, "UpdateTimestamp": "2024-01-01T00:00:00Z"}
]

# 3. Create SCD2 Mock Table (with intentional errors)
scd2_table_id = f"{dataset_id}.D_Employee_WD"
scd2_schema = [
    bigquery.SchemaField("UserId", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("UserName", "STRING"),
    bigquery.SchemaField("DWEmployeeID", "INTEGER"),
    bigquery.SchemaField("DWBeginEffDateTime", "TIMESTAMP"),
    bigquery.SchemaField("DWEndEffDateTime", "TIMESTAMP"),
    bigquery.SchemaField("DWCurrentRowFlag", "STRING")
]

# Data contains:
# - Valid chain: Row 1 -> Row 2
# - Multiple active flags -> Error
# - Overlapping dates -> Error
# - Invalid date order (Begin > End) -> Error
# - Gap in dates -> Error
scd2_data = [
    # Valid record
    {"UserId": "U1", "UserName": "User 1 Old", "DWEmployeeID": 5001, "DWBeginEffDateTime": "2023-01-01T00:00:00Z", "DWEndEffDateTime": "2023-06-01T00:00:00Z", "DWCurrentRowFlag": "N"},
    {"UserId": "U1", "UserName": "User 1 New", "DWEmployeeID": 5002, "DWBeginEffDateTime": "2023-06-01T00:00:00Z", "DWEndEffDateTime": "2099-12-31T23:59:59Z", "DWCurrentRowFlag": "Y"},
    
    # Overlapping Dates (U2)
    {"UserId": "U2", "UserName": "User 2 A", "DWEmployeeID": 5003, "DWBeginEffDateTime": "2023-01-01T00:00:00Z", "DWEndEffDateTime": "2023-08-01T00:00:00Z", "DWCurrentRowFlag": "N"},
    {"UserId": "U2", "UserName": "User 2 B", "DWEmployeeID": 5004, "DWBeginEffDateTime": "2023-07-01T00:00:00Z", "DWEndEffDateTime": "2099-12-31T23:59:59Z", "DWCurrentRowFlag": "Y"},
    
    # Multiple Active Flags (U3)
    {"UserId": "U3", "UserName": "User 3 A", "DWEmployeeID": 5005, "DWBeginEffDateTime": "2023-01-01T00:00:00Z", "DWEndEffDateTime": "2099-12-31T23:59:59Z", "DWCurrentRowFlag": "Y"},
    {"UserId": "U3", "UserName": "User 3 B", "DWEmployeeID": 5006, "DWBeginEffDateTime": "2023-06-01T00:00:00Z", "DWEndEffDateTime": "2099-12-31T23:59:59Z", "DWCurrentRowFlag": "Y"},
    
    # Invalid Date Order (U4)
    {"UserId": "U4", "UserName": "User 4", "DWEmployeeID": 5007, "DWBeginEffDateTime": "2023-12-01T00:00:00Z", "DWEndEffDateTime": "2023-01-01T00:00:00Z", "DWCurrentRowFlag": "Y"},

    # Gap (U5)
    {"UserId": "U5", "UserName": "User 5 A", "DWEmployeeID": 5008, "DWBeginEffDateTime": "2023-01-01T00:00:00Z", "DWEndEffDateTime": "2023-03-01T00:00:00Z", "DWCurrentRowFlag": "N"},
    {"UserId": "U5", "UserName": "User 5 B", "DWEmployeeID": 5009, "DWBeginEffDateTime": "2023-05-01T00:00:00Z", "DWEndEffDateTime": "2099-12-31T23:59:59Z", "DWCurrentRowFlag": "Y"}
]

def create_table_with_data(table_id, schema, data):
    table = bigquery.Table(f"leyin-sandpit.{table_id}", schema=schema)
    client.delete_table(table, not_found_ok=True)
    table = client.create_table(table)
    errors = client.insert_rows_json(table, data)
    if not errors:
        print(f"Table {table_id} version created and data inserted.")
    else:
        print(f"Errors inserting data into {table_id}: {errors}")

create_table_with_data(scd1_table_id, scd1_schema, scd1_data)
create_table_with_data(scd2_table_id, scd2_schema, scd2_data)
