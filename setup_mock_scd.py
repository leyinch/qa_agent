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

# 4. Create SCD2 Mock Table D_Player_WD (with intentional errors + Business Rules)
player_table_id = f"{dataset_id}.D_Player_WD"
player_schema = [
    bigquery.SchemaField("PlayerId", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("PlayerName", "STRING"),
    bigquery.SchemaField("DWPlayerID", "INTEGER"),
    bigquery.SchemaField("DWBeginEffDateTime", "TIMESTAMP"),
    bigquery.SchemaField("DWEndEffDateTime", "TIMESTAMP"),
    bigquery.SchemaField("DWCurrentRowFlag", "STRING"),
    # Business Rule Columns
    bigquery.SchemaField("CreatedDtm", "TIMESTAMP"),
    bigquery.SchemaField("UpdatedDtm", "TIMESTAMP")
]

# Data contains:
# - Valid chain: Row 1 -> Row 2
# - SCD2 Errors (inherited from Employee patterns):
#   - Overlapping dates
#   - Multiple active flags
#   - Invalid date order
#   - Gap in dates
# - Business Rule Errors:
#   - CreatedDtm IS NULL
#   - CreatedDtm > UpdatedDtm
player_data = [
    # 1. Valid record chain (P1)
    {
        "PlayerId": "P1", "PlayerName": "Player 1 Old", "DWPlayerID": 6001, 
        "DWBeginEffDateTime": "2023-01-01T00:00:00Z", "DWEndEffDateTime": "2023-06-01T00:00:00Z", "DWCurrentRowFlag": "N",
        "CreatedDtm": "2023-01-01T00:00:00Z", "UpdatedDtm": "2023-06-01T00:00:00Z"
    },
    {
        "PlayerId": "P1", "PlayerName": "Player 1 New", "DWPlayerID": 6002, 
        "DWBeginEffDateTime": "2023-06-01T00:00:00Z", "DWEndEffDateTime": "2099-12-31T23:59:59Z", "DWCurrentRowFlag": "Y",
        "CreatedDtm": "2023-06-01T00:00:00Z", "UpdatedDtm": None # Valid (UpdatedDtm null for current)
    },
    
    # 2. Overlapping Dates (P2) -> SCD2 Error
    {
        "PlayerId": "P2", "PlayerName": "Player 2 A", "DWPlayerID": 6003, 
        "DWBeginEffDateTime": "2023-01-01T00:00:00Z", "DWEndEffDateTime": "2023-08-01T00:00:00Z", "DWCurrentRowFlag": "N",
        "CreatedDtm": "2023-01-01T00:00:00Z", "UpdatedDtm": "2023-08-01T00:00:00Z"
    },
    {
        "PlayerId": "P2", "PlayerName": "Player 2 B", "DWPlayerID": 6004, 
        "DWBeginEffDateTime": "2023-07-01T00:00:00Z", "DWEndEffDateTime": "2099-12-31T23:59:59Z", "DWCurrentRowFlag": "Y",
        "CreatedDtm": "2023-07-01T00:00:00Z", "UpdatedDtm": None
    },
    
    # 3. Multiple Active Flags (P3) -> SCD2 Error
    {
        "PlayerId": "P3", "PlayerName": "Player 3 A", "DWPlayerID": 6005, 
        "DWBeginEffDateTime": "2023-01-01T00:00:00Z", "DWEndEffDateTime": "2099-12-31T23:59:59Z", "DWCurrentRowFlag": "Y",
        "CreatedDtm": "2023-01-01T00:00:00Z", "UpdatedDtm": None
    },
    {
        "PlayerId": "P3", "PlayerName": "Player 3 B", "DWPlayerID": 6006, 
        "DWBeginEffDateTime": "2023-06-01T00:00:00Z", "DWEndEffDateTime": "2099-12-31T23:59:59Z", "DWCurrentRowFlag": "Y",
        "CreatedDtm": "2023-06-01T00:00:00Z", "UpdatedDtm": None
    },
    
    # 4. Invalid Date Order (P4) -> SCD2 Error
    {
        "PlayerId": "P4", "PlayerName": "Player 4", "DWPlayerID": 6007, 
        "DWBeginEffDateTime": "2023-12-01T00:00:00Z", "DWEndEffDateTime": "2023-01-01T00:00:00Z", "DWCurrentRowFlag": "Y",
        "CreatedDtm": "2023-12-01T00:00:00Z", "UpdatedDtm": None
    },

    # 5. Gap (P5) -> SCD2 Error
    {
        "PlayerId": "P5", "PlayerName": "Player 5 A", "DWPlayerID": 6008, 
        "DWBeginEffDateTime": "2023-01-01T00:00:00Z", "DWEndEffDateTime": "2023-03-01T00:00:00Z", "DWCurrentRowFlag": "N",
        "CreatedDtm": "2023-01-01T00:00:00Z", "UpdatedDtm": "2023-03-01T00:00:00Z"
    },
    {
        "PlayerId": "P5", "PlayerName": "Player 5 B", "DWPlayerID": 6009, 
        "DWBeginEffDateTime": "2023-05-01T00:00:00Z", "DWEndEffDateTime": "2099-12-31T23:59:59Z", "DWCurrentRowFlag": "Y",
        "CreatedDtm": "2023-05-01T00:00:00Z", "UpdatedDtm": None
    },

    # 6. Business Rule Failures
    # P6: CreatedDtm IS NULL
    {
        "PlayerId": "P6", "PlayerName": "Player 6 Null Created", "DWPlayerID": 6010, 
        "DWBeginEffDateTime": "2023-01-01T00:00:00Z", "DWEndEffDateTime": "2099-12-31T23:59:59Z", "DWCurrentRowFlag": "Y",
        "CreatedDtm": None, "UpdatedDtm": "2023-01-01T00:00:00Z"
    },
    
    # P7: CreatedDtm > UpdatedDtm
    {
        "PlayerId": "P7", "PlayerName": "Player 7 Future Created", "DWPlayerID": 6011, 
        "DWBeginEffDateTime": "2023-01-01T00:00:00Z", "DWEndEffDateTime": "2099-12-31T23:59:59Z", "DWCurrentRowFlag": "Y",
        "CreatedDtm": "2023-02-01T00:00:00Z", "UpdatedDtm": "2023-01-01T00:00:00Z"
    }
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
create_table_with_data(player_table_id, player_schema, player_data)
