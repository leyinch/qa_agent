-- setup_full_resources.sql (Consolidated Setup)
-- Run this in BigQuery Console to set up all datasets and tables for QA Agent (SCD, GCS, and History)


-- 1. Create Datasets
CREATE SCHEMA IF NOT EXISTS `leyin-sandpit.crown_scd_mock` OPTIONS(location="US");
CREATE SCHEMA IF NOT EXISTS `leyin-sandpit.transform_config` OPTIONS(location="US");
CREATE SCHEMA IF NOT EXISTS `leyin-sandpit.qa_agent_metadata` OPTIONS(location="US");
CREATE SCHEMA IF NOT EXISTS `leyin-sandpit.analytics` OPTIONS(location="US"); -- For GCS mock targets



-- 2. Setup SCD1 Mock Table
CREATE OR REPLACE TABLE `leyin-sandpit.crown_scd_mock.D_Seat_WD` (
    TableId INT64 NOT NULL,
    PositionIDX INT64,
    PositionCode STRING,
    PositionLabel STRING,
    DWSeatID INT64,
    UpdateTimestamp TIMESTAMP
);

INSERT INTO `leyin-sandpit.crown_scd_mock.D_Seat_WD` (TableId, PositionIDX, PositionCode, PositionLabel, DWSeatID, UpdateTimestamp)
VALUES
    (101, 1, 'P1', 'Label 1', 1001, '2024-01-01 00:00:00'),
    (101, 1, 'P1_DUPE', 'Label 1 Dupe', 1002, '2024-01-02 00:00:00'), -- DUPLICATE PRIMARY KEY (101, 1)
    (102, 2, 'P2', 'Label 2', 1003, '2024-01-01 00:00:00'),
    (103, CAST(NULL AS INT64), 'P3', 'Label 3', 1004, '2024-01-01 00:00:00'); -- NULL PRIMARY KEY

-- 3. Setup SCD2 Mock Table
CREATE OR REPLACE TABLE `leyin-sandpit.crown_scd_mock.D_Employee_WD` (
    UserId STRING NOT NULL,
    UserName STRING,
    DWEmployeeID INT64,
    DWBeginEffDateTime TIMESTAMP,
    DWEndEffDateTime TIMESTAMP,
    DWCurrentRowFlag STRING
);

INSERT INTO `leyin-sandpit.crown_scd_mock.D_Employee_WD` (UserId, UserName, DWEmployeeID, DWBeginEffDateTime, DWEndEffDateTime, DWCurrentRowFlag)
VALUES
    -- Valid record
    ('U1', 'User 1 Old', 5001, '2023-01-01 00:00:00', '2023-06-01 00:00:00', 'N'),
    ('U1', 'User 1 New', 5002, '2023-06-01 00:00:00', '2099-12-31 23:59:59', 'Y'),
    
    -- Overlapping Dates (U2)
    ('U2', 'User 2 A', 5003, '2023-01-01 00:00:00', '2023-08-01 00:00:00', 'N'),
    ('U2', 'User 2 B', 5004, '2023-07-01 00:00:00', '2099-12-31 23:59:59', 'Y'),
    
    -- Multiple Active Flags (U3)
    ('U3', 'User 3 A', 5005, '2023-01-01 00:00:00', '2099-12-31 23:59:59', 'Y'),
    ('U3', 'User 3 B', 5006, '2023-06-01 00:00:00', '2099-12-31 23:59:59', 'Y'),
    
    -- Invalid Date Order (U4)
    ('U4', 'User 4', 5007, '2023-12-01 00:00:00', '2023-01-01 00:00:00', 'Y'),

    -- Gap (U5)
    ('U5', 'User 5 A', 5008, '2023-01-01 00:00:00', '2023-03-01 00:00:00', 'N'),
    ('U5', 'User 5 B', 5009, '2023-05-01 00:00:00', '2099-12-31 23:59:59', 'Y');

-- 4. Setup SCD2 Mock Table D_Player_WD (with Business Rules)
CREATE OR REPLACE TABLE `leyin-sandpit.crown_scd_mock.D_Player_WD` (
    PlayerId STRING NOT NULL,
    PlayerName STRING,
    DWPlayerID INT64,
    DWBeginEffDateTime TIMESTAMP,
    DWEndEffDateTime TIMESTAMP,
    DWCurrentRowFlag STRING,
    -- Business Rule Columns
    CreatedDtm TIMESTAMP,
    UpdatedDtm TIMESTAMP
);

INSERT INTO `leyin-sandpit.crown_scd_mock.D_Player_WD` (PlayerId, PlayerName, DWPlayerID, DWBeginEffDateTime, DWEndEffDateTime, DWCurrentRowFlag, CreatedDtm, UpdatedDtm)
VALUES
    -- 1. Valid record chain (P1)
    ('P1', 'Player 1 Old', 6001, '2023-01-01 00:00:00', '2023-06-01 00:00:00', 'N', '2023-01-01 00:00:00', '2023-06-01 00:00:00'),
    ('P1', 'Player 1 New', 6002, '2023-06-01 00:00:00', '2099-12-31 23:59:59', 'Y', '2023-06-01 00:00:00', NULL),
    
    -- 2. Overlapping Dates (P2) -> SCD2 Error
    ('P2', 'Player 2 A', 6003, '2023-01-01 00:00:00', '2023-08-01 00:00:00', 'N', '2023-01-01 00:00:00', '2023-08-01 00:00:00'),
    ('P2', 'Player 2 B', 6004, '2023-07-01 00:00:00', '2099-12-31 23:59:59', 'Y', '2023-07-01 00:00:00', NULL),
    
    -- 3. Multiple Active Flags (P3) -> SCD2 Error
    ('P3', 'Player 3 A', 6005, '2023-01-01 00:00:00', '2099-12-31 23:59:59', 'Y', '2023-01-01 00:00:00', NULL),
    ('P3', 'Player 3 B', 6006, '2023-06-01 00:00:00', '2099-12-31 23:59:59', 'Y', '2023-06-01 00:00:00', NULL),
    
    -- 4. Invalid Date Order (P4) -> SCD2 Error
    ('P4', 'Player 4', 6007, '2023-12-01 00:00:00', '2023-01-01 00:00:00', 'Y', '2023-12-01 00:00:00', NULL),

    -- 5. Gap (P5) -> SCD2 Error
    ('P5', 'Player 5 A', 6008, '2023-01-01 00:00:00', '2023-03-01 00:00:00', 'N', '2023-01-01 00:00:00', '2023-03-01 00:00:00'),
    ('P5', 'Player 5 B', 6009, '2023-05-01 00:00:00', '2099-12-31 23:59:59', 'Y', '2023-05-01 00:00:00', NULL),

    -- 6. Business Rule Failures
    -- P6: CreatedDtm IS NULL
    ('P6', 'Player 6 Null Created', 6010, '2023-01-01 00:00:00', '2099-12-31 23:59:59', 'Y', NULL, '2023-01-01 00:00:00'),
    
    -- P7: CreatedDtm > UpdatedDtm
    ('P7', 'Player 7 Future Created', 6011, '2023-01-01 00:00:00', '2099-12-31 23:59:59', 'Y', '2023-02-01 00:00:00', '2023-01-01 00:00:00');

-- 5. Setup SCD Validation Config Table

CREATE OR REPLACE TABLE `leyin-sandpit.transform_config.scd_validation_config` (
    config_id STRING NOT NULL,
    target_dataset STRING NOT NULL,
    target_table STRING NOT NULL,
    scd_type STRING NOT NULL,
    primary_keys ARRAY<STRING>,
    surrogate_key STRING,
    begin_date_column STRING,
    end_date_column STRING,
    active_flag_column STRING,
    description STRING,
    custom_tests JSON,
    cron_schedule STRING
);

INSERT INTO `leyin-sandpit.transform_config.scd_validation_config` (config_id, target_dataset, target_table, scd_type, primary_keys, surrogate_key, begin_date_column, end_date_column, active_flag_column, description, custom_tests, cron_schedule)
VALUES
    ('seat_scd1', 'crown_scd_mock', 'D_Seat_WD', 'scd1', ['TableId', 'PositionIDX'], 'DWSeatID', NULL, NULL, NULL, 'SCD1 Mock for Gaming Seats (Test Data)', NULL, '0 9 * * *'),
    ('employee_scd2', 'crown_scd_mock', 'D_Employee_WD', 'scd2', ['UserId'], 'DWEmployeeID', 'DWBeginEffDateTime', 'DWEndEffDateTime', 'DWCurrentRowFlag', 'SCD2 Mock for Employees (Test Data)', NULL, '0 9 * * *'),
    ('player_scd2', 'crown_scd_mock', 'D_Player_WD', 'scd2', ['PlayerId'], 'DWPlayerID', 'DWBeginEffDateTime', 'DWEndEffDateTime', 'DWCurrentRowFlag', 'SCD2 Mock for Players (Test Data)', JSON """[
    {
        "name": "CreatedDtm Not Null",
        "sql": "SELECT * FROM {{target}} WHERE CreatedDtm IS NULL",
        "description": "CreatedDtm must not be null",
        "severity": "HIGH"
    },
    {
        "name": "CreatedDtm before UpdatedDtm",
        "sql": "SELECT * FROM {{target}} WHERE CreatedDtm > UpdatedDtm",
        "description": "CreatedDtm must be less than or equal to UpdatedDtm",
        "severity": "HIGH"
    }
]""", '0 9 * * *');


-- 6. Setup GCS Load Mappings Configuration

CREATE TABLE IF NOT EXISTS `leyin-sandpit.transform_config.data_load_config` (
  mapping_id STRING NOT NULL,
  mapping_name STRING,
  description STRING,
  source_bucket STRING NOT NULL,
  source_file_path STRING NOT NULL,
  source_file_format STRING NOT NULL,
  target_dataset STRING NOT NULL,
  target_table STRING NOT NULL,
  primary_key_columns ARRAY<STRING>,
  required_columns ARRAY<STRING>,
  date_columns ARRAY<STRING>,
  numeric_range_checks JSON,
  date_range_checks JSON,
  foreign_key_checks JSON,
  pattern_checks JSON,
  outlier_columns ARRAY<STRING>,
  enabled_test_ids ARRAY<STRING>,
  auto_suggest BOOLEAN DEFAULT true,
  is_active BOOLEAN DEFAULT true,
  cron_schedule STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO `leyin-sandpit.transform_config.data_load_config`
(mapping_id, mapping_name, description, source_bucket, source_file_path, source_file_format, target_dataset, target_table, primary_key_columns, required_columns, enabled_test_ids, cron_schedule)
VALUES (
  'customers_daily', 'Daily Customer Load', 'Sample GCS workload', 
  'leyin-sandpit-raw', 'customers/*.csv', 'csv', 
  'analytics', 'customers', 
  ['customer_id'], ['customer_id', 'email'],
  ['row_count_match', 'no_nulls_required', 'no_duplicates_pk'],
  '0 9 * * *'
);


-- 7. Setup System Predefined Tests
CREATE TABLE IF NOT EXISTS `leyin-sandpit.transform_config.predefined_tests` (
  test_id STRING NOT NULL,
  test_name STRING NOT NULL,
  test_category STRING NOT NULL,
  severity STRING NOT NULL,
  description STRING,
  sql_template STRING,
  is_global BOOLEAN DEFAULT false,
  is_system BOOLEAN DEFAULT true,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO `leyin-sandpit.transform_config.predefined_tests` 
(test_id, test_name, test_category, severity, description, is_global, is_system)
VALUES
('row_count_match', 'Row Count Match', 'completeness', 'HIGH', 'Verify source and target row counts match', true, true),
('no_nulls_required', 'No NULLs in Required Fields', 'completeness', 'HIGH', 'Check required columns have no NULL values', true, true),
('no_duplicates_pk', 'No Duplicate Primary Keys', 'integrity', 'HIGH', 'Ensure primary key uniqueness', true, true),
('referential_integrity', 'Referential Integrity', 'integrity', 'HIGH', 'Validate foreign key relationships', false, true);

-- 8. Setup AI Suggested Tests
CREATE TABLE IF NOT EXISTS `leyin-sandpit.transform_config.suggested_tests` (
  suggestion_id STRING NOT NULL,
  mapping_id STRING NOT NULL,
  test_name STRING NOT NULL,
  test_category STRING NOT NULL,
  severity STRING NOT NULL,
  sql_query STRING NOT NULL,
  reasoning STRING,
  status STRING DEFAULT 'pending',
  suggested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);



-- 9. Setup Test Results History Table

CREATE TABLE IF NOT EXISTS `leyin-sandpit.qa_agent_metadata.test_results_history` (
  execution_id STRING NOT NULL,
  execution_timestamp TIMESTAMP NOT NULL,
  project_id STRING NOT NULL,
  comparison_mode STRING NOT NULL,
  target_dataset STRING,
  target_table STRING,
  mapping_id STRING,
  status STRING NOT NULL,
  total_tests INT64,
  passed_tests INT64,
  failed_tests INT64,
  error_message STRING,
  cron_schedule STRING,
  test_results JSON,
  executed_by STRING,
  metadata JSON
)
PARTITION BY DATE(execution_timestamp)
CLUSTER BY project_id, target_table, status;

-- 10. Setup Latest Results View

CREATE OR REPLACE VIEW `leyin-sandpit.qa_agent_metadata.latest_test_results_by_table` AS
SELECT 
  t.*
FROM `leyin-sandpit.qa_agent_metadata.test_results_history` t
INNER JOIN (
  SELECT 
    project_id,
    target_dataset,
    target_table,
    MAX(execution_timestamp) as latest_execution
  FROM `leyin-sandpit.qa_agent_metadata.test_results_history`
  WHERE target_table IS NOT NULL
  GROUP BY project_id, target_dataset, target_table
) latest
ON t.project_id = latest.project_id
  AND t.target_dataset = latest.target_dataset
  AND t.target_table = latest.target_table
  AND t.execution_timestamp = latest.latest_execution;
