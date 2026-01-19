-- setup_full_resources.sql (Consolidated Setup)
-- Run this in BigQuery Console to set up all datasets and tables for QA Agent (SCD, GCS, and History)


-- 1. Create Datasets
CREATE SCHEMA IF NOT EXISTS `{{PROJECT_ID}}.crown_scd_mock` OPTIONS(location="US"); -- Mock tables for testing SCD Type 1 and Type 2 validation logic
CREATE SCHEMA IF NOT EXISTS `{{PROJECT_ID}}.config` OPTIONS(location="US");         -- Centralized configuration tables for the QA Agent (SCD, GCS, and system-wide tests)
CREATE SCHEMA IF NOT EXISTS `{{PROJECT_ID}}.qa_results` OPTIONS(location="US");     -- History and reporting views for test execution results

-- ============================================
-- CLEANUP: Drop existing tables/views for clean re-deployment
-- ============================================
DROP TABLE IF EXISTS `{{PROJECT_ID}}.config.scd_validation_config`;
DROP TABLE IF EXISTS `{{PROJECT_ID}}.qa_results.scd_test_history`;
DROP VIEW IF EXISTS `{{PROJECT_ID}}.qa_results.latest_scd_results_by_table`;
DROP VIEW IF EXISTS `{{PROJECT_ID}}.qa_results.v_scd_validation_report`;



-- 2. Setup SCD1 Mock Table
CREATE OR REPLACE TABLE `{{PROJECT_ID}}.crown_scd_mock.D_Seat_WD` (
    TableId INT64 NOT NULL,
    PositionIDX INT64,
    PositionCode STRING,
    PositionLabel STRING,
    DWSeatID INT64,
    UpdateTimestamp TIMESTAMP
);

INSERT INTO `{{PROJECT_ID}}.crown_scd_mock.D_Seat_WD` (TableId, PositionIDX, PositionCode, PositionLabel, DWSeatID, UpdateTimestamp)
VALUES
    (101, 1, 'P1', 'Label 1', 1001, '2024-01-01 00:00:00'),
    (101, 1, 'P1_DUPE', 'Label 1 Dupe', 1002, '2024-01-02 00:00:00'), -- DUPLICATE PRIMARY KEY (101, 1)
    (102, 2, 'P2', 'Label 2', 1003, '2024-01-01 00:00:00'),
    (103, CAST(NULL AS INT64), 'P3', 'Label 3', 1004, '2024-01-01 00:00:00'); -- NULL PRIMARY KEY

-- 3. Setup SCD2 Mock Table
CREATE OR REPLACE TABLE `{{PROJECT_ID}}.crown_scd_mock.D_Employee_WD` (
    UserId STRING NOT NULL,
    UserName STRING,
    DWEmployeeID INT64,
    DWBeginEffDateTime TIMESTAMP,
    DWEndEffDateTime TIMESTAMP,
    DWCurrentRowFlag STRING
);

INSERT INTO `{{PROJECT_ID}}.crown_scd_mock.D_Employee_WD` (UserId, UserName, DWEmployeeID, DWBeginEffDateTime, DWEndEffDateTime, DWCurrentRowFlag)
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
CREATE OR REPLACE TABLE `{{PROJECT_ID}}.crown_scd_mock.D_Player_WD` (
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

INSERT INTO `{{PROJECT_ID}}.crown_scd_mock.D_Player_WD` (PlayerId, PlayerName, DWPlayerID, DWBeginEffDateTime, DWEndEffDateTime, DWCurrentRowFlag, CreatedDtm, UpdatedDtm)
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
CREATE TABLE IF NOT EXISTS `{{PROJECT_ID}}.config.scd_validation_config` (
  config_id STRING NOT NULL,
  target_dataset STRING NOT NULL,
  target_table STRING NOT NULL,
  scd_type STRING NOT NULL,         -- 'scd1' or 'scd2'
  primary_keys ARRAY<STRING>,       -- List of primary key columns
  surrogate_key STRING,             -- Surrogate key column (SCD2)
  begin_date_column STRING,         -- Effective begin date (SCD2)
  end_date_column STRING,           -- Effective end date (SCD2)
  active_flag_column STRING,        -- Current row flag (SCD2)
  description STRING,               -- Friendly description
  custom_tests JSON,                -- Array of custom business rules
  
  -- Metadata
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 6. Setup Test Execution History (SCD Only)
CREATE TABLE IF NOT EXISTS `{{PROJECT_ID}}.qa_results.scd_test_history` (
  execution_id STRING NOT NULL,
  execution_timestamp DATETIME NOT NULL,
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
  test_results JSON,
  executed_by STRING,
  metadata JSON
)
PARTITION BY DATE(execution_timestamp)
CLUSTER BY project_id, target_table, status;

-- 7. Setup Reporting Views
CREATE OR REPLACE VIEW `{{PROJECT_ID}}.qa_results.latest_scd_results_by_table` AS
SELECT 
  t.*
FROM `{{PROJECT_ID}}.qa_results.scd_test_history` t
INNER JOIN (
  SELECT 
    project_id,
    target_dataset,
    target_table,
    MAX(execution_timestamp) as latest_execution
  FROM `{{PROJECT_ID}}.qa_results.scd_test_history`
  WHERE target_table IS NOT NULL
  GROUP BY project_id, target_dataset, target_table
) latest
ON t.project_id = latest.project_id
  AND t.target_dataset = latest.target_dataset
  AND t.target_table = latest.target_table
  AND t.execution_timestamp = latest.latest_execution;

CREATE OR REPLACE VIEW `{{PROJECT_ID}}.qa_results.v_scd_validation_report` AS
SELECT 
    FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', execution_timestamp) as execution_time,
    target_table,
    executed_by,
    STRING(test.test_name) as test_name,
    STRING(test.status) as status,
    CASE 
        WHEN STRING(test.test_id) = 'table_exists' AND STRING(test.status) = 'PASS' 
            THEN 'Table is online and accessible'
        WHEN JSON_VALUE(test.rows_affected) = '0' 
            THEN 'Check passed - no issues found'
        ELSE TO_JSON_STRING(test.sample_data)
    END as validation_findings
FROM `{{PROJECT_ID}}.qa_results.scd_test_history`,
UNNEST(JSON_QUERY_ARRAY(test_results)) AS test;

-- 8. Setup Sample SCD Configurations
INSERT INTO `{{PROJECT_ID}}.config.scd_validation_config` 
(config_id, target_dataset, target_table, scd_type, primary_keys, surrogate_key, begin_date_column, end_date_column, active_flag_column, description, custom_tests)
SELECT * FROM (
  SELECT 'seat_scd1' as config_id, 'crown_scd_mock' as target_dataset, 'D_Seat_WD' as target_table, 'scd1' as scd_type, ['TableId', 'PositionIDX'] as primary_keys, 'DWSeatID' as surrogate_key, CAST(NULL AS STRING) as begin_date_column, CAST(NULL AS STRING) as end_date_column, CAST(NULL AS STRING) as active_flag_column, 'SCD1 Mock for Gaming Seats (Test Data)' as description, CAST(NULL AS JSON) as custom_tests
  UNION ALL
  SELECT 'employee_scd2', 'crown_scd_mock', 'D_Employee_WD', 'scd2', ['UserId'], 'DWEmployeeID', 'DWBeginEffDateTime', 'DWEndEffDateTime', 'DWCurrentRowFlag', 'SCD2 Mock for Employees (Test Data)', NULL
  UNION ALL
  SELECT 'player_scd2', 'crown_scd_mock', 'D_Player_WD', 'scd2', ['PlayerId'], 'DWPlayerID', 'DWBeginEffDateTime', 'DWEndEffDateTime', 'DWCurrentRowFlag', 'SCD2 Mock for Players with Business Rules', 
    JSON """[
    {
        "name": "CreatedDtm Not Null",
        "sql": "SELECT * FROM {{target}} WHERE CreatedDtm IS NULL",
        "description": "CreatedDtm should never be null",
        "severity": "HIGH"
    },
    {
        "name": "CreatedDtm before UpdatedDtm",
        "sql": "SELECT * FROM {{target}} WHERE CreatedDtm > UpdatedDtm",
        "description": "CreatedDtm must be less than or equal to UpdatedDtm",
        "severity": "HIGH"
    }
]"""
) AS t
WHERE NOT EXISTS (SELECT 1 FROM `{{PROJECT_ID}}.config.scd_validation_config` WHERE config_id = t.config_id);
