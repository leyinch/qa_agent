-- setup_scd_resources.sql
-- Run this in BigQuery Console to set up datasets and tables for SCD testing

-- 1. Create Datasets
CREATE SCHEMA IF NOT EXISTS `leyin-sandpit.crown_scd_mock` OPTIONS(location="US");
CREATE SCHEMA IF NOT EXISTS `leyin-sandpit.transform_config` OPTIONS(location="US");

-- 2. Setup SCD1 Mock Table
CREATE OR REPLACE TABLE `leyin-sandpit.crown_scd_mock.scd1_mock` (
    TableId INT64 NOT NULL,
    PositionIDX INT64,
    PositionCode STRING,
    PositionLabel STRING,
    DWSeatID INT64,
    UpdateTimestamp TIMESTAMP
);

INSERT INTO `leyin-sandpit.crown_scd_mock.scd1_mock` (TableId, PositionIDX, PositionCode, PositionLabel, DWSeatID, UpdateTimestamp)
VALUES
    (101, 1, 'P1', 'Label 1', 1001, '2024-01-01 00:00:00'),
    (101, 1, 'P1_DUPE', 'Label 1 Dupe', 1002, '2024-01-02 00:00:00'), -- DUPLICATE NATURAL KEY (101, 1)
    (102, 2, 'P2', 'Label 2', 1003, '2024-01-01 00:00:00'),
    (103, CAST(NULL AS INT64), 'P3', 'Label 3', 1004, '2024-01-01 00:00:00'); -- NULL NATURAL KEY

-- 3. Setup SCD2 Mock Table
CREATE OR REPLACE TABLE `leyin-sandpit.crown_scd_mock.scd2_mock` (
    UserId STRING NOT NULL,
    UserName STRING,
    DWEmployeeID INT64,
    DWBeginEffDateTime TIMESTAMP,
    DWEndEffDateTime TIMESTAMP,
    DWCurrentRowFlag STRING
);

INSERT INTO `leyin-sandpit.crown_scd_mock.scd2_mock` (UserId, UserName, DWEmployeeID, DWBeginEffDateTime, DWEndEffDateTime, DWCurrentRowFlag)
VALUES
    -- Valid record
    ('U1', 'User 1 Old', 5001, '2023-01-01 00:00:00', '2023-06-01 00:00:00', 'N'),
    ('U1', 'User 1 New', 5002, '2023-06-01 00:00:00', '9999-12-31 23:59:59', 'Y'),
    
    -- Overlapping Dates (U2)
    ('U2', 'User 2 A', 5003, '2023-01-01 00:00:00', '2023-08-01 00:00:00', 'N'),
    ('U2', 'User 2 B', 5004, '2023-07-01 00:00:00', '9999-12-31 23:59:59', 'Y'),
    
    -- Multiple Active Flags (U3)
    ('U3', 'User 3 A', 5005, '2023-01-01 00:00:00', '9999-12-31 23:59:59', 'Y'),
    ('U3', 'User 3 B', 5006, '2023-06-01 00:00:00', '9999-12-31 23:59:59', 'Y'),
    
    -- Invalid Date Order (U4)
    ('U4', 'User 4', 5007, '2023-12-01 00:00:00', '2023-01-01 00:00:00', 'Y'),

    -- Gap (U5)
    ('U5', 'User 5 A', 5008, '2023-01-01 00:00:00', '2023-03-01 00:00:00', 'N'),
    ('U5', 'User 5 B', 5009, '2023-05-01 00:00:00', '9999-12-31 23:59:59', 'Y');

-- 4. Setup SCD Validation Config Table
CREATE OR REPLACE TABLE `leyin-sandpit.transform_config.scd_validation_config` (
    config_id STRING NOT NULL,
    target_dataset STRING NOT NULL,
    target_table STRING NOT NULL,
    scd_type STRING NOT NULL,
    natural_keys ARRAY<STRING>,
    surrogate_key STRING,
    begin_date_column STRING,
    end_date_column STRING,
    active_flag_column STRING,
    description STRING
);

INSERT INTO `leyin-sandpit.transform_config.scd_validation_config` (config_id, target_dataset, target_table, scd_type, natural_keys, surrogate_key, begin_date_column, end_date_column, active_flag_column, description)
VALUES
    ('seat_scd1', 'DW_Dimensions', 'D_Seat_WD', 'scd1', ['TableId', 'PositionIDX', 'PositionCode', 'PositionLabel'], 'DWSeatID', NULL, NULL, NULL, 'SCD1 for Gaming Seats'),
    ('employee_scd2', 'DW_Dimensions', 'D_Employee_WD', 'scd2', ['UserId'], 'DWEmployeeID', 'DWBeginEffDateTime', 'DWEndEffDateTime', 'DWCurrentRowFlag', 'SCD2 for Employees'),
    ('player_scd2', 'DW_Dimensions', 'D_Player_WD', 'scd2', ['PlayerId'], 'DWPlayerID', 'DWBeginEffDateTime', 'DWEndEffDateTime', 'DWCurrentRowFlag', 'SCD2 for Players with Business Rules');
