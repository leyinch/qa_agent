# Config Tables Setup Guide

## Overview

This guide explains how to set up the configuration tables for the Data QA Agent's hybrid testing system.

## Quick Start

1. **Run the setup script** in BigQuery:
    - For **Standard/GCS Mappings**: Use [`config_tables_setup.sql`](./config_tables_setup.sql).
    - For **SCD Validation & History**: Use [`setup_scd_test_resources.sql`](./setup_scd_test_resources.sql) (Consolidated Setup).
    - *Note: Update project IDs and placeholders in the scripts before running.*

2. **Verify tables were created**:
   ```sql
   SELECT table_name 
   FROM `YOUR_PROJECT_ID.config.INFORMATION_SCHEMA.TABLES`;
   ```

## Config Tables

### 1. `data_load_config` - Main Configuration Table

Stores GCS-to-BigQuery mappings and test configurations.

**Key columns:**
- `mapping_id`: Unique identifier for each data load
- `source_bucket`, `source_file_path`: GCS source location (if applicable)
- `source_project`, `source_dataset`, `source_table`: BigQuery source location (if applicable)
- `target_dataset`, `target_table`: BigQuery destination
- `enabled_test_ids`: Which predefined tests to run
- `auto_suggest`: Enable/disable AI test suggestions
- `outlier_columns`: Columns to check for statistical outliers

**Example:**
```sql
SELECT * FROM `YOUR_PROJECT_ID.config.data_load_config` 
WHERE is_active = true;
```

### 2. `predefined_tests` - Test Definitions

System-wide test definitions (8 standard tests included).

**Standard tests:**
- Row Count Match
- No NULLs in Required Fields
- No Duplicate Primary Keys
- Referential Integrity
- Numeric Range Validation
- Date Range Validation
- Pattern Validation
- Statistical Outlier Detection

### 3. `suggested_tests` - AI Suggestions

Stores AI-suggested tests pending user approval.

### 4. `test_execution_history` - Audit Trail

Tracks all test executions for monitoring and debugging.

### 5. `scd_validation_config` - SCD Specific Configuration

Stores configurations for Slowly Changing Dimension (SCD) Type 1 and Type 2 validation.

**Key columns:**
- `mapping_id`: Unique identifier for the SCD validation.
- `target_dataset`, `target_table`: The dimension table to validate.
- `scd_type`: `SCD1` or `SCD2`.
- `primary_keys`: Array of columns forming the natural key.
- `surrogate_key`: The unique identifier for the specific version of a record.
- `begin_date_column`, `end_date_column`, `active_flag_column`: (SCD2 only) Temporal tracking columns.

## Adding a New Mapping

### GCS/Standard Mapping:
```sql
INSERT INTO `YOUR_PROJECT_ID.config.data_load_config`
(mapping_id, mapping_name, source_bucket, source_file_path, source_file_format,
 target_dataset, target_table, primary_key_columns, required_columns,
 enabled_test_ids, is_active)
VALUES
('my_data_load', 'My Data Load', 'my-bucket', 'raw/data.csv', 'csv',
 'analytics', 'my_table', ['id'], ['id', 'name'],
 ['row_count_match', 'no_nulls_required', 'no_duplicates_pk'], true);
```

### SCD Mapping:
```sql
INSERT INTO `YOUR_PROJECT_ID.config.scd_validation_config`
(mapping_id, target_dataset, target_table, scd_type, primary_keys, surrogate_key, 
 begin_date_column, end_date_column, active_flag_column, is_active)
VALUES
('employee_dim_v1', 'crown_scd_mock', 'D_Employee_WD', 'SCD2', ['UserId'], 'DWEmployeeID',
 'DWBeginEffDateTime', 'DWEndEffDateTime', 'DWCurrentRowFlag', true);
```

## Test Configuration Examples

### Basic Configuration
```sql
-- Minimal config - only global tests
enabled_test_ids: ['row_count_match', 'no_nulls_required', 'no_duplicates_pk']
```

### With Range Checks
```sql
-- Add numeric and date validations
enabled_test_ids: ['row_count_match', 'no_nulls_required', 'numeric_range', 'date_range']
numeric_range_checks: JSON '{"age": {"min": 0, "max": 120}, "price": {"min": 0, "max": 10000}}'
date_range_checks: JSON '{"order_date": {"min_date": "2024-01-01", "max_date": "2024-12-31"}}'
```

### With Pattern Validation
```sql
-- Validate email and phone formats
enabled_test_ids: ['row_count_match', 'pattern_validation']
pattern_checks: JSON '{"email": "^[^@]+@[^@]+\\\\.[^@]+$", "phone": "^\\\\+?[0-9]{10,15}$"}'
```

### With Foreign Keys
```sql
-- Check referential integrity
enabled_test_ids: ['row_count_match', 'referential_integrity']
foreign_key_checks: JSON '{"customer_id": {"table": "analytics.customers", "column": "id"}}'
```

### With Outlier Detection
```sql
-- Check for statistical outliers (2 standard deviations)
enabled_test_ids: ['row_count_match', 'outlier_detection']
outlier_columns: ['transaction_amount', 'processing_time_ms']
```

## Using in the App

1. **GCS/Standard Mapping**: Select "GCS File Comparison" mode -> Choose "Config Table" -> Enter the config table coordinates.
2. **SCD Validation**: Select "SCD Validation" mode -> Choose "Config Table" -> Enter the SCD config table coordinates.

## Maintenance

### View Active Mappings
```sql
SELECT mapping_id, mapping_name, target_table, is_active
FROM `YOUR_PROJECT_ID.config.data_load_config`
WHERE is_active = true;
```

### Disable a Mapping
```sql
UPDATE `YOUR_PROJECT_ID.config.data_load_config`
SET is_active = false, updated_at = CURRENT_TIMESTAMP()
WHERE mapping_id = 'my_data_load';
```
