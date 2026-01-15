# Config Tables Setup Guide

## Overview

This guide explains how to set up the configuration tables for the Data QA Agent's SCD (Slowly Changing Dimension) validation system.

## Quick Start

1. **Run the setup script** in BigQuery:
   ```bash
   # Open BigQuery Console
   # Copy and paste the contents of setup_scd_resources.sql
   # Execute the script
   ```

2. **Verify tables were created**:
   ```sql
   SELECT table_name 
   FROM `[YOUR_PROJECT_ID].config.INFORMATION_SCHEMA.TABLES`;
   ```

## Config Tables

### 1. `scd_validation_config` - Main Configuration Table

Stores SCD validation settings for each target table.

**Key columns:**
- `config_id`: Unique identifier for the table config
- `target_dataset`, `target_table`: Target table to validate
- `scd_type`: 'scd1' or 'scd2'
- `primary_keys`: Columns that form the unique key
- `surrogate_key`: The surrogate key column (SCD2)
- `begin_date_column`, `end_date_column`: Timeline tracking columns (SCD2)
- `active_flag_column`: Current row indicator (SCD2)
- `custom_tests`: JSON array of custom business logic rules
- `cron_schedule`: Automation frequency

**Example:**
```sql
SELECT * FROM `[YOUR_PROJECT_ID].config.scd_validation_config` 
WHERE config_id = 'employee_scd2';
```

### 2. `scd_test_history` - Test Execution History

Tracks **SCD test executions only** (both scheduled and manual runs). GCS and Schema Validation tests are not logged here.
- **Location**: `[YOUR_PROJECT_ID].qa_results.scd_test_history`
- **Maintenance**: Use the "Clear History" button in the frontend to truncate this table if needed.

## Managing Configurations

### Adding a New Table
You can add new tables via the UI dashboard or directly in SQL:

```sql
INSERT INTO `[YOUR_PROJECT_ID].config.scd_validation_config`
(config_id, target_dataset, target_table, scd_type, primary_keys, cron_schedule)
VALUES
('my_new_table', 'my_dataset', 'my_table', 'scd1', ['id'], '0 9 * * *');
```

## Maintenance

### View Test History
```sql
SELECT * FROM `[YOUR_PROJECT_ID].qa_results.scd_test_history`
WHERE execution_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
ORDER BY execution_timestamp DESC
LIMIT 100;
```
