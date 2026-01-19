# Config Tables Setup Guide

## Overview

This guide explains how to set up the configuration tables for the Data QA Agent. The setup is now consolidated into two main scripts:
1.  **`config_tables_setup.sql`**: Creates system datasets, configuration tables, and predefined tests.
2.  **`setup_scd_resources.sql`**: Creates mock datasets and tables for testing SCD validation.

## Quick Start

1.  **Parameterize your SQL files**:
    Run this in your terminal or Cloud Shell to ensure your `PROJECT_ID` is correctly set in the scripts:
    ```bash
    ./parameterize-sql.sh
    ```

2.  **Run the Setup Scripts**:
    Run these commands in order (using `bq` CLI or copy-pasting to BigQuery Console):
    ```bash
    # 1. Setup System Tables
    bq query --use_legacy_sql=false --project_id=your-project-id < config_tables_setup.generated.sql

    # 2. Setup Mock Resources (Optional for testing)
    bq query --use_legacy_sql=false --project_id=your-project-id < setup_scd_resources.generated.sql
    ```

## System Tables

### 1. `config.scd_validation_config`
Stores settings for SCD Type 1 and Type 2 validation.

**Columns:**
- `config_id`: Unique identifier for the configuration
- `target_dataset`, `target_table`: The BigQuery table to validate
- `scd_type`: `'scd1'` or `'scd2'`
- `primary_keys`: Array of columns forming the grain of the table
- `surrogate_key`: (SCD2) Unique key for the version
- `begin_date_column`, `end_date_column`: (SCD2) Effective history dates
- `active_flag_column`: (SCD2) Current version indicator
- `custom_tests`: JSON array of business logic rules
- `description`: Human-readable notes

### 2. `qa_results.scd_test_history`
Centralized log for all test executions. This table is partitioned by day for performance.

### 3. `config.data_load_config`
Stores configuration for GCS and Schema validation mappings.

## Views for Reporting

- `qa_results.latest_scd_results_by_table`: Shows only the most recent run for each table.
- `qa_results.v_scd_validation_report`: Human-readable detailed report of test failures.

## Maintenance

### Clearing History
You can use the **"Clear History"** button in the UI or run:
```sql
TRUNCATE TABLE `your-project-id.qa_results.scd_test_history`;
```
