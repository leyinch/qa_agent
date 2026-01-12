# Config Tables Setup Guide

## Overview

This guide explains how to set up the configuration tables for the Data QA Agent's hybrid testing system.

## Quick Start

1. **Run the setup script** in BigQuery:
   ```bash
   # Open BigQuery Console
   # Copy and paste the contents of config_tables_setup.sql
   # Execute the script
   ```

2. **Verify tables were created**:
   ```sql
   SELECT table_name 
   FROM `leyin-sandpit.transform_config.INFORMATION_SCHEMA.TABLES`;
   ```


## Config Tables

### 1. `data_load_config` - Main Configuration Table

Stores GCS-to-BigQuery mappings and test configurations.

**Key columns:**
- `mapping_id`: Unique identifier for each data load
- `source_bucket`, `source_file_path`: GCS source location
- `target_dataset`, `target_table`: BigQuery destination
- `enabled_test_ids`: Which predefined tests to run
- `auto_suggest`: Enable/disable AI test suggestions

**Example:**
```sql
SELECT * FROM `leyin-sandpit.transform_config.data_load_config` 
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

**Workflow:**
1. AI suggests tests after analyzing data
2. User reviews in UI
3. Approved tests can be added to predefined suite

### 4. `test_execution_history` - Audit Trail

Tracks all test executions for monitoring and debugging.

## Adding a New Mapping

```sql
INSERT INTO `leyin-sandpit.transform_config.data_load_config`
(mapping_id, mapping_name, source_bucket, source_file_path, source_file_format,
 target_dataset, target_table, primary_key_columns, required_columns,
 enabled_test_ids, is_active)
VALUES
('my_data_load', 'My Data Load', 'my-bucket', 'raw/data.csv', 'csv',
 'analytics', 'my_table', ['id'], ['id', 'name'],
 ['row_count_match', 'no_nulls_required', 'no_duplicates_pk'], true);
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

## Using in the App

1. **Select "GCS File Comparison" mode**
2. **Choose "Config Table" option**
3. **Enter**: `config` (dataset) and `data_load_config` (table)
4. **Click "Run Tests"**

The app will:
- Read all active mappings from the config table
- Run predefined tests on each mapping
- Generate AI suggestions for additional tests
- Display results grouped by mapping

## Maintenance

### View Active Mappings
```sql
SELECT mapping_id, mapping_name, target_table, is_active
FROM `leyin-sandpit.transform_config.data_load_config`
WHERE is_active = true;
```

### Disable a Mapping
```sql
UPDATE `leyin-sandpit.transform_config.data_load_config`
SET is_active = false, updated_at = CURRENT_TIMESTAMP()
WHERE mapping_id = 'my_data_load';
```

### View Test History
```sql
SELECT 
  mapping_id,
  test_name,
  status,
  rows_affected,
  executed_at
FROM `leyin-sandpit.transform_config.test_execution_history`
WHERE mapping_id = 'my_data_load'
ORDER BY executed_at DESC
LIMIT 100;
```

## Best Practices

1. **Start Simple**: Begin with global tests only
2. **Add Gradually**: Add specific tests as you understand your data
3. **Use Descriptive IDs**: Make mapping_id clear and meaningful
4. **Enable Auto-Suggest**: Let AI help discover edge cases
5. **Review History**: Monitor test results over time
6. **Keep Active**: Set `is_active = false` instead of deleting

## Troubleshooting

**No tables found:**
- Check dataset name is `config`
- Verify you have permissions
- Run the setup script again

**Tests not running:**
- Check `is_active = true`
- Verify `enabled_test_ids` is not empty
- Check table names are correct

**AI suggestions not appearing:**
- Ensure `auto_suggest = true`
- Check you have Vertex AI API enabled
- Verify project permissions
