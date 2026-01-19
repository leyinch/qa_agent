-- ============================================
-- Data QA Agent - Config Tables Setup
-- ============================================

-- Create config dataset if it doesn't exist
CREATE SCHEMA IF NOT EXISTS `{{PROJECT_ID}}.config`
OPTIONS(
  description="Configuration tables for Data QA Agent"
);

-- ============================================
-- 1. Data Load Mappings Configuration
-- ============================================
CREATE TABLE IF NOT EXISTS `{{PROJECT_ID}}.config.data_load_config` (
  -- Mapping identification
  mapping_id STRING NOT NULL,
  mapping_name STRING,
  description STRING,
  
  -- Source (GCS)
  source_bucket STRING NOT NULL,
  source_file_path STRING NOT NULL,
  source_file_format STRING NOT NULL,  -- 'csv', 'json', 'parquet', 'avro'
  
  -- Target (BigQuery)
  target_dataset STRING NOT NULL,
  target_table STRING NOT NULL,
  
  -- Test configuration
  primary_key_columns ARRAY<STRING>,
  required_columns ARRAY<STRING>,
  date_columns ARRAY<STRING>,
  numeric_range_checks JSON,          -- {"column": {"min": 0, "max": 100}}
  date_range_checks JSON,             -- {"column": {"min_date": "2024-01-01", "max_date": "2024-12-31"}}
  foreign_key_checks JSON,            -- {"fk_column": {"table": "ref_table", "column": "ref_column"}}
  pattern_checks JSON,                -- {"email": "^[^@]+@[^@]+\\.[^@]+$"}
  outlier_columns ARRAY<STRING>,
  
  -- Enabled tests
  enabled_test_ids ARRAY<STRING>,     -- Which predefined tests to run
  auto_suggest BOOLEAN DEFAULT true,  -- Whether to get AI suggestions
  
  -- Status
  is_active BOOLEAN DEFAULT true,

  
  -- Metadata
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  created_by STRING,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_by STRING
);

-- ============================================
-- 2. Predefined Tests (System-wide)
-- ============================================
CREATE TABLE IF NOT EXISTS `{{PROJECT_ID}}.config.predefined_tests` (
  test_id STRING NOT NULL,
  test_name STRING NOT NULL,
  test_category STRING NOT NULL,      -- 'completeness', 'integrity', 'quality', 'statistical', 'business'
  severity STRING NOT NULL,            -- 'HIGH', 'MEDIUM', 'LOW'
  description STRING,
  sql_template STRING,                 -- SQL template with placeholders
  is_global BOOLEAN DEFAULT false,     -- Applies to all mappings
  is_system BOOLEAN DEFAULT true,      -- System-defined vs user-defined
  
  -- Metadata
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  created_by STRING DEFAULT 'system',
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================
-- 3. AI Suggested Tests (Pending Approval)
-- ============================================
CREATE TABLE IF NOT EXISTS `{{PROJECT_ID}}.config.suggested_tests` (
  suggestion_id STRING NOT NULL,
  mapping_id STRING NOT NULL,
  
  -- Test details
  test_name STRING NOT NULL,
  test_category STRING NOT NULL,
  severity STRING NOT NULL,
  sql_query STRING NOT NULL,
  reasoning STRING,                    -- Why AI suggested this
  estimated_failures INT64,
  
  -- Approval status
  status STRING DEFAULT 'pending',     -- 'pending', 'approved', 'rejected'
  reviewed_by STRING,
  reviewed_at TIMESTAMP,
  review_notes STRING,
  
  -- Metadata
  suggested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  suggested_by STRING DEFAULT 'ai'
);

-- ============================================
-- 4. Test Execution History
-- ============================================
CREATE TABLE IF NOT EXISTS `{{PROJECT_ID}}.config.test_execution_history` (
  execution_id STRING NOT NULL,
  mapping_id STRING NOT NULL,
  test_id STRING,
  test_name STRING NOT NULL,
  
  -- Results
  status STRING NOT NULL,              -- 'PASS', 'FAIL', 'ERROR'
  rows_affected INT64,
  error_message STRING,
  execution_time_ms INT64,
  
  -- Context
  source_row_count INT64,
  target_row_count INT64,
  
  -- Metadata
  executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  executed_by STRING
);

-- ============================================
-- Insert System Predefined Tests
-- ============================================
INSERT INTO `{{PROJECT_ID}}.config.predefined_tests` 
(test_id, test_name, test_category, severity, description, is_global, is_system)
VALUES
('row_count_match', 'Row Count Match', 'completeness', 'HIGH', 
 'Verify source and target row counts match', true, true),

('no_nulls_required', 'No NULLs in Required Fields', 'completeness', 'HIGH',
 'Check required columns have no NULL values', true, true),

('no_duplicates_pk', 'No Duplicate Primary Keys', 'integrity', 'HIGH',
 'Ensure primary key uniqueness', true, true),

('referential_integrity', 'Referential Integrity', 'integrity', 'HIGH',
 'Validate foreign key relationships', false, true),

('numeric_range', 'Numeric Range Validation', 'quality', 'MEDIUM',
 'Check numeric values are within expected ranges', false, true),

('date_range', 'Date Range Validation', 'quality', 'MEDIUM',
 'Validate dates are within expected range', false, true),

('pattern_validation', 'Pattern Validation', 'quality', 'MEDIUM',
 'Check string patterns (email, phone, etc.)', false, true),

('outlier_detection', 'Statistical Outlier Detection', 'statistical', 'LOW',
 'Detect statistical outliers using standard deviation', false, true);

-- ============================================
-- Sample Data Load Configuration
-- ============================================
INSERT INTO `{{PROJECT_ID}}.config.data_load_config`
(mapping_id, mapping_name, description, 
 source_bucket, source_file_path, source_file_format,
 target_dataset, target_table,
 primary_key_columns, required_columns, date_columns,
 numeric_range_checks, pattern_checks,
 enabled_test_ids, auto_suggest, is_active,
 created_by)
VALUES
(
  'customers_daily_load',
  'Daily Customer Data Load',
  'Load customer data from GCS to BigQuery analytics table',
  
  'my-data-bucket',
  'raw/customers_*.csv',
  'csv',
  
  'analytics',
  'customers',
  
  ['customer_id'],                     -- Primary key
  ['customer_id', 'email', 'name'],   -- Required fields
  ['created_at', 'updated_at'],       -- Date columns
  
  JSON '{"age": {"min": 18, "max": 120}, "credit_score": {"min": 300, "max": 850}}',
  JSON '{"email": "^[^@]+@[^@]+\\\\.[^@]+$", "phone": "^\\\\+?[0-9]{10,15}$"}',
  
  ['row_count_match', 'no_nulls_required', 'no_duplicates_pk', 'numeric_range', 'pattern_validation'],
  true,  -- Auto-suggest enabled
  true,  -- Active
  'admin@example.com'
);

-- ============================================
-- Useful Queries
-- ============================================

-- View all active mappings
-- SELECT * FROM `{{PROJECT_ID}}.config.data_load_config` WHERE is_active = true;

-- View all predefined tests
-- SELECT * FROM `{{PROJECT_ID}}.config.predefined_tests` ORDER BY test_category, severity;

-- View pending AI suggestions
-- SELECT * FROM `{{PROJECT_ID}}.config.suggested_tests` WHERE status = 'pending';

-- View test execution history for a mapping
-- SELECT * FROM `{{PROJECT_ID}}.config.test_execution_history` 
-- WHERE mapping_id = 'customers_daily_load' 
-- ORDER BY executed_at DESC LIMIT 100;
