-- Create test results history table for QA Agent
-- This table stores all test execution results for historical tracking and future alerting

CREATE TABLE IF NOT EXISTS `leyin-sandpit.qa_agent_metadata.test_results_history` (
  execution_id STRING NOT NULL,
  execution_timestamp TIMESTAMP NOT NULL,
  project_id STRING NOT NULL,
  comparison_mode STRING NOT NULL,  -- schema, gcs, scd, history
  target_dataset STRING,
  target_table STRING,
  mapping_id STRING,  -- For config mode
  status STRING NOT NULL,  -- PASS, FAIL, ERROR (Overall table status)
  total_tests INT64,
  passed_tests INT64,
  failed_tests INT64,
  error_message STRING,
  test_results JSON,  -- Array of detailed test results
  executed_by STRING,  -- For future auth integration
  metadata JSON  -- Additional metadata (scd_type, source/target info, etc.)
)
PARTITION BY DATE(execution_timestamp)
CLUSTER BY project_id, target_table, status;

-- Create a view for easy querying of latest test results per table
CREATE OR REPLACE VIEW `leyin-sandpit.qa_agent_metadata.latest_test_results_by_table` AS
SELECT 
  t.*
FROM `leyin-sandpit.qa_agent_metadata.test_results_history` t
INNER JOIN (
  SELECT 
    project_id,
    target_dataset,
    target_table,
    test_name,
    MAX(execution_timestamp) as latest_execution
  FROM `leyin-sandpit.qa_agent_metadata.test_results_history`
  WHERE target_table IS NOT NULL
  GROUP BY project_id, target_dataset, target_table, test_name
) latest
ON t.project_id = latest.project_id
  AND t.target_dataset = latest.target_dataset
  AND t.target_table = latest.target_table
  AND t.test_name = latest.test_name
  AND t.execution_timestamp = latest.latest_execution;
