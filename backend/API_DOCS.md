# Data QA Agent API Documentation

## Overview
The Data QA Agent Backend provides a RESTful API for generating data quality tests, executing them against BigQuery and GCS, and managing test configurations.

## Base URL
`https://data-qa-agent-backend-750147355601.us-central1.run.app` (Production)
`http://localhost:8000` (Local)

## Endpoints

### 1. Health Check
Check the status of the API.

- **URL**: `/health`
- **Method**: `GET`
- **Response**: `200 OK`
  ```json
  {
    "status": "healthy",
    "version": "1.0.0"
  }
  ```

### 2. Generate Tests
Generate and execute data quality tests based on the specified mode.

- **URL**: `/api/generate-tests`
- **Method**: `POST`
- **Request Body**: `GenerateTestsRequest`

#### Supported Modes (`comparison_mode`)
1.  **`gcs`**: Single GCS file vs BigQuery table.
2.  **`gcs-config`**: Batch processing from a config table.
3.  **`schema`**: Validate Schema against ERD description.

#### Example Request (`gcs` mode)
```json
{
  "project_id": "my-project",
  "comparison_mode": "gcs",
  "gcs_bucket": "my-bucket",
  "gcs_file_path": "data/file.csv",
  "target_dataset": "my_dataset",
  "target_table": "my_table"
}
```

#### Example Response
Returns a JSON object containing a `summary`, `mapping_info`, `predefined_results`, and `ai_suggestions`.

### 3. Get Test History
Retrieve execution history from BigQuery.

- **URL**: `/api/history`
- **Method**: `GET`
- **Query Parameters**:
    - `project_id` (optional): Filter by project ID.
    - `limit` (optional, default 50): Max number of records.
- **Response**: List of execution records.

### 4. List Predefined Tests
Get a list of all available built-in test types.

- **URL**: `/api/predefined-tests`
- **Method**: `GET`
- **Response**:
  ```json
  {
    "tests": [
      {
        "id": "row_count_match",
        "name": "Row Count Match",
        "category": "completeness",
        "severity": "HIGH",
        ...
      }
    ]
  }
  ```

### 5. Save Custom Test
Persist a custom test case (e.g., from AI suggestions) to the backend.

- **URL**: `/api/custom-tests`
- **Method**: `POST`
- **Request Body**: `CustomTestRequest`

#### Example Request
```json
{
  "project_id": "my-project",
  "dataset_id": "config",
  "test_name": "Check for Negative Values",
  "test_category": "validity",
  "severity": "HIGH",
  "sql_query": "SELECT * FROM `p.d.t` WHERE val < 0",
  "description": "Values should be positive",
  "target_dataset": "target_ds",
  "target_table": "target_tab"
}
```

## Models

### GenerateTestsRequest
| Field | Type | Description |
|---|---|---|
| `project_id` | string | Google Cloud Project ID |
| `comparison_mode` | string | `gcs`, `gcs-config`, or `schema` |
| `gcs_bucket` | string? | Required for `gcs` mode |
| `gcs_file_path` | string? | Required for `gcs` mode |
| `config_dataset` | string? | Required for `gcs-config` mode |
| `config_table` | string? | Required for `gcs-config` mode |
| `erd_description` | string? | Required for `schema` mode |
| `datasets` | list[str]? | Required for `schema` mode |

### CustomTestRequest
| Field | Type | Description |
|---|---|---|
| `project_id` | string | Google Cloud Project ID |
| `dataset_id` | string | Config dataset ID (default: "config") |
| `test_name` | string | Name of the test |
| `sql_query` | string | BigQuery Standard SQL query |
| `severity` | string | HIGH, MEDIUM, or LOW |
