"""Pydantic models for API requests."""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any


class GenerateTestsRequest(BaseModel):
    """Request model for test generation."""
    project_id: str = Field(..., description="Google Cloud project ID")
    execution_id: Optional[str] = Field(None, description="Workflow or execution ID")
    comparison_mode: str = Field(..., description="Mode: 'schema', 'gcs', 'gcs-config', 'scd', or 'scd-config'")
    
    # Schema mode fields
    datasets: Optional[List[str]] = Field(None, description="List of BigQuery datasets")
    erd_description: Optional[str] = Field(None, description="ER diagram description")
    
    # GCS single file mode fields
    gcs_bucket: Optional[str] = Field(None, description="GCS bucket name")
    gcs_file_path: Optional[str] = Field(None, description="GCS file path (supports wildcards)")
    file_format: Optional[str] = Field("csv", description="File format: csv, json, parquet, avro")
    target_dataset: Optional[str] = Field(None, description="Target BigQuery dataset")
    target_table: Optional[str] = Field(None, description="Target BigQuery table")
    
    # GCS config table mode and SCD config mode fields
    config_dataset: Optional[str] = Field(None, description="Config table dataset")
    config_table: Optional[str] = Field(None, description="Config table name")
    config_filters: Optional[Dict[str, Any]] = Field(None, description="Key-value pairs to filter config table records")
    
    # SCD mode fields
    scd_type: Optional[str] = Field("scd2", description="SCD type: scd1, scd2")
    primary_keys: Optional[List[str]] = Field(None, description="List of primary key columns")
    surrogate_key: Optional[str] = Field(None, description="Surrogate key column")
    begin_date_column: Optional[str] = Field(None, description="SCD2 begin date column")
    end_date_column: Optional[str] = Field(None, description="SCD2 end date column")
    active_flag_column: Optional[str] = Field(None, description="SCD2 active flag column")
    custom_tests: Optional[List[Dict[str, Any]]] = Field(None, description="List of custom tests")
    
    # Common optional fields
    enabled_test_ids: Optional[List[str]] = Field(None, description="List of test IDs to enable")


class TestResult(BaseModel):
    """Model for a single test result."""
    test_id: Optional[str] = None
    test_name: str
    category: Optional[str] = None
    description: str
    status: str  # PASS, FAIL, ERROR
    severity: str  # HIGH, MEDIUM, LOW
    sql_query: str
    rows_affected: int = 0
    error_message: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    sample_data: Optional[List[Dict[str, Any]]] = None # For SCD samples


class MappingInfo(BaseModel):
    """Information about a data mapping."""
    source: str
    target: str
    file_row_count: int
    table_row_count: int


class AISuggestion(BaseModel):
    """AI-generated test suggestion."""
    test_name: str
    test_category: str
    severity: str
    sql_query: str
    reasoning: str


class MappingResult(BaseModel):
    """Results for a single mapping."""
    mapping_id: str
    mapping_info: Optional[MappingInfo] = None
    predefined_results: List[TestResult]
    ai_suggestions: List[AISuggestion] = []
    error: Optional[str] = None


class CustomTestRequest(BaseModel):
    """Request model for saving a custom test."""
    project_id: str
    dataset_id: str = "config"
    test_name: str
    test_category: str
    severity: str
    sql_query: str
    description: str
    target_dataset: Optional[str] = None
    target_table: Optional[str] = None


class ProjectSettings(BaseModel):
    """Model for project-wide settings."""
    project_id: str
    alert_emails: List[str] = []
    teams_webhook_url: Optional[str] = None
    alert_on_failure: bool = True


class SaveHistoryRequest(BaseModel):
    """Request model for saving test history."""
    project_id: str
    comparison_mode: str
    test_results: List[Dict[str, Any]]
    target_dataset: Optional[str] = None
    target_table: Optional[str] = None
    mapping_id: Optional[str] = None
    executed_by: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class AddSCDConfigRequest(BaseModel):
    """Request model for adding/updating SCD configuration."""
    project_id: str
    config_dataset: str = "config"
    config_table: str = "scd_validation_config"
    target_dataset: str
    target_table: str
    scd_type: str = "scd2"
    primary_keys: List[str]
    surrogate_key: Optional[str] = None
    begin_date_column: Optional[str] = None
    end_date_column: Optional[str] = None
    active_flag_column: Optional[str] = None
    description: Optional[str] = None
    custom_tests: Optional[List[Dict[str, Any]]] = None
