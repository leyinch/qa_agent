"""Pydantic models for API requests."""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any


class GenerateTestsRequest(BaseModel):
    """Request model for test generation."""
    project_id: str = Field(..., description="Google Cloud project ID")
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
    
    # GCS config table mode fields
    config_dataset: Optional[str] = Field(None, description="Config table dataset")
    config_table: Optional[str] = Field(None, description="Config table name")
    
    # SCD validation fields
    scd_type: Optional[str] = Field(None, description="SCD type: 'scd1' or 'scd2'")
    primary_keys: Optional[List[str]] = Field(None, description="Primary key columns")
    surrogate_key: Optional[str] = Field(None, description="Surrogate key column")
    begin_date_column: Optional[str] = Field(None, description="Effect beginning date column (SCD2)")
    end_date_column: Optional[str] = Field(None, description="Effect ending date column (SCD2)")
    active_flag_column: Optional[str] = Field(None, description="Active row flag column (SCD2)")
    custom_tests: Optional[List[Dict[str, str]]] = Field(None, description="List of custom business rules (name/sql)")
    
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
    sample_data: Optional[List[Dict[str, Any]]] = None
    error_message: Optional[str] = None


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
    dataset_id: str = "config"  # Default config dataset
    test_name: str
    test_category: str
    severity: str
    sql_query: str
    description: str
    target_dataset: Optional[str] = None
    target_table: Optional[str] = None


class AddSCDConfigRequest(BaseModel):
    """Request model for adding a new SCD configuration."""
    project_id: str = Field(..., description="Google Cloud project ID")
    config_dataset: str = Field(..., description="Config table dataset")
    config_table: str = Field(..., description="Config table name")
    config_id: str = Field(..., description="Unique configuration ID")
    target_dataset: str = Field(..., description="Target dataset containing the SCD table")
    target_table: str = Field(..., description="Target table name")
    scd_type: str = Field(..., description="SCD type: 'scd1' or 'scd2'")
    primary_keys: List[str] = Field(..., description="Primary key columns")
    surrogate_key: Optional[str] = Field(None, description="Surrogate key column")
    begin_date_column: Optional[str] = Field(None, description="Begin date column (SCD2)")
    end_date_column: Optional[str] = Field(None, description="End date column (SCD2)")
    active_flag_column: Optional[str] = Field(None, description="Active flag column (SCD2)")
    description: Optional[str] = Field("", description="Configuration description")
    custom_tests: Optional[List[Dict[str, str]]] = Field(None, description="List of custom business rules (name/sql)")

