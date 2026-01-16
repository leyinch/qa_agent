"""Pydantic models for API responses."""
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from .requests import TestResult, MappingResult


class TestSummary(BaseModel):
    """Summary statistics for test results."""
    total_tests: int = 0
    passed: int = 0
    failed: int = 0
    errors: int = 0


class ConfigTableSummary(BaseModel):
    """Summary for config table mode results."""
    total_mappings: int = 0
    total_tests: int = 0
    passed: int = 0
    failed: int = 0
    errors: int = 0
    total_suggestions: int = 0


class GenerateTestsResponse(BaseModel):
    """Response model for test generation."""
    summary: TestSummary
    results: List[TestResult] = []


class ConfigTableResponse(BaseModel):
    """Response model for config table mode."""
    summary: ConfigTableSummary
    results_by_mapping: List[MappingResult]


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    version: str = "1.0.0"


class TableMetadataResponse(BaseModel):
    """Response model for table metadata."""
    full_table_name: str
    columns: List[str]
    schema_info: Dict[str, Any]

