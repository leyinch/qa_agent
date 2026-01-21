"""Models package initialization."""
from .requests import (
    GenerateTestsRequest,
    TestResult,
    MappingInfo,
    AISuggestion,
    MappingResult,
    CustomTestRequest,
    AddSCDConfigRequest,
    SaveHistoryRequest,
    ProjectSettings
)
from .responses import (
    TestSummary,
    ConfigTableSummary,
    GenerateTestsResponse,
    ConfigTableResponse,
    HealthResponse,
    TableMetadataResponse
)

__all__ = [
    "GenerateTestsRequest",
    "TestResult",
    "MappingInfo",
    "AISuggestion",
    "MappingResult",
    "CustomTestRequest",
    "AddSCDConfigRequest",
    "SaveHistoryRequest",
    "ProjectSettings",
    "TestSummary",
    "ConfigTableSummary",
    "GenerateTestsResponse",
    "ConfigTableResponse",
    "HealthResponse",
    "TableMetadataResponse"
]
