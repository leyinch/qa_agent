"""Models package initialization."""
from .requests import (
    GenerateTestsRequest,
    TestResult,
    MappingInfo,
    AISuggestion,
    MappingResult,
    CustomTestRequest,
    AddSCDConfigRequest,
    SaveHistoryRequest
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
    "TestSummary",
    "ConfigTableSummary",
    "GenerateTestsResponse",
    "ConfigTableResponse",
    "HealthResponse",
    "TableMetadataResponse"
]
