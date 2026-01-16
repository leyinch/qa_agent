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
    ScheduledTestRunRequest
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
    "ScheduledTestRunRequest",
    "TestSummary",
    "ConfigTableSummary",
    "GenerateTestsResponse",
    "ConfigTableResponse",
    "HealthResponse",
    "TableMetadataResponse"
]
