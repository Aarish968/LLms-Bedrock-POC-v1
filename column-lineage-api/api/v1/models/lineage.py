"""Column lineage data models."""

import os
from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict, Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

# Load environment defaults
DEFAULT_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "CPS_DB")
DEFAULT_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "CPS_DSCI_BR")


class ColumnType(str, Enum):
    """Column type enumeration."""
    DIRECT = "DIRECT"
    DERIVED = "DERIVED"
    WINDOW = "WINDOW"
    ERROR = "ERROR"
    UNKNOWN = "UNKNOWN"


class ExpressionType(str, Enum):
    """Expression type enumeration."""
    SUM = "SUM"
    CASE = "CASE"
    COALESCE = "COALESCE"
    CONCAT = "CONCAT"
    WINDOW = "WINDOW"
    LITERAL = "LITERAL"
    CURRENTTIMESTAMP = "CURRENTTIMESTAMP"
    CAST = "CAST"
    MUL = "MUL"
    DIV = "DIV"
    ADD = "ADD"
    GROUPCONCAT = "GROUPCONCAT"
    COUNT = "COUNT"
    MAX = "MAX"
    MIN = "MIN"
    AVG = "AVG"
    DATEDIFF = "DATEDIFF"
    PAREN = "PAREN"


class JobStatus(str, Enum):
    """Job status enumeration."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class ColumnLineageResult(BaseModel):
    """Column lineage result model."""
    view_name: str = Field(description="Name of the view")
    view_column: str = Field(description="Column name in the view")
    column_type: ColumnType = Field(description="Type of column mapping")
    source_table: str = Field(description="Source table name")
    source_column: str = Field(description="Source column name")
    expression_type: Optional[ExpressionType] = Field(
        default=None, 
        description="Expression type for derived columns"
    )
    confidence_score: float = Field(
        default=1.0, 
        ge=0.0, 
        le=1.0, 
        description="Confidence score of the mapping"
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional metadata"
    )


class ViewInfo(BaseModel):
    """View information model."""
    view_name: str = Field(description="Name of the view")
    schema_name: str = Field(description="Schema name")
    database_name: str = Field(description="Database name")
    column_count: int = Field(description="Number of columns in the view")
    created_date: Optional[datetime] = Field(
        default=None, 
        description="View creation date"
    )
    last_modified: Optional[datetime] = Field(
        default=None, 
        description="Last modification date"
    )


class LineageAnalysisRequest(BaseModel):
    """Lineage analysis request model."""
    view_names: Optional[List[str]] = Field(
        default=None, 
        description="Specific view names to analyze. If None, analyze all views"
    )
    # Made optional with environment defaults - no longer required in API requests
    database_filter: str = Field(
        default=DEFAULT_DATABASE,
        description=f"Database name to filter views (default: {DEFAULT_DATABASE})"
    )
    # Made optional with environment defaults - no longer required in API requests  
    schema_filter: str = Field(
        default=DEFAULT_SCHEMA,
        description=f"Schema name to filter views (default: {DEFAULT_SCHEMA})"
    )
    include_system_views: bool = Field(
        default=False, 
        description="Include system views in analysis"
    )
    max_views: Optional[int] = Field(
        default=None, 
        ge=1, 
        le=1000, 
        description="Maximum number of views to analyze"
    )
    async_processing: bool = Field(
        default=True, 
        description="Process asynchronously"
    )
    include_metadata: bool = Field(
        default=True, 
        description="Include additional metadata in results"
    )


class LineageAnalysisJob(BaseModel):
    """Lineage analysis job model."""
    job_id: UUID = Field(default_factory=uuid4, description="Unique job identifier")
    status: JobStatus = Field(default=JobStatus.PENDING, description="Job status")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Job creation time")
    started_at: Optional[datetime] = Field(default=None, description="Job start time")
    completed_at: Optional[datetime] = Field(default=None, description="Job completion time")
    total_views: int = Field(default=0, description="Total number of views to process")
    processed_views: int = Field(default=0, description="Number of views processed")
    successful_views: int = Field(default=0, description="Number of successfully processed views")
    failed_views: int = Field(default=0, description="Number of failed views")
    error_message: Optional[str] = Field(default=None, description="Error message if job failed")
    results_count: int = Field(default=0, description="Number of lineage results")
    request_params: Dict[str, Any] = Field(
        default_factory=dict, 
        description="Original request parameters"
    )


class LineageAnalysisResponse(BaseModel):
    """Lineage analysis response model."""
    job_id: UUID = Field(description="Job identifier")
    status: JobStatus = Field(description="Job status")
    message: str = Field(description="Response message")
    results_url: Optional[str] = Field(
        default=None, 
        description="URL to fetch results when job is completed"
    )


class LineageResultsResponse(BaseModel):
    """Lineage results response model."""
    job_id: UUID = Field(description="Job identifier")
    status: JobStatus = Field(description="Job status")
    total_results: int = Field(description="Total number of results")
    results: List[ColumnLineageResult] = Field(description="Lineage results")
    summary: Dict[str, Any] = Field(description="Analysis summary")


class LineageExportRequest(BaseModel):
    """Lineage export request model."""
    format: str = Field(default="csv", description="Export format (csv, json, excel)")
    include_metadata: bool = Field(default=True, description="Include metadata in export")
    filter_by_confidence: Optional[float] = Field(
        default=None, 
        ge=0.0, 
        le=1.0, 
        description="Filter results by minimum confidence score"
    )


class BaseViewRecord(BaseModel):
    """Base view record model for configurable BASE_VIEW table."""
    base_primary_id: int = Field(description="Base primary ID")
    table_name: str = Field(description="Table name")


class BaseViewCreateRequest(BaseModel):
    """Request model for creating a new base view record."""
    base_primary_id: int = Field(description="Base primary ID", gt=0)
    table_name: str = Field(description="Table name", min_length=1, max_length=255)


class BaseViewUpdateRequest(BaseModel):
    """Request model for updating an existing base view record."""
    table_name: str = Field(description="Table name", min_length=1, max_length=255)


class BaseViewResponse(BaseModel):
    """Response model for BASE_VIEW table data."""
    total_records: int = Field(description="Total number of records")
    records: List[BaseViewRecord] = Field(description="List of base view records")


class ErrorDetail(BaseModel):
    """Error detail model."""
    view_name: str = Field(description="View name that caused the error")
    error_type: str = Field(description="Type of error")
    error_message: str = Field(description="Error message")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Error timestamp")