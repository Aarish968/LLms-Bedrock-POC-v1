"""ThoughtSpot liveboard analysis data models."""

from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class TSJobStatus(str, Enum):
    """ThoughtSpot analysis job status enumeration."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class LiveboardInfo(BaseModel):
    """ThoughtSpot liveboard information."""
    guid: str = Field(description="Liveboard GUID")
    name: str = Field(description="Liveboard name")
    type: str = Field(description="Object type (LIVEBOARD or PINBOARD)")
    description: Optional[str] = Field(default="", description="Liveboard description")
    url: Optional[str] = Field(default="", description="Direct URL to liveboard")
    created_by: Optional[str] = Field(default="", description="Creator username")
    created_date: Optional[str] = Field(default="", description="Creation date")
    modified_date: Optional[str] = Field(default="", description="Last modified date")
    tags: List[str] = Field(default_factory=list, description="Associated tags")


class TableLiveboardRelationship(BaseModel):
    """Relationship between a table and its liveboards."""
    table_name: str = Field(description="Table name")
    schema: str = Field(description="Table schema")
    table_type: str = Field(description="BASE TABLE or VIEW")
    liveboard_name: str = Field(description="Liveboard name")
    liveboard_guid: str = Field(description="Liveboard GUID")


class TSAnalysisRequest(BaseModel):
    """Request model for ThoughtSpot analysis."""
    sf_environment: str = Field(default="prod", description="Snowflake environment (dev, stage, prod)")
    table_pattern: Optional[str] = Field(default=None, description="Optional pattern to filter table names")
    max_workers: int = Field(default=5, ge=1, le=10, description="Number of parallel workers")
    include_views: bool = Field(default=True, description="Include views in addition to base tables")
    force_prod_urls: bool = Field(default=True, description="Force production ThoughtSpot URLs")


class TSAnalysisResponse(BaseModel):
    """Response model for starting ThoughtSpot analysis."""
    job_id: UUID = Field(description="Unique job identifier")
    status: TSJobStatus = Field(description="Current job status")
    message: str = Field(description="Status message")
    results_url: str = Field(description="URL to check results")
    started_at: datetime = Field(description="Job start timestamp")


class TSAnalysisJob(BaseModel):
    """ThoughtSpot analysis job model."""
    job_id: UUID = Field(default_factory=uuid4, description="Unique job identifier")
    status: TSJobStatus = Field(default=TSJobStatus.PENDING, description="Current job status")
    sf_environment: str = Field(description="Snowflake environment")
    table_pattern: Optional[str] = Field(default=None, description="Table name filter pattern")
    max_workers: int = Field(description="Number of parallel workers")
    include_views: bool = Field(default=True, description="Include views")
    force_prod_urls: bool = Field(default=True, description="Force production URLs")
    total_tables: int = Field(default=0, description="Total number of tables to analyze")
    processed_tables: int = Field(default=0, description="Number of processed tables")
    total_relationships: int = Field(default=0, description="Total relationships found")
    result_file: Optional[str] = Field(default=None, description="Path to result CSV file")
    error_message: Optional[str] = Field(default=None, description="Error message if failed")
    started_at: datetime = Field(default_factory=datetime.now, description="Job start timestamp")
    completed_at: Optional[datetime] = Field(default=None, description="Job completion timestamp")
    request_params: Dict = Field(default_factory=dict, description="Original request parameters")


class TSResultsResponse(BaseModel):
    """Response model for ThoughtSpot analysis results."""
    job_id: UUID = Field(description="Job identifier")
    status: TSJobStatus = Field(description="Job status")
    total_tables: int = Field(description="Total tables analyzed")
    total_relationships: int = Field(description="Total table-liveboard relationships found")
    unique_liveboards: int = Field(description="Number of unique liveboards")
    result_file: Optional[str] = Field(description="Path to result file")
    download_url: Optional[str] = Field(description="URL to download results")
    summary: Dict = Field(description="Analysis summary statistics")


class TableInfo(BaseModel):
    """Basic table information."""
    table_name: str = Field(description="Table name")
    schema: str = Field(description="Table schema")
    table_type: str = Field(description="BASE TABLE or VIEW")
    full_name: str = Field(description="Full qualified name (schema.table)")


class TableListResponse(BaseModel):
    """Response model for listing tables."""
    count: int = Field(description="Total number of tables")
    tables: List[TableInfo] = Field(description="List of tables")
