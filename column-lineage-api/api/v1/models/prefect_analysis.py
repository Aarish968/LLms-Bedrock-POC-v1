"""Prefect repository analysis data models."""

from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict, Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class PrefectAnalysisStatus(str, Enum):
    """Prefect analysis job status enumeration."""
    PENDING = "pending"
    CLONING = "cloning"
    ANALYZING = "analyzing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class PrefectAnalysisRequest(BaseModel):
    """Request model for Prefect repository analysis."""
    sf_environment: str = Field(
        default="prod",
        description="Snowflake environment (dev/stage/prod)"
    )
    max_workers: int = Field(
        default=4,
        ge=1,
        le=10,
        description="Number of parallel workers for analysis"
    )
    target_directory: str = Field(
        default="prefect_repos",
        description="Directory to clone Prefect repositories"
    )
    skip_naming_check: bool = Field(
        default=False,
        description="Skip naming convention filtering"
    )
    specific_repos: Optional[List[str]] = Field(
        default=None,
        description="Specific repository names to analyze (if None, discover all Prefect repos)"
    )
    async_processing: bool = Field(
        default=True,
        description="Whether to process the analysis asynchronously"
    )


class PrefectAnalysisResponse(BaseModel):
    """Response model for Prefect repository analysis."""
    job_id: UUID = Field(description="Unique identifier for the analysis job")
    status: PrefectAnalysisStatus = Field(description="Current status of the analysis job")
    message: str = Field(description="Human-readable status message")
    started_at: datetime = Field(description="Timestamp when the job was started")
    results_url: Optional[str] = Field(
        default=None,
        description="URL to fetch results when job is completed"
    )


class PrefectAnalysisJob(BaseModel):
    """Prefect analysis job model."""
    job_id: UUID = Field(default_factory=uuid4, description="Unique identifier for the analysis job")
    status: PrefectAnalysisStatus = Field(
        default=PrefectAnalysisStatus.PENDING,
        description="Current status of the analysis job"
    )
    message: str = Field(description="Human-readable status message")
    started_at: datetime = Field(description="Timestamp when the job was started")
    completed_at: Optional[datetime] = Field(
        default=None,
        description="Timestamp when the job was completed"
    )
    error_message: Optional[str] = Field(
        default=None,
        description="Error message if the job failed"
    )
    sf_environment: str = Field(description="Snowflake environment used")
    max_workers: int = Field(description="Number of parallel workers")
    target_directory: str = Field(description="Directory where repos were cloned")
    total_repos_found: int = Field(default=0, description="Total Prefect repositories found")
    repos_cloned: int = Field(default=0, description="Number of repositories successfully cloned")
    total_references: int = Field(default=0, description="Total table-column references found")
    unique_tables: int = Field(default=0, description="Number of unique tables referenced")
    unique_repos: int = Field(default=0, description="Number of unique repositories analyzed")
    output_file: Optional[str] = Field(
        default=None,
        description="Path to the output CSV file"
    )
    request_params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Original request parameters"
    )


class TableColumnReference(BaseModel):
    """Table-column reference model."""
    repo_name: str = Field(description="Repository name")
    function_name: str = Field(description="Function name where reference was found")
    table_name: str = Field(description="Table name")
    column_name: str = Field(description="Column name")
    file_name: str = Field(description="File name where reference was found")


class PrefectAnalysisResults(BaseModel):
    """Prefect analysis results model."""
    job_id: UUID = Field(description="Job identifier")
    status: PrefectAnalysisStatus = Field(description="Job status")
    total_references: int = Field(description="Total table-column references found")
    unique_tables: int = Field(description="Number of unique tables")
    unique_repos: int = Field(description="Number of unique repositories")
    unique_functions: int = Field(description="Number of unique functions")
    output_file: str = Field(description="Output CSV file path")
    file_size: int = Field(description="File size in bytes")
    created_at: str = Field(description="File creation timestamp")
    modified_at: str = Field(description="File modification timestamp")
    summary: Dict[str, Any] = Field(description="Analysis summary statistics")
    sample_references: List[TableColumnReference] = Field(
        description="Sample of table-column references found"
    )


class PrefectRepositoryInfo(BaseModel):
    """Prefect repository information model."""
    repo_name: str = Field(description="Repository name")
    clone_status: str = Field(description="Clone status (success/failed)")
    prefect_files_found: List[str] = Field(description="Prefect configuration files found")
    python_files_count: int = Field(description="Number of Python files")
    has_flows: bool = Field(description="Whether repository contains Prefect flows")
    has_tasks: bool = Field(description="Whether repository contains Prefect tasks")


class PrefectDiscoveryResults(BaseModel):
    """Prefect repository discovery results."""
    total_repos_checked: int = Field(description="Total repositories checked")
    prefect_repos_found: int = Field(description="Number of Prefect repositories found")
    repositories: List[PrefectRepositoryInfo] = Field(description="List of discovered repositories")
    discovery_time_seconds: float = Field(description="Time taken for discovery")


class PrefectAnalysisSummary(BaseModel):
    """Summary statistics for Prefect analysis."""
    total_repositories: int = Field(description="Total repositories analyzed")
    total_files_analyzed: int = Field(description="Total files analyzed")
    total_references_found: int = Field(description="Total table-column references found")
    unique_tables_referenced: int = Field(description="Unique tables referenced")
    unique_columns_referenced: int = Field(description="Unique columns referenced")
    top_repositories_by_references: List[Dict[str, Any]] = Field(
        description="Top repositories by reference count"
    )
    top_tables_by_references: List[Dict[str, Any]] = Field(
        description="Top tables by reference count"
    )
    analysis_duration_seconds: float = Field(description="Total analysis duration")