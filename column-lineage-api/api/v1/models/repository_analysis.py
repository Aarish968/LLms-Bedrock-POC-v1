"""Repository analysis data models."""

from datetime import datetime
from enum import Enum
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field


class AnalysisStatus(str, Enum):
    """Analysis job status enumeration."""
    PENDING = "pending"
    CLONING = "cloning"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class RepositoryAnalysisRequest(BaseModel):
    """Request model for repository analysis."""
    async_processing: bool = Field(
        default=True,
        description="Whether to process the analysis asynchronously"
    )


class RepositoryAnalysisResponse(BaseModel):
    """Response model for repository analysis."""
    job_id: UUID = Field(description="Unique identifier for the analysis job")
    status: AnalysisStatus = Field(description="Current status of the analysis job")
    message: str = Field(description="Human-readable status message")
    output_file: Optional[str] = Field(
        default=None,
        description="Name of the output file (available when completed)"
    )
    started_at: datetime = Field(description="Timestamp when the job was started")


class RepositoryAnalysisJob(BaseModel):
    """Repository analysis job model."""
    job_id: UUID = Field(description="Unique identifier for the analysis job")
    status: AnalysisStatus = Field(description="Current status of the analysis job")
    message: str = Field(description="Human-readable status message")
    output_file: Optional[str] = Field(
        default=None,
        description="Name of the output file (available when completed)"
    )
    started_at: datetime = Field(description="Timestamp when the job was started")
    completed_at: Optional[datetime] = Field(
        default=None,
        description="Timestamp when the job was completed"
    )
    error_message: Optional[str] = Field(
        default=None,
        description="Error message if the job failed"
    )
    frontend_repo_name: Optional[str] = Field(
        default=None,
        description="Frontend repository name being analyzed"
    )
    backend_repo_name: Optional[str] = Field(
        default=None,
        description="Backend repository name being analyzed"
    )


class RepositoryAnalysisResults(BaseModel):
    """Repository analysis results model."""
    job_id: UUID = Field(description="Job identifier")
    status: AnalysisStatus = Field(description="Job status")
    output_file: str = Field(description="Output file name")
    file_size: int = Field(description="File size in bytes")
    created_at: str = Field(description="File creation timestamp")
    modified_at: str = Field(description="File modification timestamp")
    message: str = Field(description="Status message")