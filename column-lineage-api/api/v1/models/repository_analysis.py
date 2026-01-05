"""Repository analysis data models."""

from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict, Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class AnalysisType(str, Enum):
    """Analysis type enumeration."""
    FRONTEND_BACKEND = "FRONTEND_BACKEND"
    REPOSITORY_DISCOVERY = "REPOSITORY_DISCOVERY"
    API_MAPPING = "API_MAPPING"
    DATABASE_LINEAGE = "DATABASE_LINEAGE"


class RepositoryType(str, Enum):
    """Repository type enumeration."""
    FRONTEND = "FRONTEND"
    BACKEND = "BACKEND"
    FULLSTACK = "FULLSTACK"
    UNKNOWN = "UNKNOWN"


class RepositoryAnalysisStatus(str, Enum):
    """Repository analysis job status enumeration."""
    PENDING = "PENDING"
    DISCOVERING = "DISCOVERING"
    CLONING = "CLONING"
    ANALYZING = "ANALYZING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class RepositoryInfo(BaseModel):
    """Repository information model."""
    repository_name: str = Field(description="Repository name")
    repository_type: RepositoryType = Field(description="Type of repository")
    clone_url: Optional[str] = Field(default=None, description="Repository clone URL")
    local_path: Optional[str] = Field(default=None, description="Local path after cloning")
    branch: str = Field(default="main", description="Branch to analyze")
    last_commit: Optional[str] = Field(default=None, description="Last commit hash")
    size_mb: Optional[float] = Field(default=None, description="Repository size in MB")
    language: Optional[str] = Field(default=None, description="Primary programming language")
    framework: Optional[str] = Field(default=None, description="Framework detected")


class ApiEndpointMapping(BaseModel):
    """API endpoint mapping model."""
    frontend_call: str = Field(description="Frontend API call")
    backend_endpoint: str = Field(description="Backend endpoint")
    http_method: str = Field(description="HTTP method")
    database_tables: List[str] = Field(default_factory=list, description="Related database tables")
    database_columns: List[str] = Field(default_factory=list, description="Related database columns")
    confidence_score: float = Field(default=1.0, ge=0.0, le=1.0, description="Mapping confidence")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")


class RepositoryAnalysisRequest(BaseModel):
    """Simplified repository analysis request model."""
    frontend_path: str = Field(
        description="Frontend project path - will check locally first, then clone from CodeCommit if missing"
    )
    backend_path: str = Field(
        description="Backend project path - will check locally first, then clone from CodeCommit if missing"
    )
    include_database_analysis: bool = Field(
        default=True,
        description="Include database table/column analysis"
    )
    output_format: str = Field(
        default="csv",
        description="Output format (csv, json, excel)"
    )
    credentials_file: str = Field(
        default="credentials.txt",
        description="AWS credentials file path"
    )


class RepositoryAnalysisJob(BaseModel):
    """Repository analysis job model."""
    job_id: UUID = Field(default_factory=uuid4, description="Unique job identifier")
    status: RepositoryAnalysisStatus = Field(default=RepositoryAnalysisStatus.PENDING, description="Job status")
    analysis_type: AnalysisType = Field(description="Type of analysis")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Job creation time")
    started_at: Optional[datetime] = Field(default=None, description="Job start time")
    completed_at: Optional[datetime] = Field(default=None, description="Job completion time")
    
    # Progress tracking
    total_repositories: int = Field(default=0, description="Total repositories to process")
    discovered_repositories: int = Field(default=0, description="Number of repositories discovered")
    cloned_repositories: int = Field(default=0, description="Number of repositories cloned")
    analyzed_repositories: int = Field(default=0, description="Number of repositories analyzed")
    failed_repositories: int = Field(default=0, description="Number of failed repositories")
    
    # Results
    api_mappings_count: int = Field(default=0, description="Number of API mappings found")
    database_tables_count: int = Field(default=0, description="Number of database tables analyzed")
    database_columns_count: int = Field(default=0, description="Number of database columns analyzed")
    
    # Error handling
    error_message: Optional[str] = Field(default=None, description="Error message if job failed")
    warnings: List[str] = Field(default_factory=list, description="Warning messages")
    
    # Request parameters
    request_params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Original request parameters"
    )
    
    # Output files
    output_files: List[str] = Field(
        default_factory=list,
        description="Generated output file paths"
    )


class RepositoryAnalysisResponse(BaseModel):
    """Repository analysis response model."""
    job_id: UUID = Field(description="Job identifier")
    status: RepositoryAnalysisStatus = Field(description="Job status")
    message: str = Field(description="Response message")
    analysis_type: AnalysisType = Field(description="Type of analysis")
    estimated_duration_minutes: Optional[int] = Field(
        default=None,
        description="Estimated duration in minutes"
    )
    results_url: Optional[str] = Field(
        default=None,
        description="URL to fetch results when job is completed"
    )
    progress_url: Optional[str] = Field(
        default=None,
        description="URL to check job progress"
    )


class RepositoryAnalysisResults(BaseModel):
    """Repository analysis results model."""
    job_id: UUID = Field(description="Job identifier")
    status: RepositoryAnalysisStatus = Field(description="Job status")
    analysis_type: AnalysisType = Field(description="Type of analysis")
    
    # Repository information
    repositories: List[RepositoryInfo] = Field(
        default_factory=list,
        description="Analyzed repositories"
    )
    
    # API mappings
    api_mappings: List[ApiEndpointMapping] = Field(
        default_factory=list,
        description="Frontend-backend API mappings"
    )
    
    # Summary statistics
    summary: Dict[str, Any] = Field(
        default_factory=dict,
        description="Analysis summary"
    )
    
    # Output files
    output_files: List[str] = Field(
        default_factory=list,
        description="Generated output file paths"
    )
    
    # Execution metadata
    execution_time_seconds: Optional[float] = Field(
        default=None,
        description="Total execution time in seconds"
    )
    created_at: datetime = Field(description="Results creation time")


class RepositoryAnalysisProgress(BaseModel):
    """Repository analysis progress model."""
    job_id: UUID = Field(description="Job identifier")
    status: RepositoryAnalysisStatus = Field(description="Current job status")
    progress_percentage: float = Field(
        ge=0.0,
        le=100.0,
        description="Progress percentage"
    )
    current_step: str = Field(description="Current processing step")
    
    # Detailed progress
    total_repositories: int = Field(description="Total repositories to process")
    discovered_repositories: int = Field(description="Repositories discovered")
    cloned_repositories: int = Field(description="Repositories cloned")
    analyzed_repositories: int = Field(description="Repositories analyzed")
    failed_repositories: int = Field(description="Repositories failed")
    
    # Time estimates
    elapsed_time_seconds: float = Field(description="Elapsed time in seconds")
    estimated_remaining_seconds: Optional[float] = Field(
        default=None,
        description="Estimated remaining time in seconds"
    )
    
    # Current activity
    current_repository: Optional[str] = Field(
        default=None,
        description="Currently processing repository"
    )
    recent_logs: List[str] = Field(
        default_factory=list,
        description="Recent log messages"
    )


class RepositoryAnalysisExportRequest(BaseModel):
    """Repository analysis export request model."""
    format: str = Field(
        default="csv",
        description="Export format (csv, json, excel)"
    )
    include_metadata: bool = Field(
        default=True,
        description="Include metadata in export"
    )
    include_repository_info: bool = Field(
        default=True,
        description="Include repository information"
    )
    filter_by_confidence: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Filter results by minimum confidence score"
    )
    compress_output: bool = Field(
        default=False,
        description="Compress output file"
    )


class RepositoryDiscoveryResult(BaseModel):
    """Repository discovery result model."""
    total_repositories: int = Field(description="Total repositories found")
    frontend_repositories: List[str] = Field(
        default_factory=list,
        description="Frontend repository names"
    )
    backend_repositories: List[str] = Field(
        default_factory=list,
        description="Backend repository names"
    )
    fullstack_repositories: List[str] = Field(
        default_factory=list,
        description="Fullstack repository names"
    )
    unknown_repositories: List[str] = Field(
        default_factory=list,
        description="Unknown type repositories"
    )
    discovery_time_seconds: float = Field(description="Discovery time in seconds")


class ErrorDetail(BaseModel):
    """Error detail model for repository analysis."""
    repository_name: Optional[str] = Field(default=None, description="Repository that caused the error")
    step: str = Field(description="Processing step where error occurred")
    error_type: str = Field(description="Type of error")
    error_message: str = Field(description="Error message")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Error timestamp")
    stack_trace: Optional[str] = Field(default=None, description="Stack trace if available")