"""Stored procedure analysis data models."""

from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class SPJobStatus(str, Enum):
    """Stored procedure analysis job status enumeration."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class SPLanguage(str, Enum):
    """Stored procedure language enumeration."""
    SQL = "SQL"
    PYTHON = "PYTHON"
    MIXED = "MIXED"


class TableColumnRelationship(BaseModel):
    """Specific table-column relationship in the stored procedure."""
    table_name: str = Field(description="The actual table name (resolved from variables if needed)")
    column_name: str = Field(description="The specific column name, or * for table-level operations")
    relationship_types: str = Field(description="Comma-separated list of relationship types")


class StoredProcedureAnalysis(BaseModel):
    """Analysis results for the stored procedure."""
    sp_name: str = Field(description="Name of the stored procedure")
    sp_schema: str = Field(description="Schema of the stored procedure")
    sp_language: str = Field(description="SQL, PYTHON, or MIXED")
    relationships: List[TableColumnRelationship] = Field(description="List of ALL table-column relationships found in the procedure")
    variables_detected: Dict[str, str] = Field(description="Variable to table name mappings detected")
    temp_tables_created: List[str] = Field(description="List of temporary tables created")
    cursors_detected: List[str] = Field(description="List of cursor names detected")


class SPAnalysisRequest(BaseModel):
    """Request model for stored procedure analysis."""
    sf_environment: str = Field(default="prod", description="Snowflake environment (dev, stage, prod)")
    max_workers: int = Field(default=4, ge=1, le=10, description="Number of parallel workers")
    resume_from_partial: bool = Field(default=True, description="Resume from partial results if available")
    procedure_names: Optional[List[str]] = Field(default=None, description="Specific procedures to analyze (if None, analyze all)")


class SingleProcedureRequest(BaseModel):
    """Request model for single procedure analysis."""
    procedure_name: str = Field(description="Name of the stored procedure")
    procedure_definition: str = Field(description="SQL definition of the stored procedure")
    procedure_schema: str = Field(description="Schema of the stored procedure")


class SPAnalysisResponse(BaseModel):
    """Response model for starting stored procedure analysis."""
    job_id: UUID = Field(description="Unique job identifier")
    status: SPJobStatus = Field(description="Current job status")
    message: str = Field(description="Status message")
    results_url: str = Field(description="URL to check results")
    started_at: datetime = Field(description="Job start timestamp")


class SPAnalysisJob(BaseModel):
    """Stored procedure analysis job model."""
    job_id: UUID = Field(default_factory=uuid4, description="Unique job identifier")
    status: SPJobStatus = Field(default=SPJobStatus.PENDING, description="Current job status")
    sf_environment: str = Field(description="Snowflake environment")
    max_workers: int = Field(description="Number of parallel workers")
    total_procedures: int = Field(default=0, description="Total number of procedures to analyze")
    completed_procedures: int = Field(default=0, description="Number of completed procedures")
    failed_procedures: int = Field(default=0, description="Number of failed procedures")
    result_file: Optional[str] = Field(default=None, description="Path to result CSV file")
    error_message: Optional[str] = Field(default=None, description="Error message if failed")
    started_at: datetime = Field(default_factory=datetime.now, description="Job start timestamp")
    completed_at: Optional[datetime] = Field(default=None, description="Job completion timestamp")
    request_params: Dict = Field(default_factory=dict, description="Original request parameters")


class SPResultsResponse(BaseModel):
    """Response model for stored procedure analysis results."""
    job_id: UUID = Field(description="Job identifier")
    status: SPJobStatus = Field(description="Job status")
    total_procedures: int = Field(description="Total procedures analyzed")
    total_relationships: int = Field(description="Total relationships found")
    unique_tables: int = Field(description="Number of unique tables referenced")
    result_file: Optional[str] = Field(description="Path to result file")
    download_url: Optional[str] = Field(description="URL to download results")
    summary: Dict = Field(description="Analysis summary statistics")


class ProcedureInfo(BaseModel):
    """Basic stored procedure information."""
    name: str = Field(description="Procedure name")
    procedure_schema: str = Field(description="Procedure schema")
    definition_length: int = Field(description="Length of procedure definition")


class ProcedureListResponse(BaseModel):
    """Response model for listing stored procedures."""
    count: int = Field(description="Total number of procedures")
    procedures: List[ProcedureInfo] = Field(description="List of procedures")