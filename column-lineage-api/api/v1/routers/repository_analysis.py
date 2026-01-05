"""Repository analysis API routes."""

import os
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks, Query
from fastapi.responses import StreamingResponse

from api.core.logging import LoggerMixin
from api.dependencies.auth import get_current_user
from api.v1.models.repository_analysis import (
    RepositoryAnalysisRequest,
    RepositoryAnalysisResponse,
    RepositoryAnalysisJob,
    RepositoryAnalysisResults,
    RepositoryAnalysisProgress,
    RepositoryAnalysisExportRequest,
    RepositoryDiscoveryResult,
    AnalysisType,
    RepositoryAnalysisStatus,
)
from api.v1.services.repository_analysis_service import RepositoryAnalysisService

router = APIRouter()


class RepositoryAnalysisRouter(LoggerMixin):
    """Repository analysis router with logging."""
    
    def __init__(self):
        self.service = RepositoryAnalysisService()


# Create router instance
repo_router = RepositoryAnalysisRouter()


@router.post("/analyze", response_model=RepositoryAnalysisResponse)
async def start_repository_analysis(
    request: RepositoryAnalysisRequest,
    background_tasks: BackgroundTasks,
    current_user: str = Depends(get_current_user),
) -> RepositoryAnalysisResponse:
    """
    Start a simplified repository analysis job.
    
    This endpoint analyzes frontend and backend repositories from provided paths.
    If paths don't exist locally, it will attempt to clone them from AWS CodeCommit.
    """
    repo_router.logger.info(
        "Starting simplified repository analysis",
        user_id=current_user,
        frontend_path=request.frontend_path,
        backend_path=request.backend_path
    )
    
    try:
        # Start the analysis job
        job = await repo_router.service.start_repository_analysis(request, current_user)
        
        # Estimate duration (simplified analysis is faster)
        estimated_duration = 5  # 5 minutes for simplified analysis
        
        response = RepositoryAnalysisResponse(
            job_id=job.job_id,
            status=job.status,
            message=f"Repository analysis job started successfully. Analyzing: {request.frontend_path} and {request.backend_path}",
            analysis_type=AnalysisType.FRONTEND_BACKEND,
            estimated_duration_minutes=estimated_duration,
            results_url=f"/api/v1/repository-analysis/{job.job_id}/results",
            progress_url=f"/api/v1/repository-analysis/{job.job_id}/progress",
        )
        
        repo_router.logger.info(
            "Repository analysis job created",
            job_id=str(job.job_id),
            user_id=current_user,
            estimated_duration=estimated_duration
        )
        
        return response
        
    except Exception as e:
        repo_router.logger.error(
            "Failed to start repository analysis",
            user_id=current_user,
            error=str(e)
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to start repository analysis: {str(e)}"
        )


@router.get("/jobs/{job_id}", response_model=RepositoryAnalysisJob)
async def get_job_status(
    job_id: UUID,
    current_user: str = Depends(get_current_user),
) -> RepositoryAnalysisJob:
    """
    Get the status of a repository analysis job.
    """
    repo_router.logger.info("Getting job status", job_id=str(job_id), user_id=current_user)
    
    job = await repo_router.service.get_job_status(job_id)
    if not job:
        raise HTTPException(
            status_code=404,
            detail=f"Job {job_id} not found"
        )
    
    return job


@router.get("/jobs/{job_id}/progress", response_model=RepositoryAnalysisProgress)
async def get_job_progress(
    job_id: UUID,
    current_user: str = Depends(get_current_user),
) -> RepositoryAnalysisProgress:
    """
    Get detailed progress information for a repository analysis job.
    """
    repo_router.logger.info("Getting job progress", job_id=str(job_id), user_id=current_user)
    
    progress = await repo_router.service.get_job_progress(job_id)
    if not progress:
        raise HTTPException(
            status_code=404,
            detail=f"Job {job_id} not found"
        )
    
    return progress


@router.get("/jobs/{job_id}/results", response_model=RepositoryAnalysisResults)
async def get_job_results(
    job_id: UUID,
    current_user: str = Depends(get_current_user),
) -> RepositoryAnalysisResults:
    """
    Get the results of a completed repository analysis job.
    """
    repo_router.logger.info("Getting job results", job_id=str(job_id), user_id=current_user)
    
    # Check job status first
    job = await repo_router.service.get_job_status(job_id)
    if not job:
        raise HTTPException(
            status_code=404,
            detail=f"Job {job_id} not found"
        )
    
    if job.status != RepositoryAnalysisStatus.COMPLETED:
        raise HTTPException(
            status_code=400,
            detail=f"Job {job_id} is not completed yet. Current status: {job.status.value}"
        )
    
    results = await repo_router.service.get_job_results(job_id)
    if not results:
        raise HTTPException(
            status_code=404,
            detail=f"Results for job {job_id} not found"
        )
    
    return results


@router.post("/jobs/{job_id}/cancel")
async def cancel_job(
    job_id: UUID,
    current_user: str = Depends(get_current_user),
) -> dict:
    """
    Cancel a running repository analysis job.
    """
    repo_router.logger.info("Cancelling job", job_id=str(job_id), user_id=current_user)
    
    success = await repo_router.service.cancel_job(job_id)
    if not success:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot cancel job {job_id}. Job may not exist or already be completed."
        )
    
    return {"message": f"Job {job_id} cancelled successfully"}


@router.get("/jobs/{job_id}/export")
async def export_job_results(
    job_id: UUID,
    format: str = Query(default="csv", description="Export format (csv, json, excel)"),
    include_metadata: bool = Query(default=True, description="Include metadata in export"),
    include_repository_info: bool = Query(default=True, description="Include repository information"),
    filter_by_confidence: Optional[float] = Query(
        default=None,
        ge=0.0,
        le=1.0,
        description="Filter results by minimum confidence score"
    ),
    current_user: str = Depends(get_current_user),
):
    """
    Export repository analysis results in the specified format.
    """
    repo_router.logger.info(
        "Exporting job results",
        job_id=str(job_id),
        format=format,
        user_id=current_user
    )
    
    # Validate format
    if format.lower() not in ["csv", "json", "excel"]:
        raise HTTPException(
            status_code=400,
            detail="Invalid format. Supported formats: csv, json, excel"
        )
    
    # Check if job exists and is completed
    job = await repo_router.service.get_job_status(job_id)
    if not job:
        raise HTTPException(
            status_code=404,
            detail=f"Job {job_id} not found"
        )
    
    if job.status != RepositoryAnalysisStatus.COMPLETED:
        raise HTTPException(
            status_code=400,
            detail=f"Job {job_id} is not completed yet. Current status: {job.status.value}"
        )
    
    try:
        # Generate export
        export_generator = repo_router.service.export_results(
            job_id=job_id,
            format=format,
            include_metadata=include_metadata,
            include_repository_info=include_repository_info,
            filter_by_confidence=filter_by_confidence,
        )
        
        # Set appropriate content type and filename
        content_types = {
            "csv": "text/csv",
            "json": "application/json",
            "excel": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }
        
        extensions = {
            "csv": "csv",
            "json": "json",
            "excel": "xlsx",
        }
        
        filename = f"repository_analysis_{str(job_id)[:8]}.{extensions[format.lower()]}"
        
        return StreamingResponse(
            export_generator,
            media_type=content_types[format.lower()],
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except Exception as e:
        repo_router.logger.error(
            "Failed to export job results",
            job_id=str(job_id),
            format=format,
            error=str(e)
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to export results: {str(e)}"
        )


@router.get("/discover-repositories", response_model=RepositoryDiscoveryResult)
async def discover_repositories(
    credentials_file: str = Query(
        default="credentials.txt",
        description="AWS credentials file path"
    ),
    current_user: str = Depends(get_current_user),
) -> RepositoryDiscoveryResult:
    """
    Discover available repositories from AWS CodeCommit.
    
    This endpoint scans CodeCommit for available repositories and categorizes them
    by type (frontend, backend, fullstack, unknown).
    """
    repo_router.logger.info(
        "Starting repository discovery",
        user_id=current_user,
        credentials_file=credentials_file
    )
    
    try:
        result = await repo_router.service.discover_repositories(credentials_file)
        
        repo_router.logger.info(
            "Repository discovery completed",
            user_id=current_user,
            total_repositories=result.total_repositories,
            discovery_time=result.discovery_time_seconds
        )
        
        return result
        
    except Exception as e:
        repo_router.logger.error(
            "Repository discovery failed",
            user_id=current_user,
            error=str(e)
        )
        raise HTTPException(
            status_code=500,
            detail=f"Repository discovery failed: {str(e)}"
        )


@router.get("/jobs", response_model=List[RepositoryAnalysisJob])
async def list_jobs(
    status: Optional[RepositoryAnalysisStatus] = Query(
        default=None,
        description="Filter jobs by status"
    ),
    analysis_type: Optional[AnalysisType] = Query(
        default=None,
        description="Filter jobs by analysis type"
    ),
    limit: int = Query(
        default=50,
        ge=1,
        le=100,
        description="Maximum number of jobs to return"
    ),
    current_user: str = Depends(get_current_user),
) -> List[RepositoryAnalysisJob]:
    """
    List repository analysis jobs with optional filtering.
    """
    repo_router.logger.info(
        "Listing jobs",
        user_id=current_user,
        status=status.value if status else None,
        analysis_type=analysis_type.value if analysis_type else None,
        limit=limit
    )
    
    # For now, return all jobs from the service
    # In a production system, you'd want to filter by user and implement proper pagination
    all_jobs = list(repo_router.service._active_jobs.values())
    
    # Apply filters
    filtered_jobs = all_jobs
    if status:
        filtered_jobs = [job for job in filtered_jobs if job.status == status]
    if analysis_type:
        filtered_jobs = [job for job in filtered_jobs if job.analysis_type == analysis_type]
    
    # Apply limit
    filtered_jobs = filtered_jobs[:limit]
    
    # Sort by creation time (newest first)
    filtered_jobs.sort(key=lambda x: x.created_at, reverse=True)
    
    return filtered_jobs


@router.delete("/jobs/{job_id}")
async def delete_job(
    job_id: UUID,
    current_user: str = Depends(get_current_user),
) -> dict:
    """
    Delete a repository analysis job and its results.
    """
    repo_router.logger.info("Deleting job", job_id=str(job_id), user_id=current_user)
    
    # Check if job exists
    job = await repo_router.service.get_job_status(job_id)
    if not job:
        raise HTTPException(
            status_code=404,
            detail=f"Job {job_id} not found"
        )
    
    # Cancel job if it's still running
    if job.status not in [RepositoryAnalysisStatus.COMPLETED, RepositoryAnalysisStatus.FAILED, RepositoryAnalysisStatus.CANCELLED]:
        await repo_router.service.cancel_job(job_id)
    
    # Remove job and results from memory
    repo_router.service._active_jobs.pop(job_id, None)
    repo_router.service._job_results.pop(job_id, None)
    
    # Clean up output files
    try:
        for output_file in job.output_files:
            if os.path.exists(output_file):
                os.remove(output_file)
                repo_router.logger.info("Deleted output file", file=output_file)
    except Exception as e:
        repo_router.logger.warning("Failed to clean up output files", error=str(e))
    
    repo_router.logger.info("Job deleted successfully", job_id=str(job_id))
    
    return {"message": f"Job {job_id} deleted successfully"}


@router.get("/health")
async def health_check() -> dict:
    """
    Health check endpoint for repository analysis service.
    """
    return {
        "status": "healthy",
        "service": "repository-analysis",
        "timestamp": datetime.utcnow().isoformat(),
        "active_jobs": len(repo_router.service._active_jobs),
    }