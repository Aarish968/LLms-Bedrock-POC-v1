"""Repository analysis API endpoints."""

from datetime import datetime
from typing import List
from uuid import UUID, uuid4

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status

from api.core.logging import get_logger
from api.dependencies.auth import get_current_active_user, User
from api.v1.models.repository_analysis import (
    AnalysisStatus,
    RepositoryAnalysisJob,
    RepositoryAnalysisRequest,
    RepositoryAnalysisResponse,
    RepositoryAnalysisResults,
)
from api.v1.services.repository_analysis_service import RepositoryAnalysisService

logger = get_logger(__name__)
router = APIRouter()

# Initialize service
analysis_service = RepositoryAnalysisService()


@router.post("/analyze", response_model=RepositoryAnalysisResponse)
async def start_repository_analysis(
    request: RepositoryAnalysisRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_active_user),
):
    """Start repository analysis."""
    logger.info(
        "Starting repository analysis request",
        user_id=current_user.id,
        frontend_repo_name=request.frontend_repo_name,
        backend_repo_name=request.backend_repo_name,
    )
    
    try:
        # Create job
        job_id = uuid4()
        job = RepositoryAnalysisJob(
            job_id=job_id,
            status=AnalysisStatus.PENDING,
            message="Analysis queued for processing",
            started_at=datetime.now(),
            frontend_repo_name=request.frontend_repo_name,
            backend_repo_name=request.backend_repo_name,
        )
        
        # Store job
        analysis_service.create_job(job)
        
        # Add background task
        background_tasks.add_task(
            analysis_service.run_analysis,
            job_id,
            request,
            current_user.id,
        )
        
        return RepositoryAnalysisResponse(
            job_id=job_id,
            status=AnalysisStatus.PENDING,
            message="Repository analysis started. Repositories will be cloned and analyzed.",
            started_at=job.started_at,
        )
        
    except Exception as e:
        logger.error("Failed to start repository analysis", error=str(e), user_id=current_user.id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start analysis: {str(e)}",
        )


@router.get("/status/{job_id}", response_model=RepositoryAnalysisJob)
async def get_analysis_status(
    job_id: UUID,
    current_user: User = Depends(get_current_active_user),
):
    """Get repository analysis job status."""
    logger.info("Getting analysis job status", job_id=str(job_id), user_id=current_user.id)
    
    job = analysis_service.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        )
    
    return job


@router.get("/jobs", response_model=List[RepositoryAnalysisJob])
async def list_analysis_jobs(
    limit: int = 50,
    offset: int = 0,
    current_user: User = Depends(get_current_active_user),
):
    """List repository analysis jobs."""
    logger.info(
        "Listing analysis jobs",
        user_id=current_user.id,
        limit=limit,
        offset=offset,
    )
    
    try:
        jobs = analysis_service.list_jobs(limit=limit, offset=offset)
        return jobs
        
    except Exception as e:
        logger.error("Failed to list analysis jobs", error=str(e), user_id=current_user.id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve jobs: {str(e)}",
        )


@router.delete("/jobs/{job_id}")
async def cancel_analysis_job(
    job_id: UUID,
    current_user: User = Depends(get_current_active_user),
):
    """Cancel a repository analysis job."""
    logger.info("Cancelling analysis job", job_id=str(job_id), user_id=current_user.id)
    
    job = analysis_service.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        )
    
    if job.status not in [AnalysisStatus.PENDING, AnalysisStatus.CLONING, AnalysisStatus.RUNNING]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot cancel job with status: {job.status}",
        )
    
    try:
        success = analysis_service.cancel_job(job_id)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Failed to cancel job",
            )
        
        return {"message": "Job cancelled successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to cancel analysis job", job_id=str(job_id), error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to cancel job: {str(e)}",
        )


@router.get("/results/{job_id}", response_model=RepositoryAnalysisResults)
async def get_analysis_results(
    job_id: UUID,
    current_user: User = Depends(get_current_active_user),
):
    """Get repository analysis results."""
    logger.info("Getting analysis results", job_id=str(job_id), user_id=current_user.id)
    
    job = analysis_service.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        )
    
    if job.status != AnalysisStatus.COMPLETED:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Job is not completed. Current status: {job.status}",
        )
    
    try:
        results_info = analysis_service.get_results_info(job_id)
        if not results_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Results not found",
            )
        
        return RepositoryAnalysisResults(**results_info)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get analysis results", job_id=str(job_id), error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve results: {str(e)}",
        )


# Public endpoints for testing without authentication
@router.post("/public/analyze", response_model=RepositoryAnalysisResponse)
async def start_repository_analysis_public(
    request: RepositoryAnalysisRequest,
    background_tasks: BackgroundTasks,
):
    """Start repository analysis (public endpoint for testing)."""
    logger.info(
        "Starting repository analysis request (public)",
        frontend_repo_name=request.frontend_repo_name,
        backend_repo_name=request.backend_repo_name,
    )
    
    try:
        # Create job
        job_id = uuid4()
        job = RepositoryAnalysisJob(
            job_id=job_id,
            status=AnalysisStatus.PENDING,
            message="Analysis queued for processing",
            started_at=datetime.now(),
            frontend_repo_name=request.frontend_repo_name,
            backend_repo_name=request.backend_repo_name,
        )
        
        # Store job
        analysis_service.create_job(job)
        
        # Add background task
        background_tasks.add_task(
            analysis_service.run_analysis,
            job_id,
            request,
            "public_user",
        )
        
        return RepositoryAnalysisResponse(
            job_id=job_id,
            status=AnalysisStatus.PENDING,
            message="Repository analysis started. Repositories will be cloned and analyzed.",
            started_at=job.started_at,
        )
        
    except Exception as e:
        logger.error("Failed to start repository analysis", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start analysis: {str(e)}",
        )


@router.get("/public/status/{job_id}", response_model=RepositoryAnalysisJob)
async def get_analysis_status_public(job_id: UUID):
    """Get repository analysis job status (public endpoint)."""
    logger.info("Getting analysis job status (public)", job_id=str(job_id))
    
    job = analysis_service.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        )
    
    return job


@router.get("/public/jobs", response_model=List[RepositoryAnalysisJob])
async def list_analysis_jobs_public(limit: int = 50, offset: int = 0):
    """List repository analysis jobs (public endpoint)."""
    logger.info("Listing analysis jobs (public)", limit=limit, offset=offset)
    
    try:
        jobs = analysis_service.list_jobs(limit=limit, offset=offset)
        return jobs
        
    except Exception as e:
        logger.error("Failed to list analysis jobs", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve jobs: {str(e)}",
        )