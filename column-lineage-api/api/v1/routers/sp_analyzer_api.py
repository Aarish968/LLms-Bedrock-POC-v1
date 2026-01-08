"""Stored procedure analysis API endpoints."""

import os
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks, Query
from fastapi.responses import FileResponse

from api.core.logging import get_logger
from api.dependencies.auth import get_current_active_user, User
from api.v1.models.sp_analysis import (
    SPAnalysisRequest,
    SPAnalysisResponse,
    SPAnalysisJob,
    SPResultsResponse,
    SingleProcedureRequest,
    StoredProcedureAnalysis,
    ProcedureListResponse,
    SPJobStatus,
)
from api.v1.services.sp_analysis_service import SPAnalysisService

logger = get_logger(__name__)
router = APIRouter()

# Initialize service
sp_analysis_service = SPAnalysisService()

@router.post("/analyze", response_model=SPAnalysisResponse)
async def start_sp_analysis(
    request: SPAnalysisRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_active_user),
):
    """Start stored procedure analysis."""
    logger.info(
        "Starting SP analysis",
        user_id=current_user.id,
        sf_environment=request.sf_environment,
        max_workers=request.max_workers,
    )
    
    try:
        # Create job
        job = sp_analysis_service.create_job(request)
        
        # Start background processing
        background_tasks.add_task(
            sp_analysis_service.process_analysis,
            job.job_id,
            request,
            current_user.id,
        )
        
        return SPAnalysisResponse(
            job_id=job.job_id,
            status=job.status,
            message="Stored procedure analysis started. Use the job_id to check status and retrieve results.",
            results_url=f"/api/v1/sp-analysis/results/{job.job_id}",
            started_at=job.started_at,
        )
        
    except Exception as e:
        logger.error("Failed to start SP analysis", error=str(e), user_id=current_user.id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start analysis: {str(e)}",
        )

@router.post("/analyze/single", response_model=StoredProcedureAnalysis)
async def analyze_single_procedure(
    request: SingleProcedureRequest,
    current_user: User = Depends(get_current_active_user),
):
    """Analyze a single stored procedure synchronously."""
    logger.info(
        "Analyzing single procedure",
        user_id=current_user.id,
        procedure_name=request.procedure_name,
    )
    
    try:
        result = await sp_analysis_service.analyze_single_procedure(request)
        
        if not result:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Analysis failed"
            )
            
        return result
        
    except Exception as e:
        logger.error("Single procedure analysis failed", error=str(e), user_id=current_user.id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@router.get("/status/{job_id}", response_model=SPAnalysisJob)
async def get_job_status(
    job_id: UUID,
    current_user: User = Depends(get_current_active_user),
):
    """Get status of a stored procedure analysis job."""
    logger.info("Getting SP job status", job_id=str(job_id), user_id=current_user.id)
    
    job = sp_analysis_service.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        )
    
    return job

@router.get("/results/{job_id}", response_model=SPResultsResponse)
async def get_analysis_results(
    job_id: UUID,
    current_user: User = Depends(get_current_active_user),
):
    """Get stored procedure analysis results."""
    logger.info("Getting SP analysis results", job_id=str(job_id), user_id=current_user.id)
    
    results = sp_analysis_service.get_results(job_id)
    if not results:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Results not found",
        )
    
    return results


@router.get("/results/{job_id}/download")
async def download_results(
    job_id: UUID,
    current_user: User = Depends(get_current_active_user),
):
    """Download analysis results CSV file."""
    logger.info("Downloading SP analysis results", job_id=str(job_id), user_id=current_user.id)
    
    job = sp_analysis_service.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found"
        )
    
    if job.status != SPJobStatus.COMPLETED:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Job not completed yet"
        )
    
    if not job.result_file or not os.path.exists(job.result_file):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Result file not found"
        )
    
    return FileResponse(
        path=job.result_file,
        filename=f"sp_analysis_{job_id}.csv",
        media_type="text/csv"
    )

@router.get("/jobs")
async def list_jobs(
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_active_user),
):
    """List stored procedure analysis jobs."""
    logger.info("Listing SP analysis jobs", user_id=current_user.id, limit=limit, offset=offset)
    
    jobs = sp_analysis_service.list_jobs(limit=limit, offset=offset)
    return {"jobs": jobs}


@router.delete("/jobs/{job_id}")
async def delete_job(
    job_id: UUID,
    current_user: User = Depends(get_current_active_user),
):
    """Delete a stored procedure analysis job and its result file."""
    logger.info("Deleting SP analysis job", job_id=str(job_id), user_id=current_user.id)
    
    success = sp_analysis_service.delete_job(job_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found"
        )
    
    return {"message": "Job deleted successfully"}

@router.get("/procedures", response_model=ProcedureListResponse)
async def list_stored_procedures(
    sf_environment: str = Query("prod", description="Snowflake environment"),
    current_user: User = Depends(get_current_active_user),
):
    """List all stored procedures from Snowflake."""
    logger.info("Listing stored procedures", user_id=current_user.id, sf_environment=sf_environment)
    
    try:
        procedures = await sp_analysis_service.get_procedures_list(sf_environment)
        return ProcedureListResponse(
            count=len(procedures),
            procedures=procedures
        )
    except Exception as e:
        logger.error("Failed to fetch procedures", error=str(e), user_id=current_user.id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


# Public endpoints (no authentication required)
@router.post("/public/analyze", response_model=SPAnalysisResponse)
async def start_sp_analysis_public(
    request: SPAnalysisRequest,
    background_tasks: BackgroundTasks,
):
    """Start stored procedure analysis (public endpoint)."""
    logger.info(
        "Starting SP analysis (public)",
        sf_environment=request.sf_environment,
        max_workers=request.max_workers,
    )
    
    try:
        # Create job
        job = sp_analysis_service.create_job(request)
        
        # Start background processing
        background_tasks.add_task(
            sp_analysis_service.process_analysis,
            job.job_id,
            request,
            None,  # No user ID for public endpoint
        )
        
        return SPAnalysisResponse(
            job_id=job.job_id,
            status=job.status,
            message="Stored procedure analysis started. Use the job_id to check status and retrieve results.",
            results_url=f"/api/v1/sp-analysis/public/results/{job.job_id}",
            started_at=job.started_at,
        )
        
    except Exception as e:
        logger.error("Failed to start SP analysis (public)", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start analysis: {str(e)}",
        )


@router.get("/public/status/{job_id}", response_model=SPAnalysisJob)
async def get_job_status_public(job_id: UUID):
    """Get status of a stored procedure analysis job (public endpoint)."""
    logger.info("Getting SP job status (public)", job_id=str(job_id))
    
    job = sp_analysis_service.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        )
    
    return job


@router.get("/public/results/{job_id}", response_model=SPResultsResponse)
async def get_analysis_results_public(job_id: UUID):
    """Get stored procedure analysis results (public endpoint)."""
    logger.info("Getting SP analysis results (public)", job_id=str(job_id))
    
    results = sp_analysis_service.get_results(job_id)
    if not results:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Results not found",
        )
    
    return results