"""ThoughtSpot liveboard analysis API endpoints."""

import os
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks, Query
from fastapi.responses import FileResponse

from api.core.logging import get_logger
from api.dependencies.auth import get_current_active_user, User
from api.v1.models.thoughtspot_analysis import (
    TSAnalysisRequest,
    TSAnalysisResponse,
    TSAnalysisJob,
    TSResultsResponse,
    TableListResponse,
    TSJobStatus,
)
from api.v1.services.thoughtspot_analysis_service import ThoughtSpotAnalysisService

logger = get_logger(__name__)
router = APIRouter()

# Initialize service
thoughtspot_analysis_service = ThoughtSpotAnalysisService()


@router.post("/analyze", response_model=TSAnalysisResponse)
async def start_thoughtspot_analysis(
    request: TSAnalysisRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_active_user),
):
    """Start ThoughtSpot liveboard analysis."""
    logger.info(
        "Starting ThoughtSpot analysis",
        user_id=current_user.id,
        sf_environment=request.sf_environment,
        max_workers=request.max_workers,
    )
    
    try:
        # Create job
        job = thoughtspot_analysis_service.create_job(request)
        
        # Start background processing
        background_tasks.add_task(
            thoughtspot_analysis_service.process_analysis,
            job.job_id,
            request,
            current_user.id,
        )
        
        return TSAnalysisResponse(
            job_id=job.job_id,
            status=job.status,
            message="ThoughtSpot liveboard analysis started. Use the job_id to check status and retrieve results.",
            results_url=f"/api/v1/thoughtspot-analysis/results/{job.job_id}",
            started_at=job.started_at,
        )
        
    except Exception as e:
        logger.error("Failed to start ThoughtSpot analysis", error=str(e), user_id=current_user.id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start analysis: {str(e)}",
        )


@router.get("/status/{job_id}", response_model=TSAnalysisJob)
async def get_job_status(
    job_id: UUID,
    current_user: User = Depends(get_current_active_user),
):
    """Get status of a ThoughtSpot analysis job."""
    logger.info("Getting ThoughtSpot job status", job_id=str(job_id), user_id=current_user.id)
    
    job = thoughtspot_analysis_service.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        )
    
    return job


@router.get("/results/{job_id}", response_model=TSResultsResponse)
async def get_analysis_results(
    job_id: UUID,
    current_user: User = Depends(get_current_active_user),
):
    """Get ThoughtSpot analysis results."""
    logger.info("Getting ThoughtSpot analysis results", job_id=str(job_id), user_id=current_user.id)
    
    results = thoughtspot_analysis_service.get_results(job_id)
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
    logger.info("Downloading ThoughtSpot analysis results", job_id=str(job_id), user_id=current_user.id)
    
    job = thoughtspot_analysis_service.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found"
        )
    
    if job.status != TSJobStatus.COMPLETED:
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
        filename=f"thoughtspot_analysis_{job_id}.csv",
        media_type="text/csv"
    )


@router.get("/jobs")
async def list_jobs(
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_active_user),
):
    """List ThoughtSpot analysis jobs."""
    logger.info("Listing ThoughtSpot analysis jobs", user_id=current_user.id, limit=limit, offset=offset)
    
    jobs = thoughtspot_analysis_service.list_jobs(limit=limit, offset=offset)
    return {"jobs": jobs}


@router.delete("/jobs/{job_id}")
async def delete_job(
    job_id: UUID,
    current_user: User = Depends(get_current_active_user),
):
    """Delete a ThoughtSpot analysis job and its result file."""
    logger.info("Deleting ThoughtSpot analysis job", job_id=str(job_id), user_id=current_user.id)
    
    success = thoughtspot_analysis_service.delete_job(job_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found"
        )
    
    return {"message": "Job deleted successfully"}


@router.get("/tables", response_model=TableListResponse)
async def list_tables(
    sf_environment: str = Query("prod", description="Snowflake environment"),
    include_views: bool = Query(True, description="Include views in addition to base tables"),
    current_user: User = Depends(get_current_active_user),
):
    """List all tables and views from Snowflake."""
    logger.info("Listing tables", user_id=current_user.id, sf_environment=sf_environment)
    
    try:
        tables = await thoughtspot_analysis_service.get_tables_list(sf_environment, include_views)
        return TableListResponse(
            count=len(tables),
            tables=tables
        )
    except Exception as e:
        logger.error("Failed to fetch tables", error=str(e), user_id=current_user.id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


# Public endpoints (no authentication required)
@router.post("/public/analyze", response_model=TSAnalysisResponse)
async def start_thoughtspot_analysis_public(
    request: TSAnalysisRequest,
    background_tasks: BackgroundTasks,
):
    """Start ThoughtSpot liveboard analysis (public endpoint)."""
    logger.info(
        "Starting ThoughtSpot analysis (public)",
        sf_environment=request.sf_environment,
        max_workers=request.max_workers,
    )
    
    try:
        # Create job
        job = thoughtspot_analysis_service.create_job(request)
        
        # Start background processing
        background_tasks.add_task(
            thoughtspot_analysis_service.process_analysis,
            job.job_id,
            request,
            None,  # No user ID for public endpoint
        )
        
        return TSAnalysisResponse(
            job_id=job.job_id,
            status=job.status,
            message="ThoughtSpot liveboard analysis started. Use the job_id to check status and retrieve results.",
            results_url=f"/api/v1/thoughtspot-analysis/public/results/{job.job_id}",
            started_at=job.started_at,
        )
        
    except Exception as e:
        logger.error("Failed to start ThoughtSpot analysis (public)", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start analysis: {str(e)}",
        )


@router.get("/public/status/{job_id}", response_model=TSAnalysisJob)
async def get_job_status_public(job_id: UUID):
    """Get status of a ThoughtSpot analysis job (public endpoint)."""
    logger.info("Getting ThoughtSpot job status (public)", job_id=str(job_id))
    
    job = thoughtspot_analysis_service.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        )
    
    return job


@router.get("/public/results/{job_id}", response_model=TSResultsResponse)
async def get_analysis_results_public(job_id: UUID):
    """Get ThoughtSpot analysis results (public endpoint)."""
    logger.info("Getting ThoughtSpot analysis results (public)", job_id=str(job_id))
    
    results = thoughtspot_analysis_service.get_results(job_id)
    if not results:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Results not found",
        )
    
    return results


@router.get("/public/results/{job_id}/download")
async def download_results_public(job_id: UUID):
    """Download analysis results CSV file (public endpoint)."""
    logger.info("Downloading ThoughtSpot analysis results (public)", job_id=str(job_id))
    
    job = thoughtspot_analysis_service.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found"
        )
    
    if job.status != TSJobStatus.COMPLETED:
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
        filename=f"thoughtspot_analysis_{job_id}.csv",
        media_type="text/csv"
    )
