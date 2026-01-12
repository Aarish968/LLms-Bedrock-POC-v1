"""Prefect repository analysis API endpoints."""

import os
from datetime import datetime
from typing import List, Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status, Query
from fastapi.responses import FileResponse

from api.core.logging import get_logger
from api.dependencies.auth import get_current_active_user, User
from api.v1.models.prefect_analysis import (
    PrefectAnalysisRequest,
    PrefectAnalysisResponse,
    PrefectAnalysisJob,
    PrefectAnalysisResults,
    PrefectAnalysisStatus,
    PrefectDiscoveryResults,
    PrefectAnalysisSummary,
)
from api.v1.services.prefect_analysis_service import PrefectAnalysisService

logger = get_logger(__name__)
router = APIRouter()

# Initialize service
prefect_analysis_service = PrefectAnalysisService()


@router.post("/analyze", response_model=PrefectAnalysisResponse)
async def start_prefect_analysis(
    request: PrefectAnalysisRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_active_user),
):
    """Start Prefect repository analysis."""
    logger.info(
        "Starting Prefect repository analysis request",
        user_id=current_user.id,
        sf_environment=request.sf_environment,
        max_workers=request.max_workers,
        target_directory=request.target_directory,
    )
    
    try:
        # Create job
        job_id = uuid4()
        job = PrefectAnalysisJob(
            job_id=job_id,
            status=PrefectAnalysisStatus.PENDING,
            message="Prefect analysis queued for processing",
            started_at=datetime.now(),
            sf_environment=request.sf_environment,
            max_workers=request.max_workers,
            target_directory=request.target_directory,
            request_params=request.model_dump(),
        )
        
        # Store job
        prefect_analysis_service.create_job(job)
        
        # Add background task
        background_tasks.add_task(
            prefect_analysis_service.run_analysis,
            job_id,
            request,
            current_user.id,
        )
        
        return PrefectAnalysisResponse(
            job_id=job_id,
            status=PrefectAnalysisStatus.PENDING,
            message="Prefect repository analysis started. Repositories will be discovered, cloned, and analyzed for table-column references.",
            started_at=job.started_at,
            results_url=f"/api/v1/prefect-analysis/results/{job_id}",
        )
        
    except Exception as e:
        logger.error("Failed to start Prefect analysis", error=str(e), user_id=current_user.id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start analysis: {str(e)}",
        )


@router.get("/status/{job_id}", response_model=PrefectAnalysisJob)
async def get_analysis_status(
    job_id: UUID,
    current_user: User = Depends(get_current_active_user),
):
    """Get Prefect analysis job status."""
    logger.info("Getting Prefect analysis job status", job_id=str(job_id), user_id=current_user.id)
    
    job = prefect_analysis_service.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        )
    
    return job


@router.get("/jobs", response_model=List[PrefectAnalysisJob])
async def list_analysis_jobs(
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_active_user),
):
    """List Prefect analysis jobs."""
    logger.info(
        "Listing Prefect analysis jobs",
        user_id=current_user.id,
        limit=limit,
        offset=offset,
    )
    
    try:
        jobs = prefect_analysis_service.list_jobs(limit=limit, offset=offset)
        return jobs
        
    except Exception as e:
        logger.error("Failed to list Prefect analysis jobs", error=str(e), user_id=current_user.id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve jobs: {str(e)}",
        )


@router.delete("/jobs/{job_id}")
async def cancel_analysis_job(
    job_id: UUID,
    current_user: User = Depends(get_current_active_user),
):
    """Cancel a Prefect analysis job."""
    logger.info("Cancelling Prefect analysis job", job_id=str(job_id), user_id=current_user.id)
    
    job = prefect_analysis_service.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        )
    
    if job.status not in [PrefectAnalysisStatus.PENDING, PrefectAnalysisStatus.CLONING, PrefectAnalysisStatus.ANALYZING]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot cancel job with status: {job.status}",
        )
    
    try:
        success = prefect_analysis_service.cancel_job(job_id)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Failed to cancel job",
            )
        
        return {"message": "Job cancelled successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to cancel Prefect analysis job", job_id=str(job_id), error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to cancel job: {str(e)}",
        )


@router.get("/results/{job_id}", response_model=PrefectAnalysisResults)
async def get_analysis_results(
    job_id: UUID,
    current_user: User = Depends(get_current_active_user),
):
    """Get Prefect analysis results."""
    logger.info("Getting Prefect analysis results", job_id=str(job_id), user_id=current_user.id)
    
    job = prefect_analysis_service.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        )
    
    if job.status != PrefectAnalysisStatus.COMPLETED:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Job is not completed. Current status: {job.status}",
        )
    
    try:
        results_info = prefect_analysis_service.get_results_info(job_id)
        if not results_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Results not found",
            )
        
        return PrefectAnalysisResults(**results_info)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get Prefect analysis results", job_id=str(job_id), error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve results: {str(e)}",
        )


@router.get("/results/{job_id}/download")
async def download_results(
    job_id: UUID,
    current_user: User = Depends(get_current_active_user),
):
    """Download Prefect analysis results CSV file."""
    logger.info("Downloading Prefect analysis results", job_id=str(job_id), user_id=current_user.id)
    
    job = prefect_analysis_service.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found"
        )
    
    if job.status != PrefectAnalysisStatus.COMPLETED:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Job not completed yet"
        )
    
    if not job.output_file or not os.path.exists(job.output_file):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Result file not found"
        )
    
    return FileResponse(
        path=job.output_file,
        filename=f"prefect_analysis_{job_id}.csv",
        media_type="text/csv"
    )


@router.get("/database-results")
async def get_database_results(
    job_id: Optional[str] = Query(None, description="Filter by specific job ID"),
    limit: Optional[int] = Query(100, ge=1, le=1000, description="Maximum number of records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    current_user: User = Depends(get_current_active_user),
):
    """Get Prefect analysis results from database table."""
    logger.info(
        "Getting Prefect database results",
        job_id=job_id,
        limit=limit,
        offset=offset,
        user_id=current_user.id
    )
    
    try:
        # Check if database connection is available
        if prefect_analysis_service.db_manager.mock_mode:
            return {
                "message": "Database connection not available (mock mode)",
                "total_records": 0,
                "records": [],
                "limit": limit,
                "offset": offset
            }
        
        table_name = "CPS_DB.CPS_DSCI_BR.PREFECT_TABLE_COLUMN_REFERENCES"
        
        # Build query with optional job_id filter
        where_clause = ""
        if job_id:
            where_clause = f"WHERE JOB_ID = '{job_id}'"
        
        # Count total records
        count_query = f"SELECT COUNT(*) as total FROM {table_name} {where_clause}"
        count_result = prefect_analysis_service.db_manager.execute_query(count_query)
        total_records = count_result[0][0] if count_result else 0
        
        # Get paginated data
        query = f"""
        SELECT 
            JOB_ID,
            REPO_NAME,
            FUNCTION_NAME,
            TABLE_NAME,
            COLUMN_NAME,
            FILE_NAME,
            ANALYSIS_TIMESTAMP,
            CREATED_AT
        FROM {table_name}
        {where_clause}
        ORDER BY CREATED_AT DESC, REPO_NAME, FUNCTION_NAME
        """
        
        if limit:
            query += f" LIMIT {limit}"
        if offset:
            query += f" OFFSET {offset}"
        
        results = prefect_analysis_service.db_manager.execute_query(query)
        
        # Convert results to list of dictionaries
        records = []
        for row in results:
            records.append({
                "job_id": row[0],
                "repo_name": row[1],
                "function_name": row[2],
                "table_name": row[3],
                "column_name": row[4],
                "file_name": row[5],
                "analysis_timestamp": row[6].isoformat() if row[6] else None,
                "created_at": row[7].isoformat() if row[7] else None,
            })
        
        return {
            "table_name": "PREFECT_TABLE_COLUMN_REFERENCES",
            "total_records": total_records,
            "records": records,
            "limit": limit,
            "offset": offset,
            "job_id_filter": job_id
        }
        
    except Exception as e:
        logger.error("Failed to get Prefect database results", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve database results: {str(e)}",
        )


# Public endpoints for testing without authentication
@router.post("/public/analyze", response_model=PrefectAnalysisResponse)
async def start_prefect_analysis_public(
    request: PrefectAnalysisRequest,
    background_tasks: BackgroundTasks,
):
    """Start Prefect repository analysis (public endpoint for testing)."""
    logger.info(
        "Starting Prefect repository analysis request (public)",
        sf_environment=request.sf_environment,
        max_workers=request.max_workers,
        target_directory=request.target_directory,
    )
    
    try:
        # Create job
        job_id = uuid4()
        job = PrefectAnalysisJob(
            job_id=job_id,
            status=PrefectAnalysisStatus.PENDING,
            message="Prefect analysis queued for processing",
            started_at=datetime.now(),
            sf_environment=request.sf_environment,
            max_workers=request.max_workers,
            target_directory=request.target_directory,
            request_params=request.model_dump(),
        )
        
        # Store job
        prefect_analysis_service.create_job(job)
        
        # Add background task
        background_tasks.add_task(
            prefect_analysis_service.run_analysis,
            job_id,
            request,
            "public_user",
        )
        
        return PrefectAnalysisResponse(
            job_id=job_id,
            status=PrefectAnalysisStatus.PENDING,
            message="Prefect repository analysis started. Repositories will be discovered, cloned, and analyzed for table-column references.",
            started_at=job.started_at,
            results_url=f"/api/v1/prefect-analysis/public/results/{job_id}",
        )
        
    except Exception as e:
        logger.error("Failed to start Prefect analysis (public)", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start analysis: {str(e)}",
        )


@router.get("/public/status/{job_id}", response_model=PrefectAnalysisJob)
async def get_analysis_status_public(job_id: UUID):
    """Get Prefect analysis job status (public endpoint)."""
    logger.info("Getting Prefect analysis job status (public)", job_id=str(job_id))
    
    job = prefect_analysis_service.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        )
    
    return job


@router.get("/public/jobs", response_model=List[PrefectAnalysisJob])
async def list_analysis_jobs_public(
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0)
):
    """List Prefect analysis jobs (public endpoint)."""
    logger.info("Listing Prefect analysis jobs (public)", limit=limit, offset=offset)
    
    try:
        jobs = prefect_analysis_service.list_jobs(limit=limit, offset=offset)
        return jobs
        
    except Exception as e:
        logger.error("Failed to list Prefect analysis jobs (public)", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve jobs: {str(e)}",
        )


@router.get("/public/results/{job_id}", response_model=PrefectAnalysisResults)
async def get_analysis_results_public(job_id: UUID):
    """Get Prefect analysis results (public endpoint)."""
    logger.info("Getting Prefect analysis results (public)", job_id=str(job_id))
    
    job = prefect_analysis_service.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        )
    
    if job.status != PrefectAnalysisStatus.COMPLETED:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Job is not completed. Current status: {job.status}",
        )
    
    try:
        results_info = prefect_analysis_service.get_results_info(job_id)
        if not results_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Results not found",
            )
        
        return PrefectAnalysisResults(**results_info)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get Prefect analysis results (public)", job_id=str(job_id), error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve results: {str(e)}",
        )


@router.get("/public/database-results")
async def get_database_results_public(
    job_id: Optional[str] = Query(None, description="Filter by specific job ID"),
    limit: Optional[int] = Query(100, ge=1, le=1000, description="Maximum number of records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
):
    """Get Prefect analysis results from database table (public endpoint)."""
    logger.info(
        "Getting Prefect database results (public)",
        job_id=job_id,
        limit=limit,
        offset=offset
    )
    
    try:
        # Check if database connection is available
        if prefect_analysis_service.db_manager.mock_mode:
            return {
                "message": "Database connection not available (mock mode)",
                "total_records": 0,
                "records": [],
                "limit": limit,
                "offset": offset
            }
        
        table_name = "CPS_DB.CPS_DSCI_BR.PREFECT_TABLE_COLUMN_REFERENCES"
        
        # Build query with optional job_id filter
        where_clause = ""
        if job_id:
            where_clause = f"WHERE JOB_ID = '{job_id}'"
        
        # Count total records
        count_query = f"SELECT COUNT(*) as total FROM {table_name} {where_clause}"
        count_result = prefect_analysis_service.db_manager.execute_query(count_query)
        total_records = count_result[0][0] if count_result else 0
        
        # Get paginated data
        query = f"""
        SELECT 
            JOB_ID,
            REPO_NAME,
            FUNCTION_NAME,
            TABLE_NAME,
            COLUMN_NAME,
            FILE_NAME,
            ANALYSIS_TIMESTAMP,
            CREATED_AT
        FROM {table_name}
        {where_clause}
        ORDER BY CREATED_AT DESC, REPO_NAME, FUNCTION_NAME
        """
        
        if limit:
            query += f" LIMIT {limit}"
        if offset:
            query += f" OFFSET {offset}"
        
        results = prefect_analysis_service.db_manager.execute_query(query)
        
        # Convert results to list of dictionaries
        records = []
        for row in results:
            records.append({
                "job_id": row[0],
                "repo_name": row[1],
                "function_name": row[2],
                "table_name": row[3],
                "column_name": row[4],
                "file_name": row[5],
                "analysis_timestamp": row[6].isoformat() if row[6] else None,
                "created_at": row[7].isoformat() if row[7] else None,
            })
        
        return {
            "table_name": "PREFECT_TABLE_COLUMN_REFERENCES",
            "total_records": total_records,
            "records": records,
            "limit": limit,
            "offset": offset,
            "job_id_filter": job_id
        }
        
    except Exception as e:
        logger.error("Failed to get Prefect database results (public)", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve database results: {str(e)}",
        )