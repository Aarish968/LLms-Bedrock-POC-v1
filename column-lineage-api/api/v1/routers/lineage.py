"""Column lineage API endpoints."""

from datetime import datetime
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks, Response
from fastapi.responses import StreamingResponse

from api.core.logging import get_logger
from api.dependencies.auth import get_current_active_user, User
from api.v1.models.lineage import (
    LineageAnalysisRequest,
    LineageAnalysisResponse,
    LineageAnalysisJob,
    LineageResultsResponse,
    LineageExportRequest,
    ViewInfo,
    JobStatus,
    BaseViewRecord,
    BaseViewResponse,
    BaseViewCreateRequest,
    BaseViewUpdateRequest,
)
from api.v1.services.lineage_service import LineageService
from api.v1.services.job_manager import JobManager

logger = get_logger(__name__)
router = APIRouter()

# Initialize services
lineage_service = LineageService()
job_manager = JobManager()


@router.post("/analyze", response_model=LineageAnalysisResponse)
async def start_lineage_analysis(
    request: LineageAnalysisRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_active_user),
):
    """Start column lineage analysis."""
    logger.info(
        "Starting lineage analysis",
        user_id=current_user.id,
        view_count=len(request.view_names) if request.view_names else "all",
        async_processing=request.async_processing,
    )
    
    try:
        # Create job immediately
        job = LineageAnalysisJob(
            total_views=0,  # Will be updated during processing
            request_params=request.model_dump(),
        )
        
        logger.info("Created job", job_id=str(job.job_id))
        
        # Store job immediately
        job_manager.create_job(job)
        
        # Always use async processing to prevent frontend timeouts
        logger.info("Adding background task", job_id=str(job.job_id))
        background_tasks.add_task(
            lineage_service.process_lineage_analysis,
            job.job_id,
            request,
            current_user.id,
        )
        
        # Return immediately with PENDING status
        return LineageAnalysisResponse(
            job_id=job.job_id,
            status=JobStatus.PENDING,
            message="Analysis started. Use the job_id to check status and retrieve results.",
            results_url=f"/api/v1/lineage/results/{job.job_id}",
        )
            
    except Exception as e:
        logger.error("Failed to start lineage analysis", error=str(e), user_id=current_user.id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start analysis: {str(e)}",
        )


@router.get("/status/{job_id}", response_model=LineageAnalysisJob)
async def get_job_status(
    job_id: UUID,
    current_user: User = Depends(get_current_active_user),
):
    """Get lineage analysis job status."""
    logger.info("Getting job status", job_id=str(job_id), user_id=current_user.id)
    
    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        )
    
    return job


@router.get("/results/{job_id}", response_model=LineageResultsResponse)
async def get_lineage_results(
    job_id: UUID,
    limit: Optional[int] = None,
    offset: int = 0,
    current_user: User = Depends(get_current_active_user),
):
    """Get lineage analysis results."""
    logger.info(
        "Getting lineage results",
        job_id=str(job_id),
        user_id=current_user.id,
        limit=limit,
        offset=offset,
    )
    
    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        )
    
    if job.status not in [JobStatus.COMPLETED, JobStatus.FAILED]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Job is not completed. Current status: {job.status}",
        )
    
    try:
        results = job_manager.get_job_results(job_id, limit=limit, offset=offset)
        summary = job_manager.get_job_summary(job_id)
        
        return LineageResultsResponse(
            job_id=job_id,
            status=job.status,
            total_results=job.results_count,
            results=results,
            summary=summary,
        )
        
    except Exception as e:
        logger.error("Failed to get results", job_id=str(job_id), error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve results: {str(e)}",
        )


@router.get("/databases", response_model=List[str])
async def list_available_databases(
    current_user: User = Depends(get_current_active_user),
):
    """List available databases."""
    logger.info("Listing available databases", user_id=current_user.id)
    
    try:
        databases = await lineage_service.get_available_databases()
        return databases
        
    except Exception as e:
        logger.error("Failed to list databases", error=str(e), user_id=current_user.id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve databases: {str(e)}",
        )


@router.get("/schemas", response_model=List[str])
async def list_available_schemas(
    database_filter: str,
    current_user: User = Depends(get_current_active_user),
):
    """List available schemas for a specific database."""
    logger.info(
        "Listing available schemas", 
        database_filter=database_filter,
        user_id=current_user.id
    )
    
    try:
        schemas = await lineage_service.get_available_schemas(database_filter)
        return schemas
        
    except Exception as e:
        logger.error("Failed to list schemas", error=str(e), user_id=current_user.id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve schemas: {str(e)}",
        )


@router.get("/public/databases", response_model=List[str])
async def list_available_databases_public():
    """List available databases (public endpoint for testing)."""
    logger.info("Listing available databases (public)")
    
    try:
        databases = await lineage_service.get_available_databases()
        return databases
        
    except Exception as e:
        logger.error("Failed to list databases", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve databases: {str(e)}",
        )


@router.get("/public/schemas", response_model=List[str])
async def list_available_schemas_public(
    database_filter: str,
):
    """List available schemas for a specific database (public endpoint for testing)."""
    logger.info("Listing available schemas (public)", database_filter=database_filter)
    
    try:
        schemas = await lineage_service.get_available_schemas(database_filter)
        return schemas
        
    except Exception as e:
        logger.error("Failed to list schemas", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve schemas: {str(e)}",
        )


@router.get("/views", response_model=List[ViewInfo])
async def list_available_views(
    schema_filter: str,  # Made mandatory
    database_filter: str,  # Added mandatory database filter
    current_user: User = Depends(get_current_active_user),
):
    """
    List available database views with mandatory schema and database filters.
    
    Parameters:
    - schema_filter: Schema name to filter views (required)
    - database_filter: Database name to filter views (required)
    """
    logger.info(
        "Listing available views",
        user_id=current_user.id,
        schema_filter=schema_filter,
        database_filter=database_filter,
    )
    
    try:
        views = await lineage_service.get_available_views(
            schema_filter=schema_filter,
            database_filter=database_filter,
        )
        return views
        
    except Exception as e:
        logger.error("Failed to list views", error=str(e), user_id=current_user.id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve views: {str(e)}",
        )


@router.get("/public/views", response_model=List[ViewInfo])
async def list_available_views_public(
    schema_filter: str,  # Made mandatory
    database_filter: str,  # Added mandatory database filter
):
    """
    List available database views (public endpoint for testing) with mandatory filters.
    
    Parameters:
    - schema_filter: Schema name to filter views (required)
    - database_filter: Database name to filter views (required)
    """
    logger.info(
        "Listing available views (public)",
        schema_filter=schema_filter,
        database_filter=database_filter,
    )
    
    try:
        views = await lineage_service.get_available_views(
            schema_filter=schema_filter,
            database_filter=database_filter,
        )
        return views
        
    except Exception as e:
        logger.error("Failed to list views", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve views: {str(e)}",
        )


@router.post("/export/{job_id}")
async def export_lineage_results(
    job_id: UUID,
    export_request: LineageExportRequest,
    current_user: User = Depends(get_current_active_user),
):
    """Export lineage analysis results."""
    logger.info(
        "Exporting lineage results",
        job_id=str(job_id),
        user_id=current_user.id,
        format=export_request.format,
    )
    
    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        )
    
    if job.status != JobStatus.COMPLETED:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Job is not completed. Current status: {job.status}",
        )
    
    try:
        # Get results
        results = job_manager.get_job_results(job_id)
        
        # Filter by confidence if specified
        if export_request.filter_by_confidence is not None:
            results = [
                r for r in results 
                if r.confidence_score >= export_request.filter_by_confidence
            ]
        
        # Generate export
        export_data = await lineage_service.export_results(
            results, 
            export_request.format,
            include_metadata=export_request.include_metadata,
        )
        
        # Determine content type and filename
        if export_request.format.lower() == "csv":
            content_type = "text/csv"
            filename = f"lineage_results_{job_id}.csv"
        elif export_request.format.lower() == "json":
            content_type = "application/json"
            filename = f"lineage_results_{job_id}.json"
        elif export_request.format.lower() == "excel":
            content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            filename = f"lineage_results_{job_id}.xlsx"
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported export format: {export_request.format}",
            )
        
        return StreamingResponse(
            export_data,
            media_type=content_type,
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )
        
    except Exception as e:
        logger.error("Failed to export results", job_id=str(job_id), error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to export results: {str(e)}",
        )


@router.delete("/jobs/{job_id}")
async def cancel_job(
    job_id: UUID,
    current_user: User = Depends(get_current_active_user),
):
    """Cancel a running job."""
    logger.info("Cancelling job", job_id=str(job_id), user_id=current_user.id)
    
    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        )
    
    if job.status not in [JobStatus.PENDING, JobStatus.RUNNING]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot cancel job with status: {job.status}",
        )
    
    try:
        job_manager.cancel_job(job_id)
        return {"message": "Job cancelled successfully"}
        
    except Exception as e:
        logger.error("Failed to cancel job", job_id=str(job_id), error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to cancel job: {str(e)}",
        )


@router.get("/jobs", response_model=List[LineageAnalysisJob])
async def list_jobs(
    status_filter: Optional[JobStatus] = None,
    limit: int = 50,
    offset: int = 0,
    current_user: User = Depends(get_current_active_user),
):
    """List lineage analysis jobs."""
    logger.info(
        "Listing jobs",
        user_id=current_user.id,
        status_filter=status_filter,
        limit=limit,
        offset=offset,
    )
    
    try:
        jobs = job_manager.list_jobs(
            status_filter=status_filter,
            limit=limit,
            offset=offset,
        )
        return jobs
        
    except Exception as e:
        logger.error("Failed to list jobs", error=str(e), user_id=current_user.id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve jobs: {str(e)}",
        )


@router.get("/public/base-view", response_model=BaseViewResponse)
async def get_base_view_data(
    mock: bool = False,  # Add mock parameter for testing
):
    """
    Public endpoint to retrieve data from configurable BASE_VIEW table.
    
    This endpoint does not require authentication and can be accessed publicly.
    Returns data from the BASE_VIEW table configured in environment variables
    with BASE_PRIMARY_ID and TABLE_NAME columns.
    
    Parameters:
    - mock: Force mock mode for testing (default: False)
    """
    logger.info(
        "Getting BASE_VIEW data",
        mock_mode=mock,
    )
    
    try:
        # Get database connection
        from api.dependencies.database import get_database_engine
        engine = get_database_engine()
        
        
        if not engine or mock:
            # Return empty response when no database connection or in mock mode
            logger.info("No database connection available or mock mode enabled")
            return BaseViewResponse(
                total_records=0,
                records=[],
            )
        
        # Query the actual Snowflake database
        logger.info("Querying Snowflake database")
        with engine.connect() as connection:
            from sqlalchemy import text
            from api.core.config import get_settings
            
            settings = get_settings()
            base_view_table = settings.BASE_VIEW_TABLE
            
            # Count total records
            count_query = text(f"SELECT COUNT(*) as total FROM {base_view_table}")
            count_result = connection.execute(count_query)
            total_records = count_result.fetchone()[0]
            
            # Get all data - no pagination
            data_query = text(f"""
                SELECT BASE_PRIMARY_ID, TABLE_NAME 
                FROM {base_view_table} 
                ORDER BY BASE_PRIMARY_ID
            """)
            result = connection.execute(data_query)
            
            records = [
                BaseViewRecord(base_primary_id=row[0], table_name=row[1])
                for row in result.fetchall()
            ]
            
            return BaseViewResponse(
                total_records=total_records,
                records=records,
            )
            
    except Exception as e:
        logger.error("Failed to retrieve BASE_VIEW data", error=str(e))
        
        # Fallback to empty response on any error
        logger.info("Falling back to empty response due to error")
        return BaseViewResponse(
            total_records=0,
            records=[],
        )


@router.post("/public/base-view", response_model=BaseViewRecord, status_code=status.HTTP_201_CREATED)
async def create_base_view_record(
    request: BaseViewCreateRequest,
):
    """
    Create a new record in the configurable BASE_VIEW table.
    
    This endpoint does not require authentication and can be accessed publicly.
    Creates a new record with the provided primary ID and table name.
    """
    logger.info(
        "Creating new BASE_VIEW record",
        base_primary_id=request.base_primary_id,
        table_name=request.table_name,
    )
    
    try:
        # Get database connection
        from api.dependencies.database import get_database_engine
        engine = get_database_engine()
        
        if not engine:
            logger.error("No database connection available")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database connection not available",
            )
        
        # Insert new record into Snowflake database
        with engine.connect() as connection:
            from sqlalchemy import text
            from api.core.config import get_settings
            
            settings = get_settings()
            base_view_table = settings.BASE_VIEW_TABLE
            
            # Check if primary ID already exists
            check_query = text(f"SELECT COUNT(*) FROM {base_view_table} WHERE BASE_PRIMARY_ID = :base_primary_id")
            check_result = connection.execute(check_query, {"base_primary_id": request.base_primary_id})
            exists = check_result.fetchone()[0] > 0
            
            if exists:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Record with primary ID {request.base_primary_id} already exists",
                )
            
            # Insert new record
            insert_query = text(f"""
                INSERT INTO {base_view_table} (BASE_PRIMARY_ID, TABLE_NAME) 
                VALUES (:base_primary_id, :table_name)
            """)
            
            connection.execute(insert_query, {
                "base_primary_id": request.base_primary_id,
                "table_name": request.table_name
            })
            connection.commit()
            
            logger.info(
                "BASE_VIEW record created successfully",
                base_primary_id=request.base_primary_id,
                table_name=request.table_name,
            )
            
            return BaseViewRecord(
                base_primary_id=request.base_primary_id,
                table_name=request.table_name
            )
            
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error("Failed to create BASE_VIEW record", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create record: {str(e)}",
        )


@router.put("/public/base-view/{base_primary_id}", response_model=BaseViewRecord)
async def update_base_view_record(
    base_primary_id: int,
    request: BaseViewUpdateRequest,
):
    """
    Update an existing record in the configurable BASE_VIEW table.
    
    This endpoint does not require authentication and can be accessed publicly.
    Updates the table name for the record with the specified primary ID.
    """
    logger.info(
        "Updating BASE_VIEW record",
        base_primary_id=base_primary_id,
        new_table_name=request.table_name,
    )
    
    try:
        # Get database connection
        from api.dependencies.database import get_database_engine
        engine = get_database_engine()
        
        if not engine:
            logger.error("No database connection available")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database connection not available",
            )
        
        # Update record in Snowflake database
        with engine.connect() as connection:
            from sqlalchemy import text
            from api.core.config import get_settings
            
            settings = get_settings()
            base_view_table = settings.BASE_VIEW_TABLE
            
            # Check if record exists
            check_query = text(f"SELECT COUNT(*) FROM {base_view_table} WHERE BASE_PRIMARY_ID = :base_primary_id")
            check_result = connection.execute(check_query, {"base_primary_id": base_primary_id})
            exists = check_result.fetchone()[0] > 0
            
            if not exists:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Record with primary ID {base_primary_id} not found",
                )
            
            # Update the record
            update_query = text(f"""
                UPDATE {base_view_table} 
                SET TABLE_NAME = :table_name 
                WHERE BASE_PRIMARY_ID = :base_primary_id
            """)
            
            result = connection.execute(update_query, {
                "base_primary_id": base_primary_id,
                "table_name": request.table_name
            })
            connection.commit()
            
            if result.rowcount == 0:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Record with primary ID {base_primary_id} not found",
                )
            
            logger.info(
                "BASE_VIEW record updated successfully",
                base_primary_id=base_primary_id,
                new_table_name=request.table_name,
            )
            
            return BaseViewRecord(
                base_primary_id=base_primary_id,
                table_name=request.table_name
            )
            
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error("Failed to update BASE_VIEW record", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update record: {str(e)}",
        )


@router.delete("/public/base-view/{base_primary_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_base_view_record(
    base_primary_id: int,
):
    """
    Delete a record from the configurable BASE_VIEW table.
    
    This endpoint does not require authentication and can be accessed publicly.
    Deletes the record with the specified primary ID.
    """
    logger.info("Deleting BASE_VIEW record", base_primary_id=base_primary_id)
    
    try:
        # Get database connection
        from api.dependencies.database import get_database_engine
        engine = get_database_engine()
        
        if not engine:
            logger.error("No database connection available")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database connection not available",
            )
        
        # Delete record from Snowflake database
        with engine.connect() as connection:
            from sqlalchemy import text
            from api.core.config import get_settings
            
            settings = get_settings()
            base_view_table = settings.BASE_VIEW_TABLE
            
            # Delete the record
            delete_query = text(f"DELETE FROM {base_view_table} WHERE BASE_PRIMARY_ID = :base_primary_id")
            result = connection.execute(delete_query, {"base_primary_id": base_primary_id})
            connection.commit()
            
            if result.rowcount == 0:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Record with primary ID {base_primary_id} not found",
                )
            
            logger.info("BASE_VIEW record deleted successfully", base_primary_id=base_primary_id)
            
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error("Failed to delete BASE_VIEW record", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete record: {str(e)}",
        )


@router.get("/saved-results")
async def list_saved_results(
    current_user: User = Depends(get_current_active_user),
):
    """List all saved CSV result files."""
    logger.info("Listing saved result files", user_id=current_user.id)
    
    try:
        from api.core.config import get_settings
        from pathlib import Path
        import os
        
        settings = get_settings()
        results_dir = Path(settings.RESULTS_DIRECTORY)
        
        if not results_dir.exists():
            return {"files": [], "message": "No results directory found"}
        
        # Get all CSV files in the results directory
        csv_files = []
        for file_path in results_dir.glob("lineage_analysis_*.csv"):
            stat = file_path.stat()
            csv_files.append({
                "filename": file_path.name,
                "filepath": str(file_path),
                "size_bytes": stat.st_size,
                "created_at": datetime.fromtimestamp(stat.st_ctime).isoformat(),
                "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            })
        
        # Sort by creation time (newest first)
        csv_files.sort(key=lambda x: x["created_at"], reverse=True)
        
        return {
            "files": csv_files,
            "total_files": len(csv_files),
            "directory": str(results_dir)
        }
        
    except Exception as e:
        logger.error("Failed to list saved results", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list saved results: {str(e)}",
        )


@router.get("/public/saved-results")
async def list_saved_results_public():
    """List all saved CSV result files (public endpoint)."""
    logger.info("Listing saved result files (public)")
    
    try:
        from api.core.config import get_settings
        from pathlib import Path
        
        settings = get_settings()
        results_dir = Path(settings.RESULTS_DIRECTORY)
        
        if not results_dir.exists():
            return {"files": [], "message": "No results directory found"}
        
        # Get all CSV files in the results directory
        csv_files = []
        for file_path in results_dir.glob("lineage_analysis_*.csv"):
            stat = file_path.stat()
            csv_files.append({
                "filename": file_path.name,
                "filepath": str(file_path),
                "size_bytes": stat.st_size,
                "created_at": datetime.fromtimestamp(stat.st_ctime).isoformat(),
                "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            })
        
        # Sort by creation time (newest first)
        csv_files.sort(key=lambda x: x["created_at"], reverse=True)
        
        return {
            "files": csv_files,
            "total_files": len(csv_files),
            "directory": str(results_dir)
        }
        
    except Exception as e:
        logger.error("Failed to list saved results", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list saved results: {str(e)}",
        )


@router.get("/database-results/{database_name}/{schema_name}")
async def get_database_results(
    database_name: str,
    schema_name: str,
    job_id: Optional[str] = None,
    limit: Optional[int] = 100,
    offset: int = 0,
    current_user: User = Depends(get_current_active_user),
):
    """Get lineage results from database table."""
    logger.info(
        "Getting database results",
        database_name=database_name,
        schema_name=schema_name,
        job_id=job_id,
        limit=limit,
        offset=offset,
        user_id=current_user.id
    )
    
    try:
        table_name = "VIEW_TO_SOURCE_COLUMN_LINEAGE"
        full_table_name = f"{database_name}.{schema_name}.{table_name}"
        
        # Build query with optional job_id filter
        where_clause = ""
        if job_id:
            where_clause = f"WHERE JOB_ID = '{job_id}'"
        
        # Count total records
        count_query = f"SELECT COUNT(*) as total FROM {full_table_name} {where_clause}"
        count_result = lineage_service.db_manager.execute_query(count_query)
        total_records = count_result[0][0] if count_result else 0
        
        # Get paginated data
        query = f"""
        SELECT 
            JOB_ID,
            VIEW_NAME,
            VIEW_COLUMN,
            COLUMN_TYPE,
            SOURCE_TABLE,
            SOURCE_COLUMN,
            EXPRESSION_TYPE,
            ANALYSIS_TIMESTAMP,
            CREATED_AT
        FROM {full_table_name}
        {where_clause}
        ORDER BY CREATED_AT DESC, VIEW_NAME, VIEW_COLUMN
        """
        
        if limit:
            query += f" LIMIT {limit}"
        if offset:
            query += f" OFFSET {offset}"
        
        results = lineage_service.db_manager.execute_query(query)
        
        # Convert results to list of dictionaries
        records = []
        for row in results:
            records.append({
                "job_id": row[0],
                "view_name": row[1],
                "view_column": row[2],
                "column_type": row[3],
                "source_table": row[4],
                "source_column": row[5],
                "expression_type": row[6],
                "analysis_timestamp": row[7].isoformat() if row[7] else None,
                "created_at": row[8].isoformat() if row[8] else None,
            })
        
        return {
            "database_name": database_name,
            "schema_name": schema_name,
            "table_name": table_name,
            "total_records": total_records,
            "records": records,
            "limit": limit,
            "offset": offset
        }
        
    except Exception as e:
        logger.error("Failed to get database results", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve database results: {str(e)}",
        )


@router.get("/public/database-results/{database_name}/{schema_name}")
async def get_database_results_public(
    database_name: str,
    schema_name: str,
    job_id: Optional[str] = None,
    limit: Optional[int] = 100,
    offset: int = 0,
):
    """Get lineage results from database table (public endpoint)."""
    logger.info(
        "Getting database results (public)",
        database_name=database_name,
        schema_name=schema_name,
        job_id=job_id,
        limit=limit,
        offset=offset
    )
    
    try:
        table_name = "VIEW_TO_SOURCE_COLUMN_LINEAGE"
        full_table_name = f"{database_name}.{schema_name}.{table_name}"
        
        # Build query with optional job_id filter
        where_clause = ""
        if job_id:
            where_clause = f"WHERE JOB_ID = '{job_id}'"
        
        # Count total records
        count_query = f"SELECT COUNT(*) as total FROM {full_table_name} {where_clause}"
        count_result = lineage_service.db_manager.execute_query(count_query)
        total_records = count_result[0][0] if count_result else 0
        
        # Get paginated data
        query = f"""
        SELECT 
            JOB_ID,
            VIEW_NAME,
            VIEW_COLUMN,
            COLUMN_TYPE,
            SOURCE_TABLE,
            SOURCE_COLUMN,
            EXPRESSION_TYPE,
            ANALYSIS_TIMESTAMP,
            CREATED_AT
        FROM {full_table_name}
        {where_clause}
        ORDER BY CREATED_AT DESC, VIEW_NAME, VIEW_COLUMN
        """
        
        if limit:
            query += f" LIMIT {limit}"
        if offset:
            query += f" OFFSET {offset}"
        
        results = lineage_service.db_manager.execute_query(query)
        
        # Convert results to list of dictionaries
        records = []
        for row in results:
            records.append({
                "job_id": row[0],
                "view_name": row[1],
                "view_column": row[2],
                "column_type": row[3],
                "source_table": row[4],
                "source_column": row[5],
                "expression_type": row[6],
                "analysis_timestamp": row[7].isoformat() if row[7] else None,
                "created_at": row[8].isoformat() if row[8] else None,
            })
        
        return {
            "database_name": database_name,
            "schema_name": schema_name,
            "table_name": table_name,
            "total_records": total_records,
            "records": records,
            "limit": limit,
            "offset": offset
        }
        
    except Exception as e:
        logger.error("Failed to get database results", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve database results: {str(e)}",
        )


@router.get("/public/saved-results")
async def list_saved_results_public():
    """List all saved CSV result files (public endpoint)."""
    logger.info("Listing saved result files (public)")
    
    try:
        from api.core.config import get_settings
        from pathlib import Path
        
        settings = get_settings()
        results_dir = Path(settings.RESULTS_DIRECTORY)
        
        if not results_dir.exists():
            return {"files": [], "message": "No results directory found"}
        
        # Get all CSV files in the results directory
        csv_files = []
        for file_path in results_dir.glob("lineage_analysis_*.csv"):
            stat = file_path.stat()
            csv_files.append({
                "filename": file_path.name,
                "filepath": str(file_path),
                "size_bytes": stat.st_size,
                "created_at": datetime.fromtimestamp(stat.st_ctime).isoformat(),
                "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            })
        
        # Sort by creation time (newest first)
        csv_files.sort(key=lambda x: x["created_at"], reverse=True)
        
        return {
            "files": csv_files,
            "total_files": len(csv_files),
            "directory": str(results_dir)
        }
        
    except Exception as e:
        logger.error("Failed to list saved results", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list saved results: {str(e)}",
        )