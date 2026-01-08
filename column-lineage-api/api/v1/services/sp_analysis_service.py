"""Stored procedure analysis service."""

import os
import asyncio
from typing import List, Optional, Dict
from uuid import UUID
from datetime import datetime

from api.core.logging import get_logger
from api.v1.models.sp_analysis import (
    SPAnalysisJob,
    SPJobStatus,
    StoredProcedureAnalysis,
    SPAnalysisRequest,
    SingleProcedureRequest,
    SPResultsResponse,
    ProcedureInfo,
)

logger = get_logger(__name__)


class SPAnalysisService:
    """Service for managing stored procedure analysis operations."""
    
    def __init__(self):
        """Initialize the service."""
        self.jobs: Dict[UUID, SPAnalysisJob] = {}
        self.results_dir = "sp_analysis_results"
        os.makedirs(self.results_dir, exist_ok=True)
    
    def create_job(self, request: SPAnalysisRequest) -> SPAnalysisJob:
        """Create a new analysis job."""
        job = SPAnalysisJob(
            sf_environment=request.sf_environment,
            max_workers=request.max_workers,
            request_params=request.model_dump(),
        )
        
        # Generate output filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        job.result_file = os.path.join(
            self.results_dir, 
            f"sp_analysis_{job.job_id}_{timestamp}.csv"
        )
        
        self.jobs[job.job_id] = job
        logger.info(f"Created SP analysis job {job.job_id}")
        return job
    
    def get_job(self, job_id: UUID) -> Optional[SPAnalysisJob]:
        """Get job by ID."""
        return self.jobs.get(job_id)
    
    def update_job_status(self, job_id: UUID, status: SPJobStatus, **kwargs):
        """Update job status and other fields."""
        if job_id in self.jobs:
            job = self.jobs[job_id]
            job.status = status
            
            for key, value in kwargs.items():
                if hasattr(job, key):
                    setattr(job, key, value)
            
            if status in [SPJobStatus.COMPLETED, SPJobStatus.FAILED, SPJobStatus.CANCELLED]:
                job.completed_at = datetime.now()
            
            logger.info(f"Updated job {job_id} status to {status}")
    
    def list_jobs(self, limit: int = 50, offset: int = 0) -> List[SPAnalysisJob]:
        """List all jobs with pagination."""
        all_jobs = sorted(
            self.jobs.values(),
            key=lambda x: x.started_at,
            reverse=True
        )
        return all_jobs[offset:offset + limit]
    
    def delete_job(self, job_id: UUID) -> bool:
        """Delete a job and its result file."""
        if job_id not in self.jobs:
            return False
        
        job = self.jobs[job_id]
        
        # Delete result file if exists
        if job.result_file and os.path.exists(job.result_file):
            try:
                os.remove(job.result_file)
                logger.info(f"Deleted result file: {job.result_file}")
            except Exception as e:
                logger.warning(f"Failed to delete result file: {e}")
        
        # Remove from jobs
        del self.jobs[job_id]
        logger.info(f"Deleted job {job_id}")
        return True
    
    async def process_analysis(self, job_id: UUID, request: SPAnalysisRequest, user_id: Optional[str] = None):
        """Process stored procedure analysis in background."""
        try:
            # Import here to avoid circular imports
            from api.core.sp_analysis.sp_analyzer import (
                analyze_all_procedures,
                fetch_stored_procedures
            )
            
            job = self.get_job(job_id)
            if not job:
                logger.error(f"Job {job_id} not found")
                return
            
            self.update_job_status(job_id, SPJobStatus.RUNNING)
            logger.info(f"Starting SP analysis for job {job_id}")
            
            # Fetch procedures first to get count
            procedures = fetch_stored_procedures(request.sf_environment)
            
            # Filter procedures if specific ones requested
            if request.procedure_names:
                procedures = [
                    p for p in procedures 
                    if p['procedure_name'].upper() in [name.upper() for name in request.procedure_names]
                ]
            
            self.update_job_status(
                job_id, 
                SPJobStatus.RUNNING,
                total_procedures=len(procedures)
            )
            
            # Run analysis in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                analyze_all_procedures,
                request.sf_environment,
                request.max_workers,
                job.result_file,
                request.resume_from_partial
            )
            
            # Update job as completed
            self.update_job_status(job_id, SPJobStatus.COMPLETED)
            logger.info(f"Completed SP analysis for job {job_id}")
            
        except Exception as e:
            logger.error(f"SP analysis failed for job {job_id}: {e}")
            self.update_job_status(
                job_id, 
                SPJobStatus.FAILED,
                error_message=str(e)
            )
    
    async def analyze_single_procedure(self, request: SingleProcedureRequest) -> Optional[StoredProcedureAnalysis]:
        """Analyze a single stored procedure."""
        try:
            # Import here to avoid circular imports
            from api.core.sp_analysis.sp_analyzer import analyze_stored_procedure
            
            logger.info(f"Analyzing single procedure: {request.procedure_name}")
            
            # Run in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                analyze_stored_procedure,
                request.procedure_definition,
                request.procedure_name,
                request.procedure_schema
            )
            
            logger.info(f"Single procedure analysis completed: {request.procedure_name}")
            return result
            
        except Exception as e:
            logger.error(f"Single procedure analysis failed: {e}")
            return None
    
    async def get_procedures_list(self, sf_environment: str) -> List[ProcedureInfo]:
        """Get list of stored procedures from Snowflake."""
        try:
            # Import here to avoid circular imports
            from api.core.sp_analysis.sp_analyzer import fetch_stored_procedures
            
            logger.info(f"Fetching procedures list for environment: {sf_environment}")
            
            # Run in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            procedures = await loop.run_in_executor(
                None,
                fetch_stored_procedures,
                sf_environment
            )
            
            return [
                ProcedureInfo(
                    name=p["procedure_name"],
                    procedure_schema=p["procedure_schema"],
                    definition_length=len(p["procedure_definition"])
                )
                for p in procedures
            ]
            
        except Exception as e:
            logger.error(f"Failed to fetch procedures list: {e}")
            return []
    
    def get_results(self, job_id: UUID) -> Optional[SPResultsResponse]:
        """Get analysis results for a job."""
        job = self.get_job(job_id)
        if not job:
            return None
        
        # Calculate summary statistics if job is completed
        summary = {}
        total_relationships = 0
        unique_tables = 0
        
        if job.status == SPJobStatus.COMPLETED and job.result_file and os.path.exists(job.result_file):
            try:
                import csv
                with open(job.result_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
                    total_relationships = len(rows)
                    unique_tables = len(set(row.get('TABLE_NAME', '') for row in rows))
                    
                    # Count relationship types
                    rel_types = {}
                    for row in rows:
                        types = row.get('RELATIONSHIP_TYPES', '').split(',')
                        for rel_type in types:
                            rel_type = rel_type.strip()
                            if rel_type:
                                rel_types[rel_type] = rel_types.get(rel_type, 0) + 1
                    
                    summary = {
                        "relationship_types": rel_types,
                        "execution_time": (job.completed_at - job.started_at).total_seconds() if job.completed_at else 0
                    }
                    
            except Exception as e:
                logger.warning(f"Failed to calculate summary for job {job_id}: {e}")
        
        download_url = f"/api/v1/sp-analysis/results/{job_id}/download" if job.status == SPJobStatus.COMPLETED else None
        
        return SPResultsResponse(
            job_id=job.job_id,
            status=job.status,
            total_procedures=job.total_procedures,
            total_relationships=total_relationships,
            unique_tables=unique_tables,
            result_file=job.result_file,
            download_url=download_url,
            summary=summary
        )