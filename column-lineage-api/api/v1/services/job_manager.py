"""Job management service for lineage analysis."""

import json
from datetime import datetime
from typing import Dict, List, Optional, Any
from uuid import UUID

from api.core.logging import LoggerMixin
from api.v1.models.lineage import (
    LineageAnalysisJob,
    ColumnLineageResult,
    JobStatus,
)


class JobManager(LoggerMixin):
    """In-memory job manager for lineage analysis jobs."""
    
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(JobManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            # In-memory storage (in production, use Redis or database)
            self._jobs: Dict[UUID, LineageAnalysisJob] = {}
            self._job_results: Dict[UUID, List[ColumnLineageResult]] = {}
            JobManager._initialized = True
    
    def create_job(self, job: LineageAnalysisJob) -> LineageAnalysisJob:
        """Create a new job."""
        self.logger.info("Creating new job", job_id=str(job.job_id))
        self._jobs[job.job_id] = job
        self.logger.debug("Job stored successfully", job_id=str(job.job_id), total_jobs=len(self._jobs))
        return job
    
    def get_job(self, job_id: UUID) -> Optional[LineageAnalysisJob]:
        """Get job by ID."""
        # Handle both UUID and string inputs
        if isinstance(job_id, str):
            try:
                job_id = UUID(job_id)
            except ValueError:
                self.logger.error("Invalid job ID format", job_id=job_id)
                return None
        return self._jobs.get(job_id)
    
    def update_job_status(
        self,
        job_id: UUID,
        status: str,
        started_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None,
        error_message: Optional[str] = None,
        results_count: Optional[int] = None,
    ) -> None:
        """Update job status."""
        # Handle both UUID and string inputs
        if isinstance(job_id, str):
            try:
                job_id = UUID(job_id)
            except ValueError:
                self.logger.error("Invalid job ID format for status update", job_id=job_id)
                return
                
        job = self._jobs.get(job_id)
        if not job:
            self.logger.error("Job not found for status update", job_id=str(job_id))
            self.logger.debug("Available jobs", job_ids=[str(jid) for jid in self._jobs.keys()])
            return
        
        job.status = JobStatus(status)
        
        if started_at:
            job.started_at = started_at
        if completed_at:
            job.completed_at = completed_at
        if error_message:
            job.error_message = error_message
        if results_count is not None:
            job.results_count = results_count
        
        self.logger.info(
            "Job status updated",
            job_id=str(job_id),
            status=status,
            results_count=results_count,
        )
    
    def update_job_progress(
        self,
        job_id: UUID,
        total_views: Optional[int] = None,
        processed_views: Optional[int] = None,
        successful_views: Optional[int] = None,
        failed_views: Optional[int] = None,
    ) -> None:
        """Update job progress."""
        # Handle both UUID and string inputs
        if isinstance(job_id, str):
            try:
                job_id = UUID(job_id)
            except ValueError:
                self.logger.error("Invalid job ID format for progress update", job_id=job_id)
                return
                
        job = self._jobs.get(job_id)
        if not job:
            self.logger.error("Job not found for progress update", job_id=str(job_id))
            self.logger.debug("Available jobs", job_ids=[str(jid) for jid in self._jobs.keys()])
            return
        
        if total_views is not None:
            job.total_views = total_views
        if processed_views is not None:
            job.processed_views = processed_views
        if successful_views is not None:
            job.successful_views = successful_views
        if failed_views is not None:
            job.failed_views = failed_views
        
        self.logger.debug(
            "Job progress updated",
            job_id=str(job_id),
            processed=processed_views,
            total=total_views,
        )
    
    def store_job_results(
        self, 
        job_id: UUID, 
        results: List[ColumnLineageResult]
    ) -> None:
        """Store job results."""
        # Handle both UUID and string inputs
        if isinstance(job_id, str):
            try:
                job_id = UUID(job_id)
            except ValueError:
                self.logger.error("Invalid job ID format for storing results", job_id=job_id)
                return
                
        self.logger.info(
            "Storing job results",
            job_id=str(job_id),
            results_count=len(results),
        )
        self._job_results[job_id] = results
    
    def get_job_results(
        self,
        job_id: UUID,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> List[ColumnLineageResult]:
        """Get job results with pagination."""
        # Handle both UUID and string inputs
        if isinstance(job_id, str):
            try:
                job_id = UUID(job_id)
            except ValueError:
                self.logger.error("Invalid job ID format for getting results", job_id=job_id)
                return []
                
        results = self._job_results.get(job_id, [])
        
        if limit is not None:
            end_idx = offset + limit
            return results[offset:end_idx]
        
        return results[offset:]
    
    def get_job_summary(self, job_id: UUID) -> Dict[str, Any]:
        """Get job summary statistics."""
        # Handle both UUID and string inputs
        if isinstance(job_id, str):
            try:
                job_id = UUID(job_id)
            except ValueError:
                self.logger.error("Invalid job ID format for getting summary", job_id=job_id)
                return {}
                
        job = self._jobs.get(job_id)
        results = self._job_results.get(job_id, [])
        
        if not job:
            return {}
        
        # Calculate statistics
        column_type_counts = {}
        confidence_stats = []
        source_table_counts = {}
        
        for result in results:
            # Column type distribution
            col_type = result.column_type.value
            column_type_counts[col_type] = column_type_counts.get(col_type, 0) + 1
            
            # Confidence scores
            confidence_stats.append(result.confidence_score)
            
            # Source table distribution
            source_table = result.source_table
            source_table_counts[source_table] = source_table_counts.get(source_table, 0) + 1
        
        # Calculate confidence statistics
        avg_confidence = sum(confidence_stats) / len(confidence_stats) if confidence_stats else 0
        min_confidence = min(confidence_stats) if confidence_stats else 0
        max_confidence = max(confidence_stats) if confidence_stats else 0
        
        return {
            "job_info": {
                "job_id": str(job_id),
                "status": job.status.value,
                "total_views": job.total_views,
                "processed_views": job.processed_views,
                "successful_views": job.successful_views,
                "failed_views": job.failed_views,
                "total_results": len(results),
                "processing_time_seconds": (
                    (job.completed_at - job.started_at).total_seconds()
                    if job.started_at and job.completed_at
                    else None
                ),
            },
            "column_type_distribution": column_type_counts,
            "confidence_statistics": {
                "average": round(avg_confidence, 3),
                "minimum": round(min_confidence, 3),
                "maximum": round(max_confidence, 3),
            },
            "source_table_distribution": dict(
                sorted(source_table_counts.items(), key=lambda x: x[1], reverse=True)[:10]
            ),
            "success_rate": {
                "view_success_rate": (
                    job.successful_views / job.total_views * 100
                    if job.total_views > 0
                    else 0
                ),
                "high_confidence_results": len([
                    r for r in results if r.confidence_score >= 0.8
                ]) / len(results) * 100 if results else 0,
            },
        }
    
    def cancel_job(self, job_id: UUID) -> None:
        """Cancel a job."""
        job = self._jobs.get(job_id)
        if not job:
            self.logger.error("Job not found for cancellation", job_id=str(job_id))
            return
        
        if job.status in [JobStatus.PENDING, JobStatus.RUNNING]:
            job.status = JobStatus.CANCELLED
            job.completed_at = datetime.utcnow()
            self.logger.info("Job cancelled", job_id=str(job_id))
        else:
            self.logger.warning(
                "Cannot cancel job with current status",
                job_id=str(job_id),
                status=job.status.value,
            )
    
    def list_jobs(
        self,
        status_filter: Optional[JobStatus] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[LineageAnalysisJob]:
        """List jobs with optional filtering."""
        jobs = list(self._jobs.values())
        
        # Filter by status if specified
        if status_filter:
            jobs = [job for job in jobs if job.status == status_filter]
        
        # Sort by creation time (newest first)
        jobs.sort(key=lambda x: x.created_at, reverse=True)
        
        # Apply pagination
        return jobs[offset:offset + limit]
    
    def cleanup_old_jobs(self, max_age_hours: int = 24) -> int:
        """Clean up old completed jobs."""
        cutoff_time = datetime.utcnow().timestamp() - (max_age_hours * 3600)
        jobs_to_remove = []
        
        for job_id, job in self._jobs.items():
            if (
                job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]
                and job.completed_at
                and job.completed_at.timestamp() < cutoff_time
            ):
                jobs_to_remove.append(job_id)
        
        # Remove old jobs and their results
        for job_id in jobs_to_remove:
            del self._jobs[job_id]
            self._job_results.pop(job_id, None)
        
        self.logger.info("Cleaned up old jobs", count=len(jobs_to_remove))
        return len(jobs_to_remove)