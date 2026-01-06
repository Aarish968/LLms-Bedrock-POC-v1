"""Repository analysis service."""

import asyncio
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
from uuid import UUID

from api.core.logging import get_logger
from api.core.repo_analysis.repository_cloning_service import RepositoryCloningService
from api.v1.models.repository_analysis import (
    AnalysisStatus,
    RepositoryAnalysisJob,
    RepositoryAnalysisRequest,
)

logger = get_logger(__name__)


class RepositoryAnalysisService:
    """Service for managing repository analysis operations."""
    
    def __init__(self):
        # In-memory job storage (in production, use a proper database)
        self._jobs: Dict[UUID, RepositoryAnalysisJob] = {}
        
        # Base directory for cloned repositories
        self.base_clone_dir = Path("Cloned_repo")
        self.frontend_clone_dir = self.base_clone_dir / "Frontend"
        self.backend_clone_dir = self.base_clone_dir / "Backend"
    
    def get_job(self, job_id: UUID) -> Optional[RepositoryAnalysisJob]:
        """Get job by ID."""
        return self._jobs.get(job_id)
    
    def create_job(self, job: RepositoryAnalysisJob) -> None:
        """Store a new job."""
        self._jobs[job.job_id] = job
    
    def update_job(self, job_id: UUID, **updates) -> None:
        """Update job with new data."""
        if job_id in self._jobs:
            job = self._jobs[job_id]
            for key, value in updates.items():
                if hasattr(job, key):
                    setattr(job, key, value)
    
    def list_jobs(self, limit: int = 50, offset: int = 0) -> list[RepositoryAnalysisJob]:
        """List jobs with pagination."""
        # Get jobs sorted by start time (newest first)
        all_jobs = sorted(self._jobs.values(), key=lambda x: x.started_at, reverse=True)
        
        # Apply pagination
        return all_jobs[offset:offset + limit]
    
    async def _clone_repositories(
        self,
        job_id: UUID,
        frontend_repo_name: str,
        backend_repo_name: str,
    ) -> tuple[str, str]:
        """Clone frontend and backend repositories."""
        logger.info("Starting repository cloning", job_id=str(job_id))
        
        # Update job status to cloning
        self.update_job(job_id, status=AnalysisStatus.CLONING, message="Cloning repositories...")
        
        try:
            # Create base directories
            self.frontend_clone_dir.mkdir(parents=True, exist_ok=True)
            self.backend_clone_dir.mkdir(parents=True, exist_ok=True)
            
            # Initialize cloning service
            cloning_service = RepositoryCloningService()
            cloning_service.setup_aws_credentials()
            
            # Clone frontend repository
            logger.info(f"Cloning frontend repository: {frontend_repo_name}")
            frontend_success = cloning_service.clone_repository(
                repo_name=frontend_repo_name,
                target_dir=str(self.frontend_clone_dir),
                region=None  # Use default from config
            )
            
            if not frontend_success:
                raise Exception(f"Failed to clone frontend repository: {frontend_repo_name}")
            
            # Clone backend repository
            logger.info(f"Cloning backend repository: {backend_repo_name}")
            backend_success = cloning_service.clone_repository(
                repo_name=backend_repo_name,
                target_dir=str(self.backend_clone_dir),
                region=None  # Use default from config
            )
            
            if not backend_success:
                raise Exception(f"Failed to clone backend repository: {backend_repo_name}")
            
            # Return paths to cloned repositories
            frontend_path = str(self.frontend_clone_dir / frontend_repo_name)
            backend_path = str(self.backend_clone_dir / backend_repo_name)
            
            logger.info(
                "Repository cloning completed successfully",
                job_id=str(job_id),
                frontend_path=frontend_path,
                backend_path=backend_path
            )
            
            return frontend_path, backend_path
            
        except Exception as e:
            logger.error("Repository cloning failed", job_id=str(job_id), error=str(e))
            raise
    
    async def run_analysis(
        self,
        job_id: UUID,
        request: RepositoryAnalysisRequest,
        user_id: str,
    ) -> None:
        """Run repository analysis in background."""
        logger.info("Starting repository analysis", job_id=str(job_id), user_id=user_id)
        
        try:
            # Step 1: Clone repositories
            frontend_path, backend_path = await self._clone_repositories(
                job_id=job_id,
                frontend_repo_name=request.frontend_repo_name,
                backend_repo_name=request.backend_repo_name,
            )
            
            # Step 2: Update job status to running analysis
            self.update_job(job_id, status=AnalysisStatus.RUNNING, message="Running analysis on cloned repositories...")
            
            # Step 3: Get the path to the main.py script
            repo_analysis_script = Path(__file__).parent.parent.parent / "core" / "repo_analysis" / "main.py"
            
            if not repo_analysis_script.exists():
                raise FileNotFoundError(f"Repository analysis script not found: {repo_analysis_script}")
            
            # Step 4: Generate output filename if not provided
            output_file = request.output_filename
            if not output_file:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = f"repo_analysis_{timestamp}.csv"
            
            # Step 5: Prepare command arguments with cloned repository paths
            cmd_args = [
                "python", str(repo_analysis_script),
                "--frontend", frontend_path,
                "--backend", backend_path,
                "--output", output_file,
            ]
            
            logger.info("Executing repository analysis command", command=" ".join(cmd_args))
            
            # Step 6: Run the analysis script
            process = await asyncio.create_subprocess_exec(
                *cmd_args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=Path.cwd(),
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode == 0:
                # Success
                logger.info("Repository analysis completed successfully", job_id=str(job_id))
                self.update_job(
                    job_id,
                    status=AnalysisStatus.COMPLETED,
                    message="Analysis completed successfully",
                    output_file=output_file,
                    completed_at=datetime.now(),
                )
            else:
                # Error
                error_msg = stderr.decode() if stderr else "Unknown error occurred"
                logger.error("Repository analysis failed", job_id=str(job_id), error=error_msg)
                self.update_job(
                    job_id,
                    status=AnalysisStatus.FAILED,
                    message="Analysis failed",
                    error_message=error_msg,
                    completed_at=datetime.now(),
                )
        
        except Exception as e:
            logger.error("Repository analysis exception", job_id=str(job_id), error=str(e))
            self.update_job(
                job_id,
                status=AnalysisStatus.FAILED,
                message="Analysis failed with exception",
                error_message=str(e),
                completed_at=datetime.now(),
            )
    
    def cancel_job(self, job_id: UUID) -> bool:
        """Cancel a job."""
        job = self.get_job(job_id)
        if not job:
            return False
        
        if job.status not in [AnalysisStatus.PENDING, AnalysisStatus.CLONING, AnalysisStatus.RUNNING]:
            return False
        
        self.update_job(
            job_id,
            status=AnalysisStatus.CANCELLED,
            message="Job cancelled by user",
            completed_at=datetime.now()
        )
        return True
    
    def get_results_info(self, job_id: UUID) -> Optional[dict]:
        """Get information about analysis results."""
        job = self.get_job(job_id)
        if not job or job.status != AnalysisStatus.COMPLETED or not job.output_file:
            return None
        
        # Check if output file exists
        output_path = Path(job.output_file)
        if not output_path.exists():
            return None
        
        # Get file info
        file_stat = output_path.stat()
        
        return {
            "job_id": job_id,
            "status": job.status,
            "output_file": job.output_file,
            "file_size": file_stat.st_size,
            "created_at": datetime.fromtimestamp(file_stat.st_ctime).isoformat(),
            "modified_at": datetime.fromtimestamp(file_stat.st_mtime).isoformat(),
            "message": "Results are ready for download",
        }
    
    def cleanup_cloned_repositories(self, job_id: UUID) -> None:
        """Clean up cloned repositories for a specific job (optional)."""
        try:
            job = self.get_job(job_id)
            if job and job.status in [AnalysisStatus.COMPLETED, AnalysisStatus.FAILED, AnalysisStatus.CANCELLED]:
                # Only cleanup if job is finished
                if self.base_clone_dir.exists():
                    logger.info(f"Cleaning up cloned repositories for job {job_id}")
                    # Note: In production, you might want to keep repos for debugging
                    # or implement a more sophisticated cleanup strategy
        except Exception as e:
            logger.warning(f"Failed to cleanup repositories for job {job_id}: {e}")