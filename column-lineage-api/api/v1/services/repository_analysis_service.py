"""Repository analysis service."""

import asyncio
import os
import shutil
import sys
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
        
        # Directory for analysis results
        self.analysis_results_dir = Path("Repo_Analyze")
        self.analysis_results_dir.mkdir(exist_ok=True)
    
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
            logger.error("Repository analysis failed", job_id=str(job_id), error=str(e))
            raise
    
    async def _run_action_to_table_analysis(
        self,
        frontend_path: str,
        backend_path: str,
        output_file: str,
        job_id: UUID
    ) -> bool:
        """Run the main.py analysis script on the cloned repositories."""
        try:
            logger.info(f"Running main.py analysis script", job_id=str(job_id))
            
            # Get the path to the main.py script
            main_script_path = Path(__file__).parent.parent.parent / "core" / "repo_analysis" / "main.py"
            
            if not main_script_path.exists():
                logger.error(f"main.py script not found: {main_script_path}")
                return False
            
            # Check if the cloned repositories exist
            if not Path(frontend_path).exists():
                logger.error(f"Frontend repository path does not exist: {frontend_path}")
                return False
            
            if not Path(backend_path).exists():
                logger.error(f"Backend repository path does not exist: {backend_path}")
                return False
            
            logger.info(f"Verified paths exist - Frontend: {frontend_path}, Backend: {backend_path}")
            
            # Parse the output file path
            output_path = Path(output_file)
            output_base = output_path.stem  # Get filename without extension
            output_dir = output_path.parent
            
            # Prepare command arguments with the actual cloned repository paths
            cmd_args = [
                "python", str(main_script_path),
                "--frontend", str(Path(frontend_path).absolute()),
                "--backend", str(Path(backend_path).absolute()),
                "--output", output_base,  # Pass just the base name to the script
            ]
            
            logger.info(f"Executing main.py command: {' '.join(cmd_args)}")
            logger.info(f"Working directory: {Path.cwd()}")
            
            # Run the main.py script
            process = await asyncio.create_subprocess_exec(
                *cmd_args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=Path.cwd(),
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode == 0:
                # Success - verify the CSV file was created
                # Check multiple possible locations and names for the CSV file
                possible_locations = [
                    Path(output_base),  # Without .csv extension (script creates this)
                    Path(f"{output_base}.csv"),  # With .csv extension
                    Path.cwd() / output_base,  # Explicit current directory without .csv
                    Path.cwd() / f"{output_base}.csv",  # Explicit current directory with .csv
                    main_script_path.parent / output_base,  # Script directory without .csv
                    main_script_path.parent / f"{output_base}.csv",  # Script directory with .csv
                ]
                
                csv_path = None
                for location in possible_locations:
                    if location.exists():
                        csv_path = location
                        logger.info(f"Found output file at: {csv_path}")
                        break
                
                if csv_path:
                    logger.info(f"Successfully found analysis output at: {csv_path}")
                    
                    # Move the file to the target directory with .csv extension
                    final_path = output_path
                    
                    try:
                        import shutil
                        # Ensure the target directory exists
                        final_path.parent.mkdir(parents=True, exist_ok=True)
                        
                        # Copy/move the file to the final location
                        shutil.copy2(str(csv_path), str(final_path))
                        logger.info(f"✅ Successfully moved output file to: {final_path}")
                        
                        # Clean up the original file if it's different from the final path
                        if csv_path.resolve() != final_path.resolve():
                            try:
                                csv_path.unlink()
                                logger.info(f"Cleaned up temporary file: {csv_path}")
                            except Exception as e:
                                logger.warning(f"Could not clean up temporary file: {e}")
                        
                        # Verify the final file exists and has content
                        if final_path.exists():
                            file_size = final_path.stat().st_size
                            logger.info(f"Final output file size: {file_size} bytes")
                            
                            # Verify it's a valid CSV by checking the first line
                            try:
                                with open(final_path, 'r', encoding='utf-8') as f:
                                    first_line = f.readline().strip()
                                    if 'Frontend_File' in first_line or 'Frontend File' in first_line:
                                        logger.info("✅ Valid CSV header detected")
                                    else:
                                        logger.warning(f"Unexpected CSV header: {first_line[:100]}")
                            except Exception as e:
                                logger.warning(f"Could not verify CSV content: {e}")
                        else:
                            logger.error(f"Final output file was not created: {final_path}")
                            return False
                            
                    except Exception as e:
                        logger.error(f"Failed to move output file: {e}")
                        return False
                    
                    # Log stdout for debugging
                    if stdout:
                        logger.info(f"Script output: {stdout.decode()}")
                    
                    return True
                else:
                    logger.error(f"Output file was not found at any expected location:")
                    for i, location in enumerate(possible_locations):
                        logger.error(f"  {i+1}. {location.absolute()} (exists: {location.exists()})")
                    
                    # List all files in current directory for debugging
                    try:
                        current_files = list(Path.cwd().glob("*"))
                        csv_files = [f for f in current_files if f.suffix.lower() == '.csv']
                        other_files = [f for f in current_files if f.name.startswith(output_base)]
                        
                        logger.info(f"CSV files in current directory: {[f.name for f in csv_files]}")
                        logger.info(f"Files starting with '{output_base}': {[f.name for f in other_files]}")
                    except Exception as e:
                        logger.error(f"Error listing files: {e}")
                    
                    # Log stdout and stderr for debugging
                    if stdout:
                        logger.info(f"Script stdout: {stdout.decode()}")
                    if stderr:
                        logger.error(f"Script stderr: {stderr.decode()}")
                    return False
            else:
                # Error
                error_msg = stderr.decode() if stderr else "Unknown error occurred"
                logger.error(f"main.py script failed with return code {process.returncode}: {error_msg}")
                
                # Also log stdout in case there's useful info
                if stdout:
                    logger.info(f"Script stdout: {stdout.decode()}")
                
                return False
                
        except Exception as e:
            logger.error(f"Error running main.py analysis script: {e}", job_id=str(job_id))
            return False
    
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
            
            # Step 3: Run the analysis using action_to_table.py
            self.update_job(job_id, status=AnalysisStatus.RUNNING, message="Running analysis on cloned repositories...")
            
            # Auto-generate output filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"repo_analysis_{timestamp}.csv"
            output_path = self.analysis_results_dir / output_filename
            
            logger.info(f"Auto-generated output filename: {output_filename}")
            logger.info(f"Output will be saved to: {output_path}")
            
            # Run the analysis using main.py script
            success = await self._run_action_to_table_analysis(
                frontend_path=frontend_path,
                backend_path=backend_path,
                output_file=str(output_path),
                job_id=job_id
            )
            
            if success:
                # Success
                logger.info("Repository analysis completed successfully", job_id=str(job_id))
                self.update_job(
                    job_id,
                    status=AnalysisStatus.COMPLETED,
                    message="Analysis completed successfully",
                    output_file=str(output_path),
                    completed_at=datetime.now(),
                )
            else:
                # Error
                logger.error("Repository analysis failed", job_id=str(job_id))
                self.update_job(
                    job_id,
                    status=AnalysisStatus.FAILED,
                    message="Analysis failed",
                    error_message="Analysis script execution failed",
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