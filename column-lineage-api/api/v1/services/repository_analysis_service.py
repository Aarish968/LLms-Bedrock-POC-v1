"""Repository analysis service."""

import asyncio
import io
import json
import csv
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any, AsyncGenerator
from uuid import UUID

import pandas as pd

from api.core.logging import LoggerMixin
from api.v1.models.repository_analysis import (
    RepositoryAnalysisRequest,
    RepositoryAnalysisJob,
    RepositoryAnalysisResults,
    RepositoryAnalysisProgress,
    RepositoryAnalysisStatus,
    AnalysisType,
    RepositoryInfo,
    RepositoryType,
    ApiEndpointMapping,
    RepositoryDiscoveryResult,
    ErrorDetail,
)
from api.v1.services.job_manager import JobManager
from api.v1.services.repository_cloning_service import RepositoryCloningService, RepositoryDiscoveryService


class RepositoryAnalysisService(LoggerMixin):
    """Repository analysis service."""
    
    def __init__(self):
        self.job_manager = JobManager()
        self._active_jobs: Dict[UUID, RepositoryAnalysisJob] = {}
        self._job_results: Dict[UUID, RepositoryAnalysisResults] = {}
        self.cloning_service = RepositoryCloningService()
        self.discovery_service = RepositoryDiscoveryService(self.cloning_service)
    
    async def start_repository_analysis(
        self,
        request: RepositoryAnalysisRequest,
        user_id: str,
    ) -> RepositoryAnalysisJob:
        """Start a simplified repository analysis job."""
        self.logger.info("Starting simplified repository analysis")
        
        # Create job with simplified parameters
        job = RepositoryAnalysisJob(
            analysis_type=AnalysisType.FRONTEND_BACKEND,
            request_params=request.model_dump(),
        )
        
        # Store job
        self._active_jobs[job.job_id] = job
        
        # Start processing asynchronously
        asyncio.create_task(self._process_simplified_analysis(job, request, user_id))
        
        return job
    
    async def get_job_status(self, job_id: UUID) -> Optional[RepositoryAnalysisJob]:
        """Get job status."""
        return self._active_jobs.get(job_id)
    
    async def get_job_progress(self, job_id: UUID) -> Optional[RepositoryAnalysisProgress]:
        """Get detailed job progress."""
        job = self._active_jobs.get(job_id)
        if not job:
            return None
        
        # Calculate progress percentage
        total_steps = 4  # Discovery, Cloning, Analysis, Results
        completed_steps = 0
        
        if job.discovered_repositories > 0:
            completed_steps += 1
        if job.cloned_repositories > 0:
            completed_steps += 1
        if job.analyzed_repositories > 0:
            completed_steps += 1
        if job.status == RepositoryAnalysisStatus.COMPLETED:
            completed_steps = total_steps
        
        progress_percentage = (completed_steps / total_steps) * 100
        
        # Calculate elapsed time
        elapsed_seconds = 0.0
        if job.started_at:
            end_time = job.completed_at or datetime.utcnow()
            elapsed_seconds = (end_time - job.started_at).total_seconds()
        
        # Estimate remaining time
        estimated_remaining = None
        if progress_percentage > 0 and job.status not in [RepositoryAnalysisStatus.COMPLETED, RepositoryAnalysisStatus.FAILED]:
            estimated_total = elapsed_seconds / (progress_percentage / 100)
            estimated_remaining = max(0, estimated_total - elapsed_seconds)
        
        return RepositoryAnalysisProgress(
            job_id=job.job_id,
            status=job.status,
            progress_percentage=progress_percentage,
            current_step=self._get_current_step_description(job.status),
            total_repositories=job.total_repositories,
            discovered_repositories=job.discovered_repositories,
            cloned_repositories=job.cloned_repositories,
            analyzed_repositories=job.analyzed_repositories,
            failed_repositories=job.failed_repositories,
            elapsed_time_seconds=elapsed_seconds,
            estimated_remaining_seconds=estimated_remaining,
            recent_logs=[]  # Could be enhanced to store recent log messages
        )
    
    async def get_job_results(self, job_id: UUID) -> Optional[RepositoryAnalysisResults]:
        """Get job results."""
        return self._job_results.get(job_id)
    
    async def cancel_job(self, job_id: UUID) -> bool:
        """Cancel a running job."""
        job = self._active_jobs.get(job_id)
        if not job:
            return False
        
        if job.status in [RepositoryAnalysisStatus.COMPLETED, RepositoryAnalysisStatus.FAILED]:
            return False
        
        job.status = RepositoryAnalysisStatus.CANCELLED
        job.completed_at = datetime.utcnow()
        
        self.logger.info("Job cancelled", job_id=str(job_id))
        return True
    
    async def discover_repositories(
        self,
        credentials_file: str = "credentials.txt"
    ) -> RepositoryDiscoveryResult:
        """Discover repositories from CodeCommit."""
        self.logger.info("Starting repository discovery")
        start_time = datetime.utcnow()
        
        try:
            # Update cloning service credentials file
            self.cloning_service.credentials_file = credentials_file
            
            # Discover repositories
            discovered = self.discovery_service.discover_repositories()
            
            # Categorize repositories
            frontend_repos = discovered.get('frontend', [])
            backend_repos = discovered.get('backend', [])
            
            # For now, we don't have fullstack detection, so everything goes to unknown
            fullstack_repos = []
            unknown_repos = []
            
            total_repos = len(frontend_repos) + len(backend_repos) + len(fullstack_repos) + len(unknown_repos)
            
            discovery_time = (datetime.utcnow() - start_time).total_seconds()
            
            result = RepositoryDiscoveryResult(
                total_repositories=total_repos,
                frontend_repositories=frontend_repos,
                backend_repositories=backend_repos,
                fullstack_repositories=fullstack_repos,
                unknown_repositories=unknown_repos,
                discovery_time_seconds=discovery_time
            )
            
            self.logger.info(
                "Repository discovery completed",
                total_repositories=total_repos,
                frontend_count=len(frontend_repos),
                backend_count=len(backend_repos),
                discovery_time=discovery_time
            )
            
            return result
            
        except Exception as e:
            self.logger.error("Repository discovery failed", error=str(e))
            raise
    
    async def export_results(
        self,
        job_id: UUID,
        format: str,
        include_metadata: bool = True,
        include_repository_info: bool = True,
        filter_by_confidence: Optional[float] = None,
    ) -> AsyncGenerator[bytes, None]:
        """Export repository analysis results in specified format."""
        results = self._job_results.get(job_id)
        if not results:
            raise ValueError(f"No results found for job {job_id}")
        
        self.logger.info(
            "Exporting repository analysis results",
            job_id=str(job_id),
            format=format,
            mappings_count=len(results.api_mappings)
        )
        
        # Filter by confidence if specified
        api_mappings = results.api_mappings
        if filter_by_confidence is not None:
            api_mappings = [
                mapping for mapping in api_mappings
                if mapping.confidence_score >= filter_by_confidence
            ]
        
        if format.lower() == "csv":
            yield await self._export_csv(
                results, api_mappings, include_metadata, include_repository_info
            )
        elif format.lower() == "json":
            yield await self._export_json(
                results, api_mappings, include_metadata, include_repository_info
            )
        elif format.lower() == "excel":
            yield await self._export_excel(
                results, api_mappings, include_metadata, include_repository_info
            )
        else:
            raise ValueError(f"Unsupported export format: {format}")
    
    async def _process_simplified_analysis(
        self,
        job: RepositoryAnalysisJob,
        request: RepositoryAnalysisRequest,
        user_id: str,
    ) -> None:
        """Process simplified repository analysis job."""
        try:
            job.status = RepositoryAnalysisStatus.DISCOVERING
            job.started_at = datetime.utcnow()
            
            self.logger.info("Starting simplified repository analysis", job_id=str(job.job_id))
            
            # Step 1: Validate and normalize paths
            frontend_path = request.frontend_path
            backend_path = request.backend_path
            
            # Check for placeholder values and provide defaults
            if frontend_path == "string" or not frontend_path or frontend_path.strip() == "":
                frontend_path = "cloned-repo/frontend/guided-workflow"
                self.logger.info("Using default frontend path", path=frontend_path)
            
            if backend_path == "string" or not backend_path or backend_path.strip() == "":
                backend_path = "cloned-repo/backend/guided-workflow-backend"
                self.logger.info("Using default backend path", path=backend_path)
            
            # Convert to absolute paths
            frontend_path = str(Path(frontend_path).resolve())
            backend_path = str(Path(backend_path).resolve())
            
            frontend_exists = Path(frontend_path).exists()
            backend_exists = Path(backend_path).exists()
            
            self.logger.info(
                "Path check results",
                frontend_path=frontend_path,
                frontend_exists=frontend_exists,
                backend_path=backend_path,
                backend_exists=backend_exists
            )
            
            # Step 2: Handle missing repositories
            if not frontend_exists or not backend_exists:
                job.status = RepositoryAnalysisStatus.CLONING
                self.logger.info("Some paths missing, checking cloned-repo directory")
                
                # Check if we have repositories in cloned-repo directory
                cloned_repo_dir = Path("cloned-repo")
                if cloned_repo_dir.exists():
                    # Look for specific frontend and backend directories
                    potential_frontend = cloned_repo_dir / "frontend" / "guided-workflow"
                    potential_backend = cloned_repo_dir / "backend" / "guided-workflow-backend"
                    
                    if not frontend_exists and potential_frontend.exists():
                        frontend_path = str(potential_frontend.resolve())
                        frontend_exists = True
                        self.logger.info("Found existing frontend in cloned-repo", path=frontend_path)
                    
                    if not backend_exists and potential_backend.exists():
                        backend_path = str(potential_backend.resolve())
                        backend_exists = True
                        self.logger.info("Found existing backend in cloned-repo", path=backend_path)
                
                # If still missing, try to clone from CodeCommit
                if not frontend_exists or not backend_exists:
                    self.logger.info("Attempting to clone missing repositories from CodeCommit")
                    
                    # Update cloning service credentials
                    self.cloning_service.credentials_file = request.credentials_file
                    
                    # Try to discover and clone repositories
                    try:
                        discovered = self.discovery_service.discover_repositories()
                        
                        if not frontend_exists and discovered.get('frontend'):
                            # Try to clone the first frontend repository
                            frontend_repo = discovered['frontend'][0]
                            self.logger.info(f"Attempting to clone frontend repository: {frontend_repo}")
                            clone_success = self.cloning_service.clone_repository(
                                frontend_repo, 
                                "cloned-repo"
                            )
                            if clone_success:
                                # Update frontend path to the cloned repository
                                frontend_path = str(Path("cloned-repo") / frontend_repo)
                                frontend_exists = Path(frontend_path).exists()
                        
                        if not backend_exists and discovered.get('backend'):
                            # Try to clone the first backend repository
                            backend_repo = discovered['backend'][0]
                            self.logger.info(f"Attempting to clone backend repository: {backend_repo}")
                            clone_success = self.cloning_service.clone_repository(
                                backend_repo, 
                                "cloned-repo"
                            )
                            if clone_success:
                                # Update backend path to the cloned repository
                                backend_path = str(Path("cloned-repo") / backend_repo)
                                backend_exists = Path(backend_path).exists()
                                
                    except Exception as e:
                        self.logger.warning("Failed to discover/clone repositories", error=str(e))
                        # Continue with existing paths if any
            
            # Step 3: Verify paths exist after processing
            frontend_exists = Path(frontend_path).exists()
            backend_exists = Path(backend_path).exists()
            
            if not frontend_exists:
                self.logger.warning(f"Frontend path still does not exist: {frontend_path}")
                # Try to find any frontend-like directory in cloned-repo
                cloned_repo_dir = Path("cloned-repo")
                if cloned_repo_dir.exists():
                    for item in cloned_repo_dir.iterdir():
                        if item.is_dir() and any(pattern in item.name.lower() for pattern in ['frontend', 'ui', 'web', 'client']):
                            frontend_path = str(item.resolve())
                            frontend_exists = True
                            self.logger.info(f"Found alternative frontend path: {frontend_path}")
                            break
            
            if not backend_exists:
                self.logger.warning(f"Backend path still does not exist: {backend_path}")
                # Try to find any backend-like directory in cloned-repo
                cloned_repo_dir = Path("cloned-repo")
                if cloned_repo_dir.exists():
                    for item in cloned_repo_dir.iterdir():
                        if item.is_dir() and any(pattern in item.name.lower() for pattern in ['backend', 'api', 'server']):
                            backend_path = str(item.resolve())
                            backend_exists = True
                            self.logger.info(f"Found alternative backend path: {backend_path}")
                            break
            
            # Final check - if still no paths, create mock analysis
            if not frontend_exists and not backend_exists:
                self.logger.warning("No valid frontend or backend paths found, creating mock analysis")
                # Create a mock analysis result
                job.status = RepositoryAnalysisStatus.COMPLETED
                job.completed_at = datetime.utcnow()
                job.analyzed_repositories = 0
                job.api_mappings_count = 0
                
                results = RepositoryAnalysisResults(
                    job_id=job.job_id,
                    status=RepositoryAnalysisStatus.COMPLETED,
                    analysis_type=AnalysisType.FRONTEND_BACKEND,
                    repositories=[],
                    api_mappings=[],
                    summary={
                        "total_repositories": 0,
                        "total_api_mappings": 0,
                        "frontend_path": frontend_path,
                        "backend_path": backend_path,
                        "execution_time_seconds": 0,
                        "error": "No valid repositories found for analysis"
                    },
                    output_files=[],
                    execution_time_seconds=0,
                    created_at=datetime.utcnow()
                )
                
                self._job_results[job.job_id] = results
                return
            
            job.cloned_repositories = 2 if frontend_exists and backend_exists else (1 if frontend_exists or backend_exists else 0)
            job.total_repositories = 2
            job.discovered_repositories = job.cloned_repositories
            
            # Step 4: Run analysis
            job.status = RepositoryAnalysisStatus.ANALYZING
            self.logger.info("Running repository analysis")
            
            # Import analysis functionality
            from main import EnhancedCSVGenerator
            
            # Generate output filename
            output_file = f"repository_analysis_{str(job.job_id)[:8]}.{request.output_format}"
            
            # Run the analysis
            generator = EnhancedCSVGenerator(frontend_path, backend_path)
            generator.generate_enhanced_csv(output_file)
            
            # Step 5: Process results
            self.logger.info("Processing analysis results")
            
            # Parse the generated file
            api_mappings = await self._parse_analysis_file(output_file, request.output_format)
            
            # Create repository info
            repositories = [
                RepositoryInfo(
                    repository_name=Path(frontend_path).name,
                    repository_type=RepositoryType.FRONTEND,
                    local_path=frontend_path
                ),
                RepositoryInfo(
                    repository_name=Path(backend_path).name,
                    repository_type=RepositoryType.BACKEND,
                    local_path=backend_path
                )
            ]
            
            job.analyzed_repositories = job.cloned_repositories
            job.api_mappings_count = len(api_mappings)
            
            # Create results
            execution_time = (datetime.utcnow() - job.started_at).total_seconds()
            
            results = RepositoryAnalysisResults(
                job_id=job.job_id,
                status=RepositoryAnalysisStatus.COMPLETED,
                analysis_type=AnalysisType.FRONTEND_BACKEND,
                repositories=repositories,
                api_mappings=api_mappings,
                summary={
                    "total_repositories": job.cloned_repositories,
                    "total_api_mappings": len(api_mappings),
                    "frontend_path": frontend_path,
                    "backend_path": backend_path,
                    "execution_time_seconds": execution_time,
                    "include_database_analysis": request.include_database_analysis
                },
                output_files=[output_file],
                execution_time_seconds=execution_time,
                created_at=datetime.utcnow()
            )
            
            # Store results
            self._job_results[job.job_id] = results
            job.output_files = [output_file]
            
            # Complete job
            job.status = RepositoryAnalysisStatus.COMPLETED
            job.completed_at = datetime.utcnow()
            
            self.logger.info(
                "Simplified repository analysis completed successfully",
                job_id=str(job.job_id),
                execution_time=execution_time,
                api_mappings=len(api_mappings),
                output_file=output_file
            )
            
        except Exception as e:
            self.logger.error("Simplified repository analysis failed", job_id=str(job.job_id), error=str(e))
            job.status = RepositoryAnalysisStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
    
    def _extract_repo_name_from_path(self, path: str) -> str:
        """Extract repository name from a path."""
        # For paths like "D:\LLms-Bedrock-POC-v1\column-lineage-api\cloned-repo\backend\guided-workflow-backend"
        # Extract "guided-workflow-backend"
        return Path(path).name
    
    async def _parse_analysis_file(self, file_path: str, format: str) -> List[ApiEndpointMapping]:
        """Parse the generated analysis file."""
        if format.lower() == "csv":
            return await self._parse_analysis_csv(file_path)
        elif format.lower() == "json":
            return await self._parse_analysis_json(file_path)
        else:
            # Default to CSV parsing
            return await self._parse_analysis_csv(file_path)
    
    async def _parse_analysis_json(self, json_file: str) -> List[ApiEndpointMapping]:
        """Parse the generated analysis JSON file."""
        api_mappings = []
        
        try:
            if not os.path.exists(json_file):
                self.logger.warning("JSON file not found", file=json_file)
                return api_mappings
            
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Assuming the JSON has an array of mappings
            mappings_data = data if isinstance(data, list) else data.get('mappings', [])
            
            for item in mappings_data:
                mapping = ApiEndpointMapping(
                    frontend_call=item.get('frontend_call', ''),
                    backend_endpoint=item.get('backend_endpoint', ''),
                    http_method=item.get('http_method', 'GET'),
                    database_tables=item.get('database_tables', []),
                    database_columns=item.get('database_columns', []),
                    confidence_score=float(item.get('confidence_score', 1.0)),
                    metadata={
                        "json_data": item,
                        "source_file": json_file
                    }
                )
                api_mappings.append(mapping)
            
            self.logger.info(f"Parsed {len(api_mappings)} API mappings from JSON")
            
        except Exception as e:
            self.logger.error("Failed to parse analysis JSON", file=json_file, error=str(e))
        
        return api_mappings
    
    async def _parse_analysis_csv(self, csv_file: str) -> List[ApiEndpointMapping]:
        """Parse the generated analysis CSV file."""
        api_mappings = []
        
        try:
            if not os.path.exists(csv_file):
                self.logger.warning("CSV file not found", file=csv_file)
                return api_mappings
            
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    # Map CSV columns to API endpoint mapping
                    mapping = ApiEndpointMapping(
                        frontend_call=row.get('Frontend_Call', ''),
                        backend_endpoint=row.get('Backend_Endpoint', ''),
                        http_method=row.get('HTTP_Method', 'GET'),
                        database_tables=row.get('Database_Tables', '').split(',') if row.get('Database_Tables') else [],
                        database_columns=row.get('Database_Columns', '').split(',') if row.get('Database_Columns') else [],
                        confidence_score=float(row.get('Confidence_Score', 1.0)),
                        metadata={
                            "csv_row": row,
                            "source_file": csv_file
                        }
                    )
                    api_mappings.append(mapping)
            
            self.logger.info(f"Parsed {len(api_mappings)} API mappings from CSV")
            
        except Exception as e:
            self.logger.error("Failed to parse analysis CSV", file=csv_file, error=str(e))
        
        return api_mappings
    
    def _get_current_step_description(self, status: RepositoryAnalysisStatus) -> str:
        """Get human-readable description of current step."""
        step_descriptions = {
            RepositoryAnalysisStatus.PENDING: "Waiting to start",
            RepositoryAnalysisStatus.DISCOVERING: "Discovering repositories from CodeCommit",
            RepositoryAnalysisStatus.CLONING: "Cloning repositories locally",
            RepositoryAnalysisStatus.ANALYZING: "Analyzing frontend-backend mappings",
            RepositoryAnalysisStatus.COMPLETED: "Analysis completed successfully",
            RepositoryAnalysisStatus.FAILED: "Analysis failed",
            RepositoryAnalysisStatus.CANCELLED: "Analysis cancelled"
        }
        return step_descriptions.get(status, "Unknown step")
    
    async def _export_csv(
        self,
        results: RepositoryAnalysisResults,
        api_mappings: List[ApiEndpointMapping],
        include_metadata: bool,
        include_repository_info: bool,
    ) -> bytes:
        """Export results as CSV."""
        output = io.StringIO()
        
        # API Mappings section
        fieldnames = [
            "Frontend_Call", "Backend_Endpoint", "HTTP_Method",
            "Database_Tables", "Database_Columns", "Confidence_Score"
        ]
        
        if include_metadata:
            fieldnames.append("Metadata")
        
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        
        for mapping in api_mappings:
            row = {
                "Frontend_Call": mapping.frontend_call,
                "Backend_Endpoint": mapping.backend_endpoint,
                "HTTP_Method": mapping.http_method,
                "Database_Tables": ",".join(mapping.database_tables),
                "Database_Columns": ",".join(mapping.database_columns),
                "Confidence_Score": mapping.confidence_score,
            }
            
            if include_metadata:
                row["Metadata"] = json.dumps(mapping.metadata)
            
            writer.writerow(row)
        
        # Repository info section
        if include_repository_info and results.repositories:
            output.write("\n\n# Repository Information\n")
            repo_fieldnames = ["Repository_Name", "Type", "Local_Path", "Language", "Framework"]
            repo_writer = csv.DictWriter(output, fieldnames=repo_fieldnames)
            repo_writer.writeheader()
            
            for repo in results.repositories:
                repo_writer.writerow({
                    "Repository_Name": repo.repository_name,
                    "Type": repo.repository_type.value,
                    "Local_Path": repo.local_path or "",
                    "Language": repo.language or "",
                    "Framework": repo.framework or "",
                })
        
        return output.getvalue().encode("utf-8")
    
    async def _export_json(
        self,
        results: RepositoryAnalysisResults,
        api_mappings: List[ApiEndpointMapping],
        include_metadata: bool,
        include_repository_info: bool,
    ) -> bytes:
        """Export results as JSON."""
        data = {
            "job_id": str(results.job_id),
            "analysis_type": results.analysis_type.value,
            "status": results.status.value,
            "api_mappings": [],
            "summary": results.summary,
            "execution_time_seconds": results.execution_time_seconds,
            "created_at": results.created_at.isoformat(),
        }
        
        # Add API mappings
        for mapping in api_mappings:
            mapping_data = mapping.model_dump()
            if not include_metadata:
                mapping_data.pop("metadata", None)
            data["api_mappings"].append(mapping_data)
        
        # Add repository info
        if include_repository_info:
            data["repositories"] = [repo.model_dump() for repo in results.repositories]
        
        return json.dumps(data, indent=2, default=str).encode("utf-8")
    
    async def _export_excel(
        self,
        results: RepositoryAnalysisResults,
        api_mappings: List[ApiEndpointMapping],
        include_metadata: bool,
        include_repository_info: bool,
    ) -> bytes:
        """Export results as Excel."""
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            # API Mappings sheet
            mappings_data = []
            for mapping in api_mappings:
                row = {
                    "Frontend_Call": mapping.frontend_call,
                    "Backend_Endpoint": mapping.backend_endpoint,
                    "HTTP_Method": mapping.http_method,
                    "Database_Tables": ",".join(mapping.database_tables),
                    "Database_Columns": ",".join(mapping.database_columns),
                    "Confidence_Score": mapping.confidence_score,
                }
                
                if include_metadata:
                    row["Metadata"] = json.dumps(mapping.metadata)
                
                mappings_data.append(row)
            
            if mappings_data:
                df_mappings = pd.DataFrame(mappings_data)
                df_mappings.to_excel(writer, sheet_name="API_Mappings", index=False)
            
            # Repository info sheet
            if include_repository_info and results.repositories:
                repo_data = []
                for repo in results.repositories:
                    repo_data.append({
                        "Repository_Name": repo.repository_name,
                        "Type": repo.repository_type.value,
                        "Local_Path": repo.local_path or "",
                        "Language": repo.language or "",
                        "Framework": repo.framework or "",
                        "Size_MB": repo.size_mb or 0,
                        "Last_Commit": repo.last_commit or "",
                    })
                
                if repo_data:
                    df_repos = pd.DataFrame(repo_data)
                    df_repos.to_excel(writer, sheet_name="Repositories", index=False)
            
            # Summary sheet
            summary_data = [
                {"Metric", "Value"},
                {"Job ID", str(results.job_id)},
                {"Analysis Type", results.analysis_type.value},
                {"Total Repositories", len(results.repositories)},
                {"Total API Mappings", len(api_mappings)},
                {"Execution Time (seconds)", results.execution_time_seconds or 0},
                {"Created At", results.created_at.isoformat()},
            ]
            
            df_summary = pd.DataFrame(summary_data[1:], columns=summary_data[0])
            df_summary.to_excel(writer, sheet_name="Summary", index=False)
        
        return output.getvalue()