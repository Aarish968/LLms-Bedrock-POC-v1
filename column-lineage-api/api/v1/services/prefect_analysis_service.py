"""Prefect repository analysis service."""

import asyncio
import csv
import os
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from uuid import UUID

from api.core.logging import get_logger
from api.core.prefect_repo_analysis.prefect_repo_clone_service import PrefectRepoCloner
from api.core.prefect_repo_analysis.table_column_reference_with_prefect_repos import TableColumnReferenceAnalyzer
from api.dependencies.database import DatabaseManager
from api.v1.models.prefect_analysis import (
    PrefectAnalysisJob,
    PrefectAnalysisRequest,
    PrefectAnalysisStatus,
    PrefectAnalysisResults,
    PrefectAnalysisSummary,
    PrefectDiscoveryResults,
    PrefectRepositoryInfo,
    TableColumnReference,
)

logger = get_logger(__name__)


class PrefectAnalysisService:
    """Service for managing Prefect repository analysis operations."""
    
    def __init__(self):
        # In-memory job storage (in production, use a proper database)
        self._jobs: Dict[UUID, PrefectAnalysisJob] = {}
        
        # Base directory for analysis results
        self.analysis_results_dir = Path("prefect_analysis_results")
        self.analysis_results_dir.mkdir(exist_ok=True)
        
        # Initialize database manager
        self.db_manager = DatabaseManager()
        
        logger.info("Prefect Analysis Service initialized")
    
    def get_job(self, job_id: UUID) -> Optional[PrefectAnalysisJob]:
        """Get job by ID."""
        return self._jobs.get(job_id)
    
    def create_job(self, job: PrefectAnalysisJob) -> None:
        """Store a new job."""
        self._jobs[job.job_id] = job
    
    def update_job(self, job_id: UUID, **updates) -> None:
        """Update job with new data."""
        if job_id in self._jobs:
            job = self._jobs[job_id]
            for key, value in updates.items():
                if hasattr(job, key):
                    setattr(job, key, value)
    
    def list_jobs(self, limit: int = 50, offset: int = 0) -> List[PrefectAnalysisJob]:
        """List jobs with pagination."""
        # Get jobs sorted by start time (newest first)
        all_jobs = sorted(self._jobs.values(), key=lambda x: x.started_at, reverse=True)
        
        # Apply pagination
        return all_jobs[offset:offset + limit]
    
    async def discover_prefect_repositories(
        self,
        job_id: UUID,
        request: PrefectAnalysisRequest
    ) -> PrefectDiscoveryResults:
        """Discover and clone Prefect repositories."""
        logger.info("Starting Prefect repository discovery", job_id=str(job_id))
        
        # Update job status
        self.update_job(job_id, status=PrefectAnalysisStatus.CLONING, message="Discovering and cloning Prefect repositories...")
        
        start_time = time.time()
        
        try:
            # Initialize Prefect repo cloner
            cloner = PrefectRepoCloner()
            
            # Setup AWS credentials
            cloner.setup_aws_credentials()
            
            # Get all repositories
            if request.specific_repos:
                # Validate repository names
                suspicious_names = {'string', 'str', 'name', 'test', 'temp', 'tmp', 'null', 'undefined'}
                valid_repos = []
                for repo_name in request.specific_repos:
                    if repo_name and repo_name.lower() not in suspicious_names:
                        valid_repos.append(repo_name)
                    else:
                        logger.warning(f"Skipping invalid repository name in request: {repo_name}")
                
                if not valid_repos:
                    logger.info("No valid repositories in specific_repos, switching to auto-discovery")
                    all_repos = cloner.get_all_repositories_via_boto3()
                    if not all_repos:
                        raise Exception("No repositories found. Please check your AWS configuration.")
                else:
                    all_repos = [{'repositoryName': name} for name in valid_repos]
                    logger.info(f"Using provided repository list: {len(all_repos)} repositories")
            else:
                all_repos = cloner.get_all_repositories_via_boto3()
                if not all_repos:
                    raise Exception("No repositories found. Please check your AWS configuration.")
                
                # Debug: Log all discovered repositories
                logger.info(f"Discovered {len(all_repos)} repositories from AWS CodeCommit:")
                for i, repo in enumerate(all_repos[:10], 1):  # Log first 10 repos
                    repo_name = repo.get('repositoryName', 'UNKNOWN')
                    logger.info(f"  {i}. {repo_name}")
                    if 'string' in repo_name.lower():
                        logger.warning(f"Found suspicious repository name: {repo}")
                if len(all_repos) > 10:
                    logger.info(f"  ... and {len(all_repos) - 10} more repositories")
            
            prefect_repos = set()
            repository_info = []
            
            # Step 1: Filter by naming convention (if not skipped)
            if not request.skip_naming_check:
                logger.info("Filtering by naming conventions...")
                prefect_repos.update(cloner.filter_by_naming_convention(all_repos))
            
            # Handle clone_all_repos option
            if request.clone_all_repos:
                logger.info("clone_all_repos=True: Adding ALL repositories for cloning")
                all_repo_names = [repo['repositoryName'] for repo in all_repos]
                prefect_repos.update(all_repo_names)
                logger.info(f"Added all {len(all_repo_names)} repositories for cloning")
            
            # Step 2: Content-based discovery (optional)
            if not request.skip_discovery:
                logger.info("Performing content-based discovery (this may take longer)...")
                
                remaining_repos = [r for r in all_repos if r['repositoryName'] not in prefect_repos]
                logger.info(f"Found {len(prefect_repos)} repos by naming convention, checking {len(remaining_repos)} remaining repos for Prefect patterns in parallel")
                
                if remaining_repos:
                    # Use parallel checking with appropriate number of workers
                    check_workers = min(request.max_workers, 10)  # Cap at 10 for checking
                    logger.info(f"Using {check_workers} parallel workers for Prefect pattern checking")
                    
                    remaining_repo_names = [r['repositoryName'] for r in remaining_repos]
                    additional_prefect_repos = cloner.check_repositories_for_prefect_parallel(
                        remaining_repo_names, 
                        max_workers=check_workers
                    )
                    prefect_repos.update(additional_prefect_repos)
            else:
                logger.info("Skipping content-based discovery for faster processing...")
                logger.info("Will clone repositories by naming patterns and verify Prefect content after cloning")
                
                # If no repos found by naming, add some common patterns
                if not prefect_repos:
                    logger.info("No repositories found by naming patterns, using fallback list")
                    fallback_repos = []
                    for repo in all_repos:
                        repo_name = repo['repositoryName'].lower()
                        # Look for common patterns that might indicate Prefect usage
                        if any(keyword in repo_name for keyword in ['flow', 'prefect', 'pipeline', 'etl', 'data']):
                            fallback_repos.append(repo['repositoryName'])
                    
                    if fallback_repos:
                        prefect_repos.update(fallback_repos[:15])  # Limit to first 15 matches
                        logger.info(f"Added {len(fallback_repos[:15])} repositories from fallback patterns")
            
            logger.info(f"Total repositories selected for cloning: {len(prefect_repos)}")
            if prefect_repos:
                logger.info(f"Repositories to process: {list(prefect_repos)}")
            else:
                logger.warning("No repositories selected for processing!")
            
            # Step 3: Clone repositories in parallel and verify Prefect content
            if prefect_repos:
                logger.info(f"\nStep 3: Cloning {len(prefect_repos)} repositories in parallel...")
                
                # Use parallel cloning with appropriate number of workers
                max_workers = min(request.max_workers, 8)  # Cap at 8 for cloning
                logger.info(f"Using {max_workers} parallel workers for cloning")
                
                clone_results = cloner.clone_repositories_parallel(
                    repo_names=list(prefect_repos), 
                    target_dir=request.target_directory,
                    region=None,  # Use default from credentials
                    max_workers=max_workers
                )
                
                # Validate clone_results structure
                if not clone_results or not isinstance(clone_results, dict):
                    logger.error(f"Invalid clone_results structure: {type(clone_results)}")
                    raise Exception("Clone operation returned invalid results")
                
                successful_clones = clone_results.get('successful', 0)
                
                # Log detailed results
                logger.info(f"Clone results: {clone_results.get('cloned', 0)} new, {clone_results.get('updated', 0)} updated, {clone_results.get('existing', 0)} existing, {clone_results.get('failed', 0)} failed")
                
                # Step 4: Verify which cloned repositories actually contain Prefect patterns
                logger.info(f"\nStep 4: Verifying Prefect patterns in cloned repositories...")
                
                # Safely get successfully cloned repos
                successfully_cloned_repos = []
                details = clone_results.get('details', [])
                
                if details and isinstance(details, list):
                    logger.info(f"Processing {len(details)} clone details")
                    for detail in details:
                        if detail and isinstance(detail, dict) and detail.get('success', False):
                            repo_name = detail.get('repo_name')
                            if repo_name:
                                successfully_cloned_repos.append(repo_name)
                    logger.info(f"Found {len(successfully_cloned_repos)} successfully cloned repositories")
                else:
                    logger.warning(f"Clone details is empty or invalid, using original repo list")
                    successfully_cloned_repos = list(prefect_repos)
                
                logger.info(f"Successfully cloned repos to verify: {len(successfully_cloned_repos)}")
                
                if successfully_cloned_repos:
                    logger.info(f"Checking {len(successfully_cloned_repos)} successfully cloned repositories for Prefect patterns")
                    try:
                        verified_prefect_repos = cloner.check_cloned_repositories_for_prefect(
                            successfully_cloned_repos, 
                            request.target_directory
                        )
                        logger.info(f"Verified {len(verified_prefect_repos)} repositories actually contain Prefect patterns")
                        
                        # Update prefect_repos to only include verified ones
                        prefect_repos = verified_prefect_repos
                    except Exception as e:
                        logger.error(f"Error verifying Prefect patterns: {e}")
                        import traceback
                        logger.error(f"Traceback: {traceback.format_exc()}")
                        logger.info("Using original repository list without verification")
                        # Keep original prefect_repos if verification fails
                else:
                    logger.warning("No repositories were successfully cloned")
                    prefect_repos = set()
                
                # Gather repository info for all repos (successful and failed)
                logger.info("Gathering repository information...")
                all_repo_names = list(prefect_repos) if prefect_repos else []
                
                # Add failed repos to the analysis
                details = clone_results.get('details', [])
                if details and isinstance(details, list):
                    failed_repos = [
                        d.get('repo_name') for d in details 
                        if d and isinstance(d, dict) and not d.get('success', False) and d.get('repo_name')
                    ]
                    all_repo_names.extend(failed_repos)
                    logger.info(f"Added {len(failed_repos)} failed repositories to analysis")
                
                logger.info(f"Total repositories to analyze: {len(all_repo_names)}")
                
                for i, repo_name in enumerate(all_repo_names):
                    try:
                        logger.info(f"Analyzing repository {i+1}/{len(all_repo_names)}: {repo_name}")
                        
                        # Find the clone result for this repo
                        repo_result = None
                        details = clone_results.get('details', [])
                        if details and isinstance(details, list):
                            repo_result = next(
                                (r for r in details 
                                 if r and isinstance(r, dict) and r.get('repo_name') == repo_name), 
                                None
                            )
                        
                        clone_success = repo_result.get('success', True) if repo_result and isinstance(repo_result, dict) else True
                        
                        repo_info = self._analyze_repository_info(
                            repo_name, 
                            request.target_directory, 
                            clone_success
                        )
                        repository_info.append(repo_info)
                        
                    except Exception as e:
                        logger.error(f"Error analyzing repository {repo_name}: {e}")
                        import traceback
                        logger.error(f"Traceback: {traceback.format_exc()}")
                        # Create a basic repo info if analysis fails
                        try:
                            basic_repo_info = PrefectRepositoryInfo(
                                repo_name=repo_name,
                                clone_status='unknown',
                                prefect_files_found=[],
                                python_files_count=0,
                                has_flows=False,
                                has_tasks=False
                            )
                            repository_info.append(basic_repo_info)
                        except Exception as e2:
                            logger.error(f"Error creating basic repo info for {repo_name}: {e2}")
                
                logger.info(f"Repository analysis completed: {len(repository_info)} repositories processed")
            else:
                logger.info(f"\nStep 3: No repositories to clone")
                successful_clones = 0
            
            discovery_time = time.time() - start_time
            
            # Ensure we have valid data for the response
            final_prefect_repos_count = len(prefect_repos) if prefect_repos else 0
            final_successful_clones = successful_clones if successful_clones is not None else 0
            final_repository_info = repository_info if repository_info else []
            final_all_repos_count = len(all_repos) if all_repos else 0
            
            # Update job with discovery results
            self.update_job(
                job_id,
                total_repos_found=final_prefect_repos_count,
                repos_cloned=final_successful_clones,
                message=f"Discovered {final_prefect_repos_count} Prefect repositories, cloned {final_successful_clones}"
            )
            
            logger.info(f"Discovery completed successfully:")
            logger.info(f"  Total repos checked: {final_all_repos_count}")
            logger.info(f"  Prefect repos found: {final_prefect_repos_count}")
            logger.info(f"  Successfully cloned: {final_successful_clones}")
            logger.info(f"  Discovery time: {discovery_time:.2f} seconds")
            
            return PrefectDiscoveryResults(
                total_repos_checked=final_all_repos_count,
                prefect_repos_found=final_prefect_repos_count,
                repositories=final_repository_info,
                discovery_time_seconds=discovery_time
            )
            
        except Exception as e:
            logger.error("Prefect repository discovery failed", job_id=str(job_id), error=str(e))
            raise
    
    def _analyze_repository_info(
        self, 
        repo_name: str, 
        target_directory: str, 
        clone_success: bool
    ) -> PrefectRepositoryInfo:
        """Analyze repository for Prefect-specific information."""
        repo_path = Path(target_directory) / repo_name
        
        prefect_files = []
        python_files_count = 0
        has_flows = False
        has_tasks = False
        
        if clone_success and repo_path.exists():
            try:
                # Look for Prefect configuration files
                prefect_config_patterns = [
                    'prefect.yaml', 'prefect.dev.yaml', 'prefect.prod.yaml', 'prefect.staging.yaml'
                ]
                
                for pattern in prefect_config_patterns:
                    if list(repo_path.rglob(pattern)):
                        prefect_files.append(pattern)
                
                # Count Python files and check for flows/tasks
                for py_file in repo_path.rglob("*.py"):
                    python_files_count += 1
                    
                    try:
                        with open(py_file, 'r', encoding='utf-8', errors='ignore') as f:
                            content = f.read()
                            
                        if '@flow' in content or 'prefect.flow' in content:
                            has_flows = True
                        if '@task' in content or 'prefect.task' in content:
                            has_tasks = True
                            
                    except Exception:
                        continue
                        
            except Exception as e:
                logger.warning(f"Error analyzing repository {repo_name}: {e}")
        
        return PrefectRepositoryInfo(
            repo_name=repo_name,
            clone_status="success" if clone_success else "failed",
            prefect_files_found=prefect_files,
            python_files_count=python_files_count,
            has_flows=has_flows,
            has_tasks=has_tasks
        )
    
    async def run_table_column_analysis(
        self,
        job_id: UUID,
        request: PrefectAnalysisRequest
    ) -> str:
        """Run table-column reference analysis on cloned repositories."""
        logger.info("Starting table-column reference analysis", job_id=str(job_id))
        
        # Update job status
        self.update_job(job_id, status=PrefectAnalysisStatus.ANALYZING, message="Analyzing table-column references...")
        
        try:
            # Auto-generate output filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"prefect_table_column_references_{timestamp}.csv"
            output_path = self.analysis_results_dir / output_filename
            
            logger.info(f"Analysis output will be saved to: {output_path}")
            
            # Initialize analyzer
            analyzer = TableColumnReferenceAnalyzer(
                sf_env=request.sf_environment,
                search_dir=request.target_directory
            )
            
            # Run analysis
            results = analyzer.run_analysis(
                output_file=str(output_path),
                max_workers=request.max_workers
            )
            
            # Update job with analysis results
            if results:
                unique_repos = len(set(r['repo_name'] for r in results))
                unique_tables = len(set(r['table_name'] for r in results if r['table_name']))
                
                self.update_job(
                    job_id,
                    total_references=len(results),
                    unique_tables=unique_tables,
                    unique_repos=unique_repos,
                    output_file=str(output_path)
                )
            
            return str(output_path)
            
        except Exception as e:
            logger.error("Table-column analysis failed", job_id=str(job_id), error=str(e))
            raise
    
    async def insert_results_to_database(
        self,
        job_id: UUID,
        output_file: str
    ) -> bool:
        """Insert analysis results into database."""
        logger.info("Inserting results to database", job_id=str(job_id))
        
        try:
            # Check if database connection is available
            if self.db_manager.mock_mode:
                logger.info("Mock mode - skipping database insertion")
                return True
            
            # Create table if it doesn't exist
            if not self._check_and_create_prefect_table():
                logger.error("Failed to ensure Prefect analysis table exists")
                return False
            
            # Insert CSV data
            return self._insert_csv_data_to_database(output_file, job_id)
            
        except Exception as e:
            logger.error("Failed to insert results to database", job_id=str(job_id), error=str(e))
            return False
    
    def _check_and_create_prefect_table(self) -> bool:
        """Check if PREFECT_TABLE_COLUMN_REFERENCES table exists, create if not."""
        try:
            # Check if table exists
            check_table_query = """
            SELECT COUNT(*) as table_count
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'CPS_DSCI_BR' 
            AND TABLE_NAME = 'PREFECT_TABLE_COLUMN_REFERENCES'
            AND TABLE_CATALOG = 'CPS_DB'
            """
            
            result = self.db_manager.execute_query(check_table_query)
            table_exists = result[0][0] > 0 if result else False
            
            if not table_exists:
                logger.info("Table PREFECT_TABLE_COLUMN_REFERENCES does not exist, creating it...")
                
                # Create table
                create_table_query = """
                CREATE OR REPLACE TABLE CPS_DB.CPS_DSCI_BR.PREFECT_TABLE_COLUMN_REFERENCES (
                    JOB_ID VARCHAR(36),
                    REPO_NAME VARCHAR(200),
                    FUNCTION_NAME VARCHAR(200),
                    TABLE_NAME VARCHAR(200),
                    COLUMN_NAME VARCHAR(200),
                    FILE_NAME VARCHAR(200),
                    ANALYSIS_TIMESTAMP TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                    CREATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
                )
                """
                
                self.db_manager.execute_query(create_table_query)
                logger.info("Table PREFECT_TABLE_COLUMN_REFERENCES created successfully")
            else:
                logger.info("Table PREFECT_TABLE_COLUMN_REFERENCES already exists")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to check/create Prefect table: {e}")
            return False
    
    def _insert_csv_data_to_database(self, csv_file_path: str, job_id: UUID) -> bool:
        """Insert CSV data into PREFECT_TABLE_COLUMN_REFERENCES table."""
        try:
            logger.info(f"Starting to insert CSV data to database", job_id=str(job_id), csv_file=csv_file_path)
            
            # Check if CSV file exists
            if not Path(csv_file_path).exists():
                logger.error(f"CSV file not found: {csv_file_path}")
                return False
            
            # Read CSV and prepare data
            insert_data = []
            total_csv_rows = 0
            
            with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                for row_num, row in enumerate(reader, 1):
                    total_csv_rows += 1
                    try:
                        # Map CSV columns to database columns
                        row_data = {
                            'job_id': str(job_id),
                            'repo_name': row.get('repo_name', ''),
                            'function_name': row.get('function_name', ''),
                            'table_name': row.get('table_name', ''),
                            'column_name': row.get('column_name', ''),
                            'file_name': row.get('file_name', ''),
                        }
                        insert_data.append(row_data)
                        
                    except Exception as row_error:
                        logger.warning(f"Failed to process row {row_num}: {row_error}")
                        continue
            
            # Insert data using bulk insert
            inserted_count = self._bulk_insert_prefect_data(insert_data)
            
            logger.info(f"CSV insertion summary - Total rows: {total_csv_rows}, Successfully inserted: {inserted_count}")
            
            return inserted_count > 0
            
        except Exception as e:
            logger.error(f"Failed to insert CSV data: {e}")
            return False
    
    def _bulk_insert_prefect_data(self, insert_data: List[Dict]) -> int:
        """Insert data in batches using bulk INSERT statements."""
        try:
            if not insert_data:
                logger.info("No data to insert")
                return 0
            
            batch_size = 100
            total_inserted = 0
            current_timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            
            logger.info(f"Starting bulk insert of {len(insert_data)} rows in batches of {batch_size}")
            
            for i in range(0, len(insert_data), batch_size):
                batch = insert_data[i:i + batch_size]
                batch_num = i // batch_size + 1
                
                try:
                    # Build bulk INSERT statement
                    values_list = []
                    
                    for row in batch:
                        def escape_value(value):
                            if value is None or value == '':
                                return "NULL"
                            escaped = str(value).replace("'", "''").replace("\\", "\\\\")
                            return f"'{escaped}'"
                        
                        values_clause = f"""(
                            {escape_value(row['job_id'])}, 
                            {escape_value(row['repo_name'])}, 
                            {escape_value(row['function_name'])}, 
                            {escape_value(row['table_name'])}, 
                            {escape_value(row['column_name'])}, 
                            {escape_value(row['file_name'])}, 
                            '{current_timestamp}', 
                            '{current_timestamp}'
                        )"""
                        
                        values_list.append(values_clause)
                    
                    # Execute bulk insert
                    bulk_insert_sql = f"""
                    INSERT INTO CPS_DB.CPS_DSCI_BR.PREFECT_TABLE_COLUMN_REFERENCES 
                    (JOB_ID, REPO_NAME, FUNCTION_NAME, TABLE_NAME, COLUMN_NAME, FILE_NAME, ANALYSIS_TIMESTAMP, CREATED_AT)
                    VALUES {', '.join(values_list)}
                    """
                    
                    self.db_manager.execute_query(bulk_insert_sql)
                    total_inserted += len(batch)
                    logger.info(f"Successfully inserted batch {batch_num}: {len(batch)} rows")
                    
                except Exception as batch_error:
                    logger.error(f"Bulk insert failed for batch {batch_num}: {batch_error}")
                    continue
            
            logger.info(f"Bulk insert completed: {total_inserted}/{len(insert_data)} rows inserted")
            return total_inserted
            
        except Exception as e:
            logger.error(f"Failed to perform bulk insert: {e}")
            return 0
    
    async def run_analysis(
        self,
        job_id: UUID,
        request: PrefectAnalysisRequest,
        user_id: str,
    ) -> None:
        """Run complete Prefect repository analysis in background."""
        logger.info(
            "Starting Prefect repository analysis", 
            job_id=str(job_id), 
            user_id=user_id,
            sf_environment=request.sf_environment
        )
        
        try:
            # Step 1: Discover and clone Prefect repositories
            discovery_results = await self.discover_prefect_repositories(job_id, request)
            
            if discovery_results.prefect_repos_found == 0:
                self.update_job(
                    job_id,
                    status=PrefectAnalysisStatus.COMPLETED,
                    message="No Prefect repositories found",
                    completed_at=datetime.now()
                )
                return
            
            # Step 2: Run table-column analysis
            output_file = await self.run_table_column_analysis(job_id, request)
            
            # Step 3: Insert results to database
            db_success = await self.insert_results_to_database(job_id, output_file)
            
            # Step 4: Complete job
            if db_success:
                message = "Analysis completed successfully and data inserted into database"
            else:
                message = "Analysis completed successfully but database insertion failed"
            
            self.update_job(
                job_id,
                status=PrefectAnalysisStatus.COMPLETED,
                message=message,
                completed_at=datetime.now()
            )
            
        except Exception as e:
            logger.error("Prefect repository analysis failed", job_id=str(job_id), error=str(e))
            self.update_job(
                job_id,
                status=PrefectAnalysisStatus.FAILED,
                message="Analysis failed with exception",
                error_message=str(e),
                completed_at=datetime.now()
            )
    
    def cancel_job(self, job_id: UUID) -> bool:
        """Cancel a job."""
        job = self.get_job(job_id)
        if not job:
            return False
        
        if job.status not in [PrefectAnalysisStatus.PENDING, PrefectAnalysisStatus.CLONING, PrefectAnalysisStatus.ANALYZING]:
            return False
        
        self.update_job(
            job_id,
            status=PrefectAnalysisStatus.CANCELLED,
            message="Job cancelled by user",
            completed_at=datetime.now()
        )
        return True
    
    def get_results_info(self, job_id: UUID) -> Optional[Dict]:
        """Get information about analysis results."""
        job = self.get_job(job_id)
        if not job or job.status != PrefectAnalysisStatus.COMPLETED or not job.output_file:
            return None
        
        # Check if output file exists
        output_path = Path(job.output_file)
        if not output_path.exists():
            return None
        
        # Get file info
        file_stat = output_path.stat()
        
        # Load sample references from CSV
        sample_references = self._load_sample_references(job.output_file, limit=10)
        
        # Generate summary statistics
        summary = self._generate_analysis_summary(job.output_file)
        
        return {
            "job_id": job_id,
            "status": job.status,
            "total_references": job.total_references,
            "unique_tables": job.unique_tables,
            "unique_repos": job.unique_repos,
            "unique_functions": len(set(ref.function_name for ref in sample_references)),
            "output_file": job.output_file,
            "file_size": file_stat.st_size,
            "created_at": datetime.fromtimestamp(file_stat.st_ctime).isoformat(),
            "modified_at": datetime.fromtimestamp(file_stat.st_mtime).isoformat(),
            "summary": summary,
            "sample_references": sample_references,
        }
    
    def _load_sample_references(self, csv_file: str, limit: int = 10) -> List[TableColumnReference]:
        """Load sample references from CSV file."""
        references = []
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    if i >= limit:
                        break
                    references.append(TableColumnReference(
                        repo_name=row.get('repo_name', ''),
                        function_name=row.get('function_name', ''),
                        table_name=row.get('table_name', ''),
                        column_name=row.get('column_name', ''),
                        file_name=row.get('file_name', '')
                    ))
        except Exception as e:
            logger.warning(f"Failed to load sample references: {e}")
        
        return references
    
    def _generate_analysis_summary(self, csv_file: str) -> Dict[str, Any]:
        """Generate analysis summary from CSV file."""
        summary = {
            "total_repositories": 0,
            "total_files_analyzed": 0,
            "total_references_found": 0,
            "unique_tables_referenced": 0,
            "unique_columns_referenced": 0,
            "top_repositories_by_references": [],
            "top_tables_by_references": []
        }
        
        try:
            repo_counts = {}
            table_counts = {}
            unique_files = set()
            unique_tables = set()
            unique_columns = set()
            
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    summary["total_references_found"] += 1
                    
                    repo_name = row.get('repo_name', '')
                    table_name = row.get('table_name', '')
                    column_name = row.get('column_name', '')
                    file_name = row.get('file_name', '')
                    
                    if repo_name:
                        repo_counts[repo_name] = repo_counts.get(repo_name, 0) + 1
                    if table_name:
                        table_counts[table_name] = table_counts.get(table_name, 0) + 1
                        unique_tables.add(table_name)
                    if column_name:
                        unique_columns.add(column_name)
                    if file_name:
                        unique_files.add(file_name)
            
            summary["total_repositories"] = len(repo_counts)
            summary["total_files_analyzed"] = len(unique_files)
            summary["unique_tables_referenced"] = len(unique_tables)
            summary["unique_columns_referenced"] = len(unique_columns)
            
            # Top repositories by references
            top_repos = sorted(repo_counts.items(), key=lambda x: x[1], reverse=True)[:5]
            summary["top_repositories_by_references"] = [
                {"repository": repo, "reference_count": count} for repo, count in top_repos
            ]
            
            # Top tables by references
            top_tables = sorted(table_counts.items(), key=lambda x: x[1], reverse=True)[:5]
            summary["top_tables_by_references"] = [
                {"table": table, "reference_count": count} for table, count in top_tables
            ]
            
        except Exception as e:
            logger.warning(f"Failed to generate analysis summary: {e}")
        
        return summary