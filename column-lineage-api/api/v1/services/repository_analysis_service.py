"""Repository analysis service."""

import asyncio
import csv
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
from uuid import UUID

from api.core.logging import get_logger
from api.core.repo_analysis.repository_cloning_service import RepositoryCloningService
from api.dependencies.database import DatabaseManager
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
        
        # Default repository names from environment variables
        self.default_frontend_repo = os.getenv("DEFAULT_FRONTEND_REPO", "guided-workflow")
        self.default_backend_repo = os.getenv("DEFAULT_BACKEND_REPO", "guided-workflow-backend")
        
        # Initialize database manager
        self.db_manager = DatabaseManager()
        
        logger.info(f"Default repositories configured - Frontend: {self.default_frontend_repo}, Backend: {self.default_backend_repo}")
    
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
        # Use default repository names from environment variables
        frontend_repo_name = self.default_frontend_repo
        backend_repo_name = self.default_backend_repo
        
        logger.info(
            "Starting repository analysis", 
            job_id=str(job_id), 
            user_id=user_id,
            frontend_repo_name=frontend_repo_name,
            backend_repo_name=backend_repo_name
        )
        
        try:
            # Step 1: Clone repositories
            frontend_path, backend_path = await self._clone_repositories(
                job_id=job_id,
                frontend_repo_name=frontend_repo_name,
                backend_repo_name=backend_repo_name,
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
                # Success - now insert data into database
                logger.info("Repository analysis completed successfully, inserting data to database", job_id=str(job_id))
                
                # Insert CSV data into database
                db_success = self._insert_csv_data_to_database(str(output_path), job_id)
                
                if db_success:
                    logger.info("Data successfully inserted into ACTION_TO_ENDPOINTS_TABLES_MAPPING table", job_id=str(job_id))
                    message = "Analysis completed successfully and data inserted into database"
                else:
                    logger.warning("Analysis completed but database insertion failed", job_id=str(job_id))
                    message = "Analysis completed successfully but database insertion failed"
                
                self.update_job(
                    job_id,
                    status=AnalysisStatus.COMPLETED,
                    message=message,
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
    
    def _check_and_create_table(self) -> bool:
        """Check if ACTION_TO_ENDPOINTS_TABLES_MAPPING table exists, create if not."""
        try:
            # Check if table exists
            check_table_query = """
            SELECT COUNT(*) as table_count
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'CPS_DSCI_BR' 
            AND TABLE_NAME = 'ACTION_TO_ENDPOINTS_TABLES_MAPPING'
            AND TABLE_CATALOG = 'CPS_DB'
            """
            
            result = self.db_manager.execute_query(check_table_query)
            table_exists = result[0][0] > 0 if result else False
            
            if not table_exists:
                logger.info("Table ACTION_TO_ENDPOINTS_TABLES_MAPPING does not exist, creating it...")
                
                # Create table with your DDL including new timestamp columns and larger VARCHAR sizes
                create_table_query = """
                CREATE OR REPLACE TABLE CPS_DB.CPS_DSCI_BR.ACTION_TO_ENDPOINTS_TABLES_MAPPING (
                    FRONTEND_FILE VARCHAR(200),
                    FRONTEND_FUNCTION VARCHAR(200),
                    HTTP_METHOD VARCHAR(20),
                    FRONTEND_URL VARCHAR(1000),
                    BACKEND_FILE VARCHAR(200),
                    BACKEND_FUNCTION VARCHAR(200),
                    BACKEND_ROUTE VARCHAR(1000),
                    DATABASE_TABLES VARCHAR(16777216),
                    STORED_PROCEDURES VARCHAR(16777216),
                    FLOW_CALLS VARCHAR(16777216),
                    RESPONSE_MODEL VARCHAR(1000),
                    RESPONSE_FIELDS VARCHAR(16777216),
                    NESTED_FIELDS VARCHAR(16777216),
                    TABLE_COLUMN_DETAILS VARCHAR(16777216),
                    ANALYSIS_TIMESTAMP TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                    CREATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
                )
                """
                
                self.db_manager.execute_query(create_table_query)
                logger.info("Table ACTION_TO_ENDPOINTS_TABLES_MAPPING created successfully")
            else:
                logger.info("Table ACTION_TO_ENDPOINTS_TABLES_MAPPING already exists")
                
                # Truncate existing data
                truncate_query = "TRUNCATE TABLE CPS_DB.CPS_DSCI_BR.ACTION_TO_ENDPOINTS_TABLES_MAPPING"
                self.db_manager.execute_query(truncate_query)
                logger.info("Table ACTION_TO_ENDPOINTS_TABLES_MAPPING truncated successfully")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to check/create table: {e}")
            return False
    
    def _insert_csv_data_to_database(self, csv_file_path: str, job_id: UUID) -> bool:
        """Insert CSV data into ACTION_TO_ENDPOINTS_TABLES_MAPPING table."""
        try:
            logger.info(f"Starting to insert CSV data to database", job_id=str(job_id), csv_file=csv_file_path)
            
            # Check if CSV file exists
            if not Path(csv_file_path).exists():
                logger.error(f"CSV file not found: {csv_file_path}")
                return False
            
            # Check and create table if needed
            if not self._check_and_create_table():
                logger.error("Failed to ensure table exists")
                return False
            
            # Try direct CSV loading first (more efficient for large datasets)
            if self._try_direct_csv_load(csv_file_path, job_id):
                logger.info("Successfully loaded data using direct CSV method")
                return True
            
            # Fallback to row-by-row insertion
            logger.info("Direct CSV load failed, falling back to row-by-row insertion")
            return self._insert_csv_row_by_row(csv_file_path, job_id)
            
        except Exception as e:
            logger.error(f"Failed to insert CSV data to database: {e}")
            return False
    
    def _try_direct_csv_load(self, csv_file_path: str, job_id: UUID) -> bool:
        """Try to load CSV directly using Snowflake's COPY INTO command."""
        try:
            # This is a more advanced approach that would require staging the file
            # For now, we'll skip this and use the row-by-row approach
            logger.info("Direct CSV load not implemented, using row-by-row insertion")
            return False
            
        except Exception as e:
            logger.warning(f"Direct CSV load failed: {e}")
            return False
    
    def _insert_csv_row_by_row(self, csv_file_path: str, job_id: UUID) -> bool:
        """Insert CSV data row by row (fallback method)."""
        try:
            # Read CSV and prepare batch insert data
            insert_data = []
            total_csv_rows = 0
            skipped_rows = []
            
            with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                # Detect delimiter
                sample = csvfile.read(1024)
                csvfile.seek(0)
                sniffer = csv.Sniffer()
                delimiter = sniffer.sniff(sample).delimiter
                
                reader = csv.DictReader(csvfile, delimiter=delimiter)
                
                # Process each row and collect data
                for row_num, row in enumerate(reader, 1):
                    total_csv_rows += 1
                    try:
                        # Check if row has essential data
                        frontend_file = self._get_csv_value(row, ['Frontend_File', 'Frontend File', 'FRONTEND_FILE'])
                        frontend_function = self._get_csv_value(row, ['Frontend_Function', 'Frontend Function', 'FRONTEND_FUNCTION'])
                        
                        # Skip rows that don't have essential data
                        if not frontend_file and not frontend_function:
                            skipped_rows.append({
                                'row_number': row_num,
                                'reason': 'Missing essential data (frontend_file and frontend_function)',
                                'data': dict(row)
                            })
                            logger.warning(f"Skipping row {row_num}: Missing essential data")
                            continue
                        
                        # Map CSV columns to database columns (handle different possible column names)
                        row_data = {
                            'frontend_file': frontend_file,
                            'frontend_function': frontend_function,
                            'http_method': self._get_csv_value(row, ['HTTP_Method', 'HTTP Method', 'HTTP_METHOD']),
                            'frontend_url': self._get_csv_value(row, ['Frontend_URL', 'Frontend URL', 'FRONTEND_URL']),
                            'backend_file': self._get_csv_value(row, ['Backend_File', 'Backend File', 'BACKEND_FILE']),
                            'backend_function': self._get_csv_value(row, ['Backend_Function', 'Backend Function', 'BACKEND_FUNCTION']),
                            'backend_route': self._get_csv_value(row, ['Backend_Route', 'Backend Route', 'BACKEND_ROUTE']),
                            'database_tables': self._get_csv_value(row, ['Database_Tables', 'Database Tables', 'DATABASE_TABLES']),
                            'stored_procedures': self._get_csv_value(row, ['Stored_Procedures', 'Stored Procedures', 'STORED_PROCEDURES']),
                            'flow_calls': self._get_csv_value(row, ['Flow_Calls', 'Flow Calls', 'FLOW_CALLS']),
                            'response_model': self._get_csv_value(row, ['Response_Model', 'Response Model', 'RESPONSE_MODEL']),
                            'response_fields': self._get_csv_value(row, ['Response_Fields', 'Response Fields', 'RESPONSE_FIELDS']),
                            'nested_fields': self._get_csv_value(row, ['Nested_Fields', 'Nested Fields', 'NESTED_FIELDS']),
                            'table_column_details': self._get_csv_value(row, ['Table_Column_Details', 'Table Column Details', 'TABLE_COLUMN_DETAILS'])
                        }
                        insert_data.append(row_data)
                        
                    except Exception as row_error:
                        skipped_rows.append({
                            'row_number': row_num,
                            'reason': f'Processing error: {row_error}',
                            'data': dict(row)
                        })
                        logger.warning(f"Failed to process row {row_num}: {row_error}, row data: {row}")
                        continue
            
            # Log CSV processing summary
            logger.info(f"CSV processing summary - Total rows: {total_csv_rows}, Processed: {len(insert_data)}, Skipped: {len(skipped_rows)}")
            
            if skipped_rows:
                logger.warning(f"Skipped {len(skipped_rows)} rows during CSV processing:")
                for skipped in skipped_rows:
                    logger.warning(f"  Row {skipped['row_number']}: {skipped['reason']}")
                    logger.debug(f"    Data: {skipped['data']}")
            
            # Insert data using the improved batch method
            inserted_count = self._batch_insert_data(insert_data)
            
            # Final summary
            logger.info(f"Final summary - CSV rows: {total_csv_rows}, Processed for insertion: {len(insert_data)}, Successfully inserted: {inserted_count}, Failed insertions: {len(insert_data) - inserted_count}")
            
            return inserted_count > 0
            
        except Exception as e:
            logger.error(f"Failed to insert CSV data row by row: {e}")
            return False
    
    def _batch_insert_data(self, insert_data: list) -> int:
        """Insert data in batches using individual INSERT statements to handle large data."""
        try:
            if not insert_data:
                logger.info("No data to insert")
                return 0
            
            # Use smaller batch size and individual INSERTs to handle large data
            batch_size = 100  # Reduced batch size for large data
            total_inserted = 0
            failed_rows = []
            
            # Generate current timestamp for all records
            from datetime import datetime
            current_timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # Format with milliseconds
            
            logger.info(f"Starting to insert {len(insert_data)} rows in batches of {batch_size}")
            
            for i in range(0, len(insert_data), batch_size):
                batch = insert_data[i:i + batch_size]
                
                # Insert each row individually to avoid query size limits
                for row_index, row in enumerate(batch):
                    global_row_num = i + row_index + 1
                    try:
                        # Escape single quotes in values and handle large text
                        def escape_value(value):
                            if value is None or value == '':
                                return "NULL"
                            # Convert to string and escape single quotes and backslashes
                            escaped = str(value).replace("'", "''").replace("\\", "\\\\")
                            return f"'{escaped}'"
                        
                        # Log the row being processed for debugging
                        logger.debug(f"Processing row {global_row_num}: {row.get('frontend_file', 'unknown')} - {row.get('frontend_function', 'unknown')}")
                        
                        # Build individual INSERT statement
                        insert_sql = f"""
                        INSERT INTO CPS_DB.CPS_DSCI_BR.ACTION_TO_ENDPOINTS_TABLES_MAPPING 
                        (FRONTEND_FILE, FRONTEND_FUNCTION, HTTP_METHOD, FRONTEND_URL, BACKEND_FILE, BACKEND_FUNCTION, BACKEND_ROUTE, DATABASE_TABLES, STORED_PROCEDURES, FLOW_CALLS, RESPONSE_MODEL, RESPONSE_FIELDS, NESTED_FIELDS, TABLE_COLUMN_DETAILS, ANALYSIS_TIMESTAMP, CREATED_AT)
                        VALUES (
                            {escape_value(row['frontend_file'])}, 
                            {escape_value(row['frontend_function'])}, 
                            {escape_value(row['http_method'])}, 
                            {escape_value(row['frontend_url'])}, 
                            {escape_value(row['backend_file'])}, 
                            {escape_value(row['backend_function'])}, 
                            {escape_value(row['backend_route'])}, 
                            {escape_value(row['database_tables'])}, 
                            {escape_value(row['stored_procedures'])}, 
                            {escape_value(row['flow_calls'])}, 
                            {escape_value(row['response_model'])}, 
                            {escape_value(row['response_fields'])}, 
                            {escape_value(row['nested_fields'])}, 
                            {escape_value(row['table_column_details'])}, 
                            '{current_timestamp}', 
                            '{current_timestamp}'
                        )
                        """
                        
                        self.db_manager.execute_query(insert_sql)
                        total_inserted += 1
                        
                        # Log progress every 10 rows
                        if total_inserted % 10 == 0:
                            logger.info(f"Inserted {total_inserted} rows so far...")
                        
                    except Exception as row_error:
                        failed_rows.append({
                            'row_number': global_row_num,
                            'error': str(row_error),
                            'data': row
                        })
                        logger.error(f"❌ FAILED to insert row {global_row_num}: {row_error}")
                        # Log the specific row data that failed with more detail
                        logger.error(f"   Frontend_File: {row.get('frontend_file', 'N/A')}")
                        logger.error(f"   Frontend_Function: {row.get('frontend_function', 'N/A')}")
                        logger.error(f"   HTTP_Method: {row.get('http_method', 'N/A')}")
                        logger.error(f"   Backend_Route: {row.get('backend_route', 'N/A')}")
                        
                        # Check for potential issues
                        for key, value in row.items():
                            if value and len(str(value)) > 1000:
                                logger.error(f"   Large field {key}: {len(str(value))} characters")
                            if value and ("'" in str(value)):
                                quote_count = str(value).count("'")
                                if quote_count > 5:
                                    logger.error(f"   Many quotes in {key}: {quote_count} quotes")
                        
                        continue
                
                logger.debug(f"Completed batch {i//batch_size + 1}")
            
            # Log summary of results
            logger.info(f"Batch insert completed - Total inserted: {total_inserted}, Failed: {len(failed_rows)}")
            
            if failed_rows:
                logger.warning(f"Failed to insert {len(failed_rows)} rows:")
                for failed_row in failed_rows:
                    logger.warning(f"  Row {failed_row['row_number']}: {failed_row['error']}")
                    logger.debug(f"    Data: {failed_row['data']}")
            
            return total_inserted
            
        except Exception as e:
            logger.error(f"Failed to batch insert data: {e}")
            return total_inserted  # Return what we managed to insert
    
    def _get_csv_value(self, row: dict, possible_keys: list) -> str:
        """Get value from CSV row using possible column name variations."""
        for key in possible_keys:
            if key in row:
                value = row[key]
                return value if value is not None else ""
        return ""