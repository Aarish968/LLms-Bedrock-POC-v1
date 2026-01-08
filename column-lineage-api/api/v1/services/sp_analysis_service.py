"""Stored procedure analysis service."""

import os
import csv
from typing import List, Optional, Dict
from uuid import UUID
from datetime import datetime
from pathlib import Path

from api.core.logging import get_logger
from api.core.config import get_settings
from api.dependencies.database import DatabaseManager
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
        
        # Initialize database manager
        self.db_manager = DatabaseManager()
        
        # Get settings
        self.settings = get_settings()
        
        # Bulk insert configuration from settings
        self.bulk_insert_batch_size = self.settings.BULK_INSERT_BATCH_SIZE
        
        logger.info(f"SP Analysis Service initialized with bulk insert batch size: {self.bulk_insert_batch_size}")
    
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
            procedures = fetch_stored_procedures()
            
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
            
            # Run analysis directly in the background task (not in thread pool)
            # This prevents blocking the main thread while still running in background
            analyze_all_procedures(
                sf_environment=None,  # Use existing database infrastructure
                max_workers=request.max_workers,
                output_file=job.result_file,
                resume_from_partial=request.resume_from_partial
            )
            
            # Insert CSV data into database after analysis completes
            if Path(job.result_file).exists():
                logger.info("SP analysis completed successfully, inserting data to database", job_id=str(job_id))
                
                # Insert CSV data into database
                db_success = self._insert_csv_data_to_database(job.result_file, job_id)
                
                if db_success:
                    logger.info("Data successfully inserted into SP_TABLE_COLUMN_MAPPING table", job_id=str(job_id))
                    message = "Analysis completed successfully and data inserted into database"
                else:
                    logger.warning("Analysis completed but database insertion failed", job_id=str(job_id))
                    message = "Analysis completed successfully but database insertion failed"
                
                # Update job as completed
                self.update_job_status(job_id, SPJobStatus.COMPLETED, message=message)
            else:
                logger.error("Analysis completed but result file not found", job_id=str(job_id))
                self.update_job_status(job_id, SPJobStatus.FAILED, error_message="Result file not found")
            
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
            
            # Run directly without thread pool to avoid blocking issues
            result = analyze_stored_procedure(
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
            
            # Run directly without thread pool to avoid blocking issues
            procedures = fetch_stored_procedures()
            
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
    
    def _check_and_create_sp_table(self) -> bool:
        """Check if SP_TABLE_COLUMN_MAPPING table exists, create if not."""
        try:
            # Check if table exists
            check_table_query = """
            SELECT COUNT(*) as table_count
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'CPS_DSCI_BR' 
            AND TABLE_NAME = 'SP_TABLE_COLUMN_MAPPING'
            AND TABLE_CATALOG = 'CPS_DB'
            """
            
            result = self.db_manager.execute_query(check_table_query)
            table_exists = result[0][0] > 0 if result else False
            
            if not table_exists:
                logger.info("Table SP_TABLE_COLUMN_MAPPING does not exist, creating it...")
                
                # Create table with your DDL
                create_table_query = """
                CREATE OR REPLACE TABLE CPS_DB.CPS_DSCI_BR.SP_TABLE_COLUMN_MAPPING (
                    SP_NAME VARCHAR(255),
                    SP_SCHEMA VARCHAR(255),
                    SP_LANGUAGE VARCHAR(50),
                    TABLE_NAME VARCHAR(255),
                    COLUMN_NAME VARCHAR(255),
                    RELATIONSHIP_TYPES VARCHAR(500),
                    ANALYSIS_TIMESTAMP TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                    CREATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
                )
                """
                
                self.db_manager.execute_query(create_table_query)
                logger.info("Table SP_TABLE_COLUMN_MAPPING created successfully")
            else:
                logger.info("Table SP_TABLE_COLUMN_MAPPING already exists")
                
                # Truncate existing data
                truncate_query = "TRUNCATE TABLE CPS_DB.CPS_DSCI_BR.SP_TABLE_COLUMN_MAPPING"
                self.db_manager.execute_query(truncate_query)
                logger.info("Table SP_TABLE_COLUMN_MAPPING truncated successfully")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to check/create SP table: {e}")
            return False
    
    def _insert_csv_data_to_database(self, csv_file_path: str, job_id: UUID) -> bool:
        """Insert CSV data into SP_TABLE_COLUMN_MAPPING table."""
        try:
            logger.info(f"Starting to insert SP CSV data to database", job_id=str(job_id), csv_file=csv_file_path)
            
            # Check if CSV file exists
            if not Path(csv_file_path).exists():
                logger.error(f"CSV file not found: {csv_file_path}")
                return False
            
            # Log CSV file info
            csv_path = Path(csv_file_path)
            file_size = csv_path.stat().st_size
            logger.info(f"CSV file info - Size: {file_size} bytes, Path: {csv_file_path}")
            
            # Quick CSV file validation
            try:
                with open(csv_file_path, 'r', encoding='utf-8') as f:
                    first_line = f.readline().strip()
                    logger.info(f"CSV first line (header): {first_line}")
                    
                    # Count total lines
                    f.seek(0)
                    total_lines = sum(1 for _ in f) - 1  # Subtract header
                    logger.info(f"Total data rows in CSV: {total_lines}")
            except Exception as e:
                logger.warning(f"Could not validate CSV file: {e}")
            
            # Check and create table if needed
            if not self._check_and_create_sp_table():
                logger.error("Failed to ensure SP table exists")
                return False
            
            # Insert CSV data row by row with bulk processing
            return self._insert_sp_csv_row_by_row(csv_file_path, job_id)
            
        except Exception as e:
            logger.error(f"Failed to insert SP CSV data to database: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False
    
    def _insert_sp_csv_row_by_row(self, csv_file_path: str, job_id: UUID) -> bool:
        """Insert SP CSV data row by row with bulk processing."""
        try:
            # Read CSV and prepare batch insert data
            insert_data = []
            total_csv_rows = 0
            skipped_rows = []
            
            logger.info(f"Starting to process CSV file: {csv_file_path}")
            
            with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                # Detect delimiter
                sample = csvfile.read(1024)
                csvfile.seek(0)
                sniffer = csv.Sniffer()
                delimiter = sniffer.sniff(sample).delimiter
                
                logger.info(f"Detected CSV delimiter: '{delimiter}'")
                
                reader = csv.DictReader(csvfile, delimiter=delimiter)
                
                # Log CSV headers
                headers = reader.fieldnames
                logger.info(f"CSV headers detected: {headers}")
                
                # Process each row and collect data
                for row_num, row in enumerate(reader, 1):
                    total_csv_rows += 1
                    
                    # Log first few rows for debugging
                    if row_num <= 3:
                        logger.info(f"Sample row {row_num}: {dict(row)}")
                    
                    try:
                        # Check if row has essential data
                        sp_name = self._get_csv_value(row, ['SP_NAME', 'sp_name', 'Sp_Name'])
                        table_name = self._get_csv_value(row, ['TABLE_NAME', 'table_name', 'Table_Name'])
                        
                        # More detailed logging for debugging
                        if row_num <= 5:
                            logger.info(f"Row {row_num} - SP_NAME: '{sp_name}', TABLE_NAME: '{table_name}'")
                        
                        # Skip rows that don't have essential data
                        if not sp_name and not table_name:
                            skipped_rows.append({
                                'row_number': row_num,
                                'reason': 'Missing essential data (sp_name and table_name)',
                                'data': dict(row)
                            })
                            logger.warning(f"Skipping row {row_num}: Missing essential data - SP_NAME: '{sp_name}', TABLE_NAME: '{table_name}'")
                            continue
                        
                        # Skip rows with empty or null values in critical fields
                        if (sp_name.strip() == '' or sp_name.lower() in ['null', 'none', '']) and \
                           (table_name.strip() == '' or table_name.lower() in ['null', 'none', '']):
                            skipped_rows.append({
                                'row_number': row_num,
                                'reason': 'Empty or null critical fields',
                                'data': dict(row)
                            })
                            logger.warning(f"Skipping row {row_num}: Empty or null critical fields")
                            continue
                        
                        # Map CSV columns to database columns
                        row_data = {
                            'sp_name': sp_name,
                            'sp_schema': self._get_csv_value(row, ['SP_SCHEMA', 'sp_schema', 'Sp_Schema']),
                            'sp_language': self._get_csv_value(row, ['SP_LANGUAGE', 'sp_language', 'Sp_Language']),
                            'table_name': table_name,
                            'column_name': self._get_csv_value(row, ['COLUMN_NAME', 'column_name', 'Column_Name']),
                            'relationship_types': self._get_csv_value(row, ['RELATIONSHIP_TYPES', 'relationship_types', 'Relationship_Types'])
                        }
                        insert_data.append(row_data)
                        
                        # Log progress every 50 rows
                        if row_num % 50 == 0:
                            logger.info(f"Processed {row_num} rows, {len(insert_data)} valid rows collected")
                        
                    except Exception as row_error:
                        skipped_rows.append({
                            'row_number': row_num,
                            'reason': f'Processing error: {row_error}',
                            'data': dict(row)
                        })
                        logger.error(f"Failed to process row {row_num}: {row_error}")
                        logger.error(f"Row data: {dict(row)}")
                        continue
            
            # Detailed CSV processing summary
            logger.info(f"=== SP CSV PROCESSING SUMMARY ===")
            logger.info(f"Total CSV rows read: {total_csv_rows}")
            logger.info(f"Valid rows processed: {len(insert_data)}")
            logger.info(f"Rows skipped: {len(skipped_rows)}")
            logger.info(f"Processing success rate: {(len(insert_data)/total_csv_rows*100):.1f}%")
            
            if skipped_rows:
                logger.warning(f"=== SKIPPED ROWS DETAILS ===")
                for skipped in skipped_rows:
                    logger.warning(f"Row {skipped['row_number']}: {skipped['reason']}")
                    logger.debug(f"Skipped row data: {skipped['data']}")
            
            # Validate we have data to insert
            if len(insert_data) == 0:
                logger.error("No valid data found to insert!")
                return False
            
            # Insert data using the optimized bulk insert method
            inserted_count = self._bulk_insert_sp_data(insert_data)
            
            # Final comprehensive summary
            logger.info(f"=== SP FINAL INSERTION SUMMARY ===")
            logger.info(f"CSV rows read: {total_csv_rows}")
            logger.info(f"Rows processed for insertion: {len(insert_data)}")
            logger.info(f"Rows successfully inserted: {inserted_count}")
            logger.info(f"Rows failed to insert: {len(insert_data) - inserted_count}")
            logger.info(f"Overall success rate: {(inserted_count/total_csv_rows*100):.1f}%")
            
            return inserted_count > 0
            
        except Exception as e:
            logger.error(f"Failed to insert SP CSV data row by row: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False
    
    def _bulk_insert_sp_data(self, insert_data: list) -> int:
        """Insert SP data in batches using bulk INSERT statements for better performance."""
        try:
            if not insert_data:
                logger.info("No SP data to insert")
                return 0
            
            # Use bulk insert with configurable batch size
            batch_size = self.bulk_insert_batch_size
            total_inserted = 0
            failed_batches = []
            
            # Generate current timestamp for all records
            current_timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            
            logger.info(f"Starting SP bulk insert of {len(insert_data)} rows in batches of {batch_size}")
            
            for i in range(0, len(insert_data), batch_size):
                batch = insert_data[i:i + batch_size]
                batch_num = i // batch_size + 1
                
                try:
                    # Build bulk INSERT statement
                    values_list = []
                    
                    for row in batch:
                        # Escape and format values
                        def escape_value(value):
                            if value is None or value == '':
                                return "NULL"
                            # Convert to string and escape single quotes and backslashes
                            escaped = str(value).replace("'", "''").replace("\\", "\\\\")
                            return f"'{escaped}'"
                        
                        # Create VALUES clause for this row
                        values_clause = f"""(
                            {escape_value(row['sp_name'])}, 
                            {escape_value(row['sp_schema'])}, 
                            {escape_value(row['sp_language'])}, 
                            {escape_value(row['table_name'])}, 
                            {escape_value(row['column_name'])}, 
                            {escape_value(row['relationship_types'])}, 
                            '{current_timestamp}', 
                            '{current_timestamp}'
                        )"""
                        
                        values_list.append(values_clause)
                    
                    # Build complete bulk INSERT statement
                    bulk_insert_sql = f"""
                    INSERT INTO CPS_DB.CPS_DSCI_BR.SP_TABLE_COLUMN_MAPPING 
                    (SP_NAME, SP_SCHEMA, SP_LANGUAGE, TABLE_NAME, COLUMN_NAME, RELATIONSHIP_TYPES, ANALYSIS_TIMESTAMP, CREATED_AT)
                    VALUES {', '.join(values_list)}
                    """
                    
                    # Execute bulk insert
                    logger.info(f"Executing SP bulk insert for batch {batch_num} ({len(batch)} rows)")
                    self.db_manager.execute_query(bulk_insert_sql)
                    
                    total_inserted += len(batch)
                    logger.info(f"✅ Successfully inserted SP batch {batch_num}: {len(batch)} rows (Total: {total_inserted})")
                    
                except Exception as batch_error:
                    logger.error(f"❌ SP Bulk insert failed for batch {batch_num}: {batch_error}")
                    failed_batches.append({
                        'batch_number': batch_num,
                        'batch_size': len(batch),
                        'error': str(batch_error)
                    })
                    
                    # Fallback to individual inserts for this batch
                    logger.info(f"Falling back to individual SP inserts for batch {batch_num}")
                    individual_inserted = self._insert_sp_batch_individually(batch, current_timestamp, i)
                    total_inserted += individual_inserted
                    
                    continue
            
            # Log final summary
            logger.info(f"🎉 SP Bulk insert completed!")
            logger.info(f"   Total rows processed: {len(insert_data)}")
            logger.info(f"   Successfully inserted: {total_inserted}")
            logger.info(f"   Failed batches: {len(failed_batches)}")
            logger.info(f"   Success rate: {(total_inserted/len(insert_data)*100):.1f}%")
            
            if failed_batches:
                logger.warning(f"Failed SP batches summary:")
                for failed_batch in failed_batches:
                    logger.warning(f"  Batch {failed_batch['batch_number']}: {failed_batch['error']}")
            
            return total_inserted
            
        except Exception as e:
            logger.error(f"Failed to perform SP bulk insert: {e}")
            return total_inserted
    
    def _insert_sp_batch_individually(self, batch: list, current_timestamp: str, batch_start_index: int) -> int:
        """Fallback method to insert SP batch row by row when bulk insert fails."""
        individual_inserted = 0
        
        for row_index, row in enumerate(batch):
            global_row_num = batch_start_index + row_index + 1
            try:
                # Escape values
                def escape_value(value):
                    if value is None or value == '':
                        return "NULL"
                    escaped = str(value).replace("'", "''").replace("\\", "\\\\")
                    return f"'{escaped}'"
                
                # Build individual INSERT statement
                insert_sql = f"""
                INSERT INTO CPS_DB.CPS_DSCI_BR.SP_TABLE_COLUMN_MAPPING 
                (SP_NAME, SP_SCHEMA, SP_LANGUAGE, TABLE_NAME, COLUMN_NAME, RELATIONSHIP_TYPES, ANALYSIS_TIMESTAMP, CREATED_AT)
                VALUES (
                    {escape_value(row['sp_name'])}, 
                    {escape_value(row['sp_schema'])}, 
                    {escape_value(row['sp_language'])}, 
                    {escape_value(row['table_name'])}, 
                    {escape_value(row['column_name'])}, 
                    {escape_value(row['relationship_types'])}, 
                    '{current_timestamp}', 
                    '{current_timestamp}'
                )
                """
                
                self.db_manager.execute_query(insert_sql)
                individual_inserted += 1
                
            except Exception as row_error:
                logger.error(f"❌ SP Individual insert failed for row {global_row_num}: {row_error}")
                continue
        
        logger.info(f"SP Individual fallback completed: {individual_inserted}/{len(batch)} rows inserted")
        return individual_inserted
    
    def _get_csv_value(self, row: dict, possible_keys: list) -> str:
        """Get value from CSV row using possible column name variations."""
        for key in possible_keys:
            if key in row:
                value = row[key]
                # Handle None, empty string, and whitespace-only values
                if value is not None:
                    value_str = str(value).strip()
                    # Return empty string for null-like values
                    if value_str.lower() in ['null', 'none', 'n/a', 'na', '']:
                        return ""
                    return value_str
        return ""