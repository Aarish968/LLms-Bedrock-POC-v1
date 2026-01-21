"""ThoughtSpot liveboard analysis service."""

import os
import csv
import sys
from typing import List, Optional, Dict
from uuid import UUID
from datetime import datetime
from pathlib import Path

from api.core.logging import get_logger
from api.core.config import get_settings
from api.dependencies.database import DatabaseManager
from api.v1.models.thoughtspot_analysis import (
    TSAnalysisJob,
    TSJobStatus,
    TSAnalysisRequest,
    TSResultsResponse,
    TableInfo,
)

logger = get_logger(__name__)


class ThoughtSpotAnalysisService:
    """Service for managing ThoughtSpot liveboard analysis operations."""
    
    def __init__(self):
        """Initialize the service."""
        self.jobs: Dict[UUID, TSAnalysisJob] = {}
        self.results_dir = "thoughtspot_analysis_results"
        os.makedirs(self.results_dir, exist_ok=True)
        
        # Initialize database manager
        self.db_manager = DatabaseManager()
        
        # Get settings
        self.settings = get_settings()
        
        # Bulk insert configuration from settings
        self.bulk_insert_batch_size = self.settings.BULK_INSERT_BATCH_SIZE
        
        logger.info(f"ThoughtSpot Analysis Service initialized with bulk insert batch size: {self.bulk_insert_batch_size}")
    
    def create_job(self, request: TSAnalysisRequest) -> TSAnalysisJob:
        """Create a new analysis job."""
        job = TSAnalysisJob(
            sf_environment=request.sf_environment,
            table_pattern=request.table_pattern,
            max_workers=request.max_workers,
            include_views=request.include_views,
            force_prod_urls=request.force_prod_urls,
            request_params=request.model_dump(),
        )
        
        # Generate output filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        job.result_file = os.path.join(
            self.results_dir, 
            f"thoughtspot_analysis_{job.job_id}_{timestamp}.csv"
        )
        
        self.jobs[job.job_id] = job
        logger.info(f"Created ThoughtSpot analysis job {job.job_id}")
        return job
    
    def get_job(self, job_id: UUID) -> Optional[TSAnalysisJob]:
        """Get job by ID."""
        return self.jobs.get(job_id)
    
    def update_job_status(self, job_id: UUID, status: TSJobStatus, **kwargs):
        """Update job status and other fields."""
        if job_id in self.jobs:
            job = self.jobs[job_id]
            job.status = status
            
            for key, value in kwargs.items():
                if hasattr(job, key):
                    setattr(job, key, value)
            
            if status in [TSJobStatus.COMPLETED, TSJobStatus.FAILED, TSJobStatus.CANCELLED]:
                job.completed_at = datetime.now()
            
            logger.info(f"Updated job {job_id} status to {status}")
    
    def list_jobs(self, limit: int = 50, offset: int = 0) -> List[TSAnalysisJob]:
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
    
    async def process_analysis(self, job_id: UUID, request: TSAnalysisRequest, user_id: Optional[str] = None):
        """Process ThoughtSpot analysis in background."""
        try:
            job = self.get_job(job_id)
            if not job:
                logger.error(f"Job {job_id} not found")
                return
            
            self.update_job_status(job_id, TSJobStatus.RUNNING)
            logger.info(f"Starting ThoughtSpot analysis for job {job_id}")
            
            # Add the correct path to sys.path for dc_canvas_service imports
            import os
            current_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            src_path = os.path.join(current_dir, 'api', 'core', 'toughtspot_to_table', 'src')
            sys.path.insert(0, src_path)
            
            from api.core.toughtspot_to_table.thoughtspot_to_table_analysis import (
                run_csv_liveboard_analysis,
                create_thoughtspot_csv_analysis_extension
            )
            from dc_canvas_service.common import Settings as TSSettings
            from dc_canvas_service.services.s3 import S3Service
            
            # Initialize ThoughtSpot services
            ts_settings = TSSettings()
            s3_service = S3Service(ts_settings)
            
            # Run analysis
            logger.info(f"Running ThoughtSpot CSV analysis for {request.sf_environment} environment")
            
            output_file = run_csv_liveboard_analysis(
                settings=ts_settings,
                s3_service=s3_service,
                sf_env=request.sf_environment,
                output_file=job.result_file,
                table_pattern=request.table_pattern,
                max_workers=request.max_workers,
                force_prod_urls=request.force_prod_urls,
                include_views=request.include_views
            )
            
            # Verify result file exists
            if Path(output_file).exists():
                logger.info("ThoughtSpot analysis completed successfully", job_id=str(job_id))
                
                # Count relationships in CSV
                total_relationships = self._count_csv_rows(output_file)
                
                # Insert CSV data into database
                db_success = self._insert_csv_data_to_database(output_file, job_id)
                
                if db_success:
                    logger.info("Data successfully inserted into THOUGHTSPOT_TABLEtable", job_id=str(job_id))
                    message = "Analysis completed successfully and data inserted into database"
                else:
                    logger.warning("Analysis completed but database insertion failed", job_id=str(job_id))
                    message = "Analysis completed successfully but database insertion failed"
                
                # Update job as completed
                self.update_job_status(
                    job_id, 
                    TSJobStatus.COMPLETED,
                    total_relationships=total_relationships,
                    message=message
                )
            else:
                logger.error("Analysis completed but result file not found", job_id=str(job_id))
                self.update_job_status(job_id, TSJobStatus.FAILED, error_message="Result file not found")
            
            logger.info(f"Completed ThoughtSpot analysis for job {job_id}")
            
        except Exception as e:
            logger.error(f"ThoughtSpot analysis failed for job {job_id}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            self.update_job_status(
                job_id, 
                TSJobStatus.FAILED,
                error_message=str(e)
            )
    
    def _count_csv_rows(self, csv_file_path: str) -> int:
        """Count the number of data rows in CSV file."""
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as f:
                return sum(1 for _ in f) - 1  # Subtract header
        except Exception as e:
            logger.warning(f"Failed to count CSV rows: {e}")
            return 0
    
    async def get_tables_list(self, sf_environment: str, include_views: bool = True) -> List[TableInfo]:
        """Get list of tables and views from Snowflake."""
        try:
            # Add the correct path to sys.path for dc_canvas_service imports
            import os
            current_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            src_path = os.path.join(current_dir, 'api', 'core', 'toughtspot_to_table', 'src')
            sys.path.insert(0, src_path)
            
            from api.core.toughtspot_to_table.thoughtspot_to_table_analysis import (
                create_thoughtspot_csv_analysis_extension
            )
            from dc_canvas_service.common import Settings as TSSettings
            from dc_canvas_service.services.s3 import S3Service
            
            # Initialize services
            ts_settings = TSSettings()
            s3_service = S3Service(ts_settings)
            csv_extension = create_thoughtspot_csv_analysis_extension(ts_settings, s3_service)
            
            logger.info(f"Fetching tables list for environment: {sf_environment}")
            
            # Get tables
            tables_info = csv_extension.get_all_tables_and_views_optimized(
                sf_env=sf_environment,
                include_views=include_views
            )
            
            return [
                TableInfo(
                    table_name=t["table_name"],
                    schema=t["schema"],
                    table_type=t["type"],
                    full_name=t["full_name"]
                )
                for t in tables_info
            ]
            
        except Exception as e:
            logger.error(f"Failed to fetch tables list: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return []
    
    def get_results(self, job_id: UUID) -> Optional[TSResultsResponse]:
        """Get analysis results for a job."""
        job = self.get_job(job_id)
        if not job:
            return None
        
        # Calculate summary statistics if job is completed
        summary = {}
        total_relationships = 0
        unique_liveboards = 0
        
        if job.status == TSJobStatus.COMPLETED and job.result_file and os.path.exists(job.result_file):
            try:
                with open(job.result_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
                    total_relationships = len(rows)
                    unique_liveboards = len(set(row.get('GUID', '') for row in rows if row.get('GUID')))
                    
                    # Count by schema
                    schema_counts = {}
                    for row in rows:
                        schema = row.get('Schema', '')
                        if schema:
                            schema_counts[schema] = schema_counts.get(schema, 0) + 1
                    
                    # Count by type
                    type_counts = {}
                    for row in rows:
                        table_type = row.get('Type', '')
                        if table_type:
                            type_counts[table_type] = type_counts.get(table_type, 0) + 1
                    
                    summary = {
                        "schema_distribution": schema_counts,
                        "type_distribution": type_counts,
                        "execution_time": (job.completed_at - job.started_at).total_seconds() if job.completed_at else 0
                    }
                    
            except Exception as e:
                logger.warning(f"Failed to calculate summary for job {job_id}: {e}")
        
        download_url = f"/api/v1/thoughtspot-analysis/results/{job_id}/download" if job.status == TSJobStatus.COMPLETED else None
        
        return TSResultsResponse(
            job_id=job.job_id,
            status=job.status,
            total_tables=job.total_tables,
            total_relationships=total_relationships,
            unique_liveboards=unique_liveboards,
            result_file=job.result_file,
            download_url=download_url,
            summary=summary
        )
    
    def _check_and_create_ts_table(self) -> bool:
        """Check if THOUGHTSPOT_TABLEtable exists, create if not."""
        try:
            # Check if table exists
            check_table_query = """
            SELECT COUNT(*) as table_count
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'CPS_DSCI_BR' 
            AND TABLE_NAME = 'THOUGHTSPOT_TABLE'
            AND TABLE_CATALOG = 'CPS_DB'
            """
            
            result = self.db_manager.execute_query(check_table_query)
            table_exists = result[0][0] > 0 if result else False
            
            if not table_exists:
                logger.info("Table THOUGHTSPOT_TABLEdoes not exist, creating it...")
                
                # Create table
                create_table_query = """
                CREATE OR REPLACE TABLE CPS_DB.CPS_DSCI_BR.THOUGHTSPOT_TABLE(
                    TABLE_NAME VARCHAR(255),
                    LIVEBOARD_NAME VARCHAR(500),
                    GUID VARCHAR(255),
                    SCHEMA VARCHAR(255),
                    TYPE VARCHAR(50),
                    ANALYSIS_TIMESTAMP TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                    CREATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
                )
                """
                
                self.db_manager.execute_query(create_table_query)
                logger.info("Table THOUGHTSPOT_TABLEcreated successfully")
            else:
                logger.info("Table THOUGHTSPOT_TABLEalready exists")
                
                # Truncate existing data
                truncate_query = "TRUNCATE TABLE CPS_DB.CPS_DSCI_BR.THOUGHTSPOT_TABLE"
                self.db_manager.execute_query(truncate_query)
                logger.info("Table THOUGHTSPOT_TABLEtruncated successfully")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to check/create TS table: {e}")
            return False
    
    def _insert_csv_data_to_database(self, csv_file_path: str, job_id: UUID) -> bool:
        """Insert CSV data into THOUGHTSPOT_TABLEtable."""
        try:
            logger.info(f"Starting to insert ThoughtSpot CSV data to database", job_id=str(job_id), csv_file=csv_file_path)
            
            # Check if CSV file exists
            if not Path(csv_file_path).exists():
                logger.error(f"CSV file not found: {csv_file_path}")
                return False
            
            # Log CSV file info
            csv_path = Path(csv_file_path)
            file_size = csv_path.stat().st_size
            logger.info(f"CSV file info - Size: {file_size} bytes, Path: {csv_file_path}")
            
            # Check and create table if needed
            if not self._check_and_create_ts_table():
                logger.error("Failed to ensure TS table exists")
                return False
            
            # Insert CSV data with bulk processing
            return self._insert_ts_csv_bulk(csv_file_path, job_id)
            
        except Exception as e:
            logger.error(f"Failed to insert ThoughtSpot CSV data to database: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False
    
    def _insert_ts_csv_bulk(self, csv_file_path: str, job_id: UUID) -> bool:
        """Insert ThoughtSpot CSV data with bulk processing."""
        try:
            # Read CSV and prepare batch insert data
            insert_data = []
            total_csv_rows = 0
            skipped_rows = []
            
            logger.info(f"Starting to process CSV file: {csv_file_path}")
            
            with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                # Log CSV headers
                headers = reader.fieldnames
                logger.info(f"CSV headers detected: {headers}")
                
                # Process each row
                for row_num, row in enumerate(reader, 1):
                    total_csv_rows += 1
                    
                    # Log first few rows for debugging
                    if row_num <= 3:
                        logger.info(f"Sample row {row_num}: {dict(row)}")
                    
                    try:
                        # Get values from CSV
                        table_name = self._get_csv_value(row, ['Table_name', 'TABLE_NAME', 'table_name'])
                        liveboard_name = self._get_csv_value(row, ['Liveboard_name', 'LIVEBOARD_NAME', 'liveboard_name'])
                        guid = self._get_csv_value(row, ['GUID', 'guid'])
                        schema = self._get_csv_value(row, ['Schema', 'SCHEMA', 'schema'])
                        table_type = self._get_csv_value(row, ['Type', 'TYPE', 'type'])
                        
                        # Skip rows without essential data
                        if not table_name or not liveboard_name:
                            skipped_rows.append({
                                'row_number': row_num,
                                'reason': 'Missing essential data (table_name or liveboard_name)',
                                'data': dict(row)
                            })
                            continue
                        
                        # Prepare row data
                        row_data = {
                            'table_name': table_name,
                            'liveboard_name': liveboard_name,
                            'guid': guid,
                            'schema': schema,
                            'type': table_type
                        }
                        insert_data.append(row_data)
                        
                        # Log progress every 100 rows
                        if row_num % 100 == 0:
                            logger.info(f"Processed {row_num} rows, {len(insert_data)} valid rows collected")
                        
                    except Exception as row_error:
                        skipped_rows.append({
                            'row_number': row_num,
                            'reason': f'Processing error: {row_error}',
                            'data': dict(row)
                        })
                        logger.error(f"Failed to process row {row_num}: {row_error}")
                        continue
            
            # Log processing summary
            logger.info(f"=== ThoughtSpot CSV PROCESSING SUMMARY ===")
            logger.info(f"Total CSV rows read: {total_csv_rows}")
            logger.info(f"Valid rows processed: {len(insert_data)}")
            logger.info(f"Rows skipped: {len(skipped_rows)}")
            logger.info(f"Processing success rate: {(len(insert_data)/total_csv_rows*100):.1f}%" if total_csv_rows > 0 else "N/A")
            
            if skipped_rows:
                logger.warning(f"=== SKIPPED ROWS DETAILS ===")
                for skipped in skipped_rows[:10]:  # Log first 10 skipped rows
                    logger.warning(f"Row {skipped['row_number']}: {skipped['reason']}")
            
            # Validate we have data to insert
            if len(insert_data) == 0:
                logger.error("No valid data found to insert!")
                return False
            
            # Insert data using bulk insert method
            inserted_count = self._bulk_insert_ts_data(insert_data)
            
            # Final summary
            logger.info(f"=== ThoughtSpot FINAL INSERTION SUMMARY ===")
            logger.info(f"CSV rows read: {total_csv_rows}")
            logger.info(f"Rows processed for insertion: {len(insert_data)}")
            logger.info(f"Rows successfully inserted: {inserted_count}")
            logger.info(f"Overall success rate: {(inserted_count/total_csv_rows*100):.1f}%" if total_csv_rows > 0 else "N/A")
            
            return inserted_count > 0
            
        except Exception as e:
            logger.error(f"Failed to insert ThoughtSpot CSV data: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False
    
    def _bulk_insert_ts_data(self, insert_data: list) -> int:
        """Insert ThoughtSpot data in batches using bulk INSERT statements."""
        try:
            if not insert_data:
                logger.info("No ThoughtSpot data to insert")
                return 0
            
            batch_size = self.bulk_insert_batch_size
            total_inserted = 0
            failed_batches = []
            
            # Generate current timestamp
            current_timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            
            logger.info(f"Starting ThoughtSpot bulk insert of {len(insert_data)} rows in batches of {batch_size}")
            
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
                            escaped = str(value).replace("'", "''").replace("\\", "\\\\")
                            return f"'{escaped}'"
                        
                        # Create VALUES clause
                        values_clause = f"""(
                            {escape_value(row['table_name'])}, 
                            {escape_value(row['liveboard_name'])}, 
                            {escape_value(row['guid'])}, 
                            {escape_value(row['schema'])}, 
                            {escape_value(row['type'])}, 
                            '{current_timestamp}', 
                            '{current_timestamp}'
                        )"""
                        
                        values_list.append(values_clause)
                    
                    # Build complete bulk INSERT statement
                    bulk_insert_sql = f"""
                    INSERT INTO CPS_DB.CPS_DSCI_BR.THOUGHTSPOT_TABLE
                    (TABLE_NAME, LIVEBOARD_NAME, GUID, SCHEMA, TYPE, ANALYSIS_TIMESTAMP, CREATED_AT)
                    VALUES {', '.join(values_list)}
                    """
                    
                    # Execute bulk insert
                    logger.info(f"Executing ThoughtSpot bulk insert for batch {batch_num} ({len(batch)} rows)")
                    self.db_manager.execute_query(bulk_insert_sql)
                    
                    total_inserted += len(batch)
                    logger.info(f"✅ Successfully inserted ThoughtSpot batch {batch_num}: {len(batch)} rows (Total: {total_inserted})")
                    
                except Exception as batch_error:
                    logger.error(f"❌ ThoughtSpot bulk insert failed for batch {batch_num}: {batch_error}")
                    failed_batches.append({
                        'batch_number': batch_num,
                        'batch_size': len(batch),
                        'error': str(batch_error)
                    })
                    
                    # Fallback to individual inserts
                    logger.info(f"Falling back to individual inserts for batch {batch_num}")
                    individual_inserted = self._insert_ts_batch_individually(batch, current_timestamp, i)
                    total_inserted += individual_inserted
                    
                    continue
            
            # Log final summary
            logger.info(f"🎉 ThoughtSpot bulk insert completed!")
            logger.info(f"   Total rows processed: {len(insert_data)}")
            logger.info(f"   Successfully inserted: {total_inserted}")
            logger.info(f"   Failed batches: {len(failed_batches)}")
            logger.info(f"   Success rate: {(total_inserted/len(insert_data)*100):.1f}%")
            
            if failed_batches:
                logger.warning(f"Failed ThoughtSpot batches summary:")
                for failed_batch in failed_batches:
                    logger.warning(f"  Batch {failed_batch['batch_number']}: {failed_batch['error']}")
            
            return total_inserted
            
        except Exception as e:
            logger.error(f"Failed to perform ThoughtSpot bulk insert: {e}")
            return total_inserted
    
    def _insert_ts_batch_individually(self, batch: list, current_timestamp: str, batch_start_index: int) -> int:
        """Fallback method to insert ThoughtSpot batch row by row."""
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
                INSERT INTO CPS_DB.CPS_DSCI_BR.THOUGHTSPOT_TABLE
                (TABLE_NAME, LIVEBOARD_NAME, GUID, SCHEMA, TYPE, ANALYSIS_TIMESTAMP, CREATED_AT)
                VALUES (
                    {escape_value(row['table_name'])}, 
                    {escape_value(row['liveboard_name'])}, 
                    {escape_value(row['guid'])}, 
                    {escape_value(row['schema'])}, 
                    {escape_value(row['type'])}, 
                    '{current_timestamp}', 
                    '{current_timestamp}'
                )
                """
                
                self.db_manager.execute_query(insert_sql)
                individual_inserted += 1
                
            except Exception as row_error:
                logger.error(f"❌ ThoughtSpot individual insert failed for row {global_row_num}: {row_error}")
                continue
        
        logger.info(f"ThoughtSpot individual fallback completed: {individual_inserted}/{len(batch)} rows inserted")
        return individual_inserted
    
    def _get_csv_value(self, row: dict, possible_keys: list) -> str:
        """Get value from CSV row using possible column name variations."""
        for key in possible_keys:
            if key in row:
                value = row[key]
                if value is not None:
                    value_str = str(value).strip()
                    if value_str.lower() in ['null', 'none', 'n/a', 'na', '']:
                        return ""
                    return value_str
        return ""
