"""Column lineage analysis service."""

import io
import json
import csv
from datetime import datetime
from typing import List, Optional, Dict, Any, AsyncGenerator
from uuid import UUID

import pandas as pd

from api.core.logging import LoggerMixin
from api.dependencies.database import DatabaseManager, get_database_engine
from api.v1.models.lineage import (
    LineageAnalysisRequest,
    ColumnLineageResult,
    ViewInfo,
    ColumnType,
    ExpressionType,
)
from api.v1.services.job_manager import JobManager
from api.core.analysis import process_all_views, save_results_to_csv, get_analysis_summary
from api.core.config import get_settings

class LineageService(LoggerMixin):
    """Column lineage analysis service."""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.job_manager = JobManager()
    
    async def process_lineage_analysis(
        self,
        job_id: UUID,
        request: LineageAnalysisRequest,
        user_id: str,
    ) -> List[ColumnLineageResult]:
        """Process column lineage analysis using standalone analysis module."""
        self.logger.info("Starting lineage analysis processing", job_id=str(job_id))
        
        try:
            # Update job status
            self.job_manager.update_job_status(job_id, "RUNNING", started_at=datetime.utcnow())
            
            # Get database engine for the standalone module
            engine = get_database_engine()
            
            # Determine environment from request or default to prod
            sf_env = getattr(request, 'environment', 'prod')
            
            # Get the actual list of views to process for progress tracking
            if request.view_names:
                views_to_process = request.view_names
                total_views = len(views_to_process)
            else:
                # Skip the expensive get_available_views call that causes hanging
                # Use a reasonable estimate and let the analysis discover views dynamically
                self.logger.info("No specific views provided, using dynamic discovery")
                total_views = 50  # Conservative estimate
                views_to_process = None
            
            # Update job with actual total views count
            self.job_manager.update_job_progress(
                job_id,
                total_views=total_views,
                processed_views=0
            )
            
            # Use standalone analysis module
            self.logger.info("Starting standalone analysis", 
                           view_count=total_views,
                           environment=sf_env)
            
            # Process views using standalone module
            csv_rows = process_all_views(
                sf_env=sf_env,
                view_names=request.view_names,
                engine=engine
            )
            
            # Convert CSV rows to API result format
            results = self._convert_csv_rows_to_api_results(csv_rows)
            
            # Calculate successful and failed views from results
            processed_view_names = set()
            for result in results:
                processed_view_names.add(result.view_name)
            
            successful_views = len(processed_view_names)
            failed_views = max(0, total_views - successful_views)
            
            # Update final progress
            self.job_manager.update_job_progress(
                job_id,
                processed_views=total_views,
                successful_views=successful_views,
                failed_views=failed_views
            )
            
            # Store results
            self.job_manager.store_job_results(job_id, results)
            
            # Auto-save results to CSV file
            await self._auto_save_results_to_csv(job_id, results)
            
            # Auto-save results to database table
            if request.database_filter and request.schema_filter:
                settings = get_settings()
                target_database = settings.AUTO_SAVE_TARGET_DATABASE or request.database_filter
                target_schema = settings.AUTO_SAVE_TARGET_SCHEMA or request.schema_filter
                await self._auto_save_results_to_database(
                    results, 
                    target_database, 
                    target_schema
                )
            
            # Update job completion
            self.job_manager.update_job_status(
                job_id,
                "COMPLETED",
                completed_at=datetime.utcnow(),
                results_count=len(results),
            )
            
            self.logger.info(
                "Lineage analysis completed",
                job_id=str(job_id),
                total_results=len(results),
                successful_views=successful_views,
                failed_views=failed_views
            )
            
            return results
            
        except Exception as e:
            self.logger.error("Lineage analysis failed", job_id=str(job_id), error=str(e))
            self.job_manager.update_job_status(
                job_id,
                "FAILED",
                completed_at=datetime.utcnow(),
                error_message=str(e),
            )
            raise
    
    def _convert_csv_rows_to_api_results(self, csv_rows: List[List[str]]) -> List[ColumnLineageResult]:
        """Convert CSV rows from standalone analysis to API result format."""
        results = []
        
        for row in csv_rows:
            if len(row) < 6:
                continue
                
            view_name, view_column, column_type, source_table, source_column, expression_type = row[:6]
            
            # Map column type
            if column_type.upper() == 'DIRECT':
                col_type = ColumnType.DIRECT
                confidence = 1.0
            elif column_type.upper() == 'DERIVED':
                col_type = ColumnType.DERIVED
                confidence = 0.8
            elif column_type.upper() == 'ERROR':
                col_type = ColumnType.UNKNOWN
                confidence = 0.0
            else:
                col_type = ColumnType.UNKNOWN
                confidence = 0.5
            
            # Map expression type
            expr_type = None
            if expression_type and expression_type.upper() not in ['', 'ERROR']:
                try:
                    expr_type = ExpressionType(expression_type.upper())
                except ValueError:
                    expr_type = None
            
            result = ColumnLineageResult(
                view_name=view_name,
                view_column=view_column,
                column_type=col_type,
                source_table=source_table,
                source_column=source_column,
                expression_type=expr_type,
                confidence_score=confidence,
                metadata={
                    "analysis_method": "standalone_integrated_parser",
                    "original_expression_type": expression_type
                }
            )
            results.append(result)
        
        return results
    
    async def get_available_databases(self) -> List[str]:
        """Get list of available databases."""
        self.logger.info("Getting available databases")
        
        try:
            query = """
            SELECT DATABASE_NAME
            FROM INFORMATION_SCHEMA.DATABASES
            ORDER BY DATABASE_NAME
            """
            
            results = self.db_manager.execute_query(query)
            
            # Handle different possible column name cases
            databases = []
            for row in results:
                # Try different column name variations
                db_name = None
                for attr in ['DATABASE_NAME', 'database_name', 'Database_Name']:
                    try:
                        db_name = getattr(row, attr, None)
                        if db_name:
                            break
                    except AttributeError:
                        continue
                
                # If attribute access fails, try dictionary-style access
                if not db_name and hasattr(row, '_mapping'):
                    mapping = row._mapping
                    for key in ['DATABASE_NAME', 'database_name', 'Database_Name']:
                        if key in mapping:
                            db_name = mapping[key]
                            break
                
                # If still no luck, try index access (first column)
                if not db_name:
                    try:
                        db_name = row[0]
                    except (IndexError, TypeError):
                        pass
                
                if db_name:
                    databases.append(db_name)
            
            self.logger.info(f"Found {len(databases)} databases: {databases}")
            return databases
            
        except Exception as e:
            self.logger.error("Failed to get available databases", error=str(e))
            # Log the actual row structure for debugging
            try:
                results = self.db_manager.execute_query(query)
                if results:
                    sample_row = results[0]
                    self.logger.error(f"Sample row structure: {dir(sample_row)}")
                    if hasattr(sample_row, '_mapping'):
                        self.logger.error(f"Row mapping keys: {list(sample_row._mapping.keys())}")
            except Exception as debug_error:
                self.logger.error(f"Debug query failed: {debug_error}")
            raise

    async def get_available_schemas(self, database_filter: str) -> List[str]:
        """Get list of available schemas for a specific database."""
        self.logger.info("Getting available schemas", database_filter=database_filter)
        
        try:
            # First, we need to switch to the target database context to query its schemas
            # In Snowflake, we can query schemas from INFORMATION_SCHEMA.SCHEMATA
            # but we need to specify the database context
            query = f"""
            SELECT SCHEMA_NAME
            FROM {database_filter}.INFORMATION_SCHEMA.SCHEMATA
            WHERE CATALOG_NAME = :database_filter
            ORDER BY SCHEMA_NAME
            """
            
            params = {"database_filter": database_filter}
            results = self.db_manager.execute_query(query, params)
            
            # Handle different possible column name cases
            schemas = []
            for row in results:
                # Try different column name variations
                schema_name = None
                for attr in ['SCHEMA_NAME', 'schema_name', 'Schema_Name']:
                    try:
                        schema_name = getattr(row, attr, None)
                        if schema_name:
                            break
                    except AttributeError:
                        continue
                
                # If attribute access fails, try dictionary-style access
                if not schema_name and hasattr(row, '_mapping'):
                    mapping = row._mapping
                    for key in ['SCHEMA_NAME', 'schema_name', 'Schema_Name']:
                        if key in mapping:
                            schema_name = mapping[key]
                            break
                
                # If still no luck, try index access (first column)
                if not schema_name:
                    try:
                        schema_name = row[0]
                    except (IndexError, TypeError):
                        pass
                
                if schema_name:
                    schemas.append(schema_name)
            
            self.logger.info(f"Found {len(schemas)} schemas for database {database_filter}")
            return schemas
            
        except Exception as e:
            self.logger.error("Failed to get available schemas", error=str(e))
            # Fallback: try without database prefix
            try:
                self.logger.info("Trying fallback query without database prefix")
                query = """
                SELECT DISTINCT SCHEMA_NAME
                FROM INFORMATION_SCHEMA.SCHEMATA
                WHERE CATALOG_NAME = :database_filter
                ORDER BY SCHEMA_NAME
                """
                
                params = {"database_filter": database_filter}
                results = self.db_manager.execute_query(query, params)
                
                # Handle different possible column name cases
                schemas = []
                for row in results:
                    # Try different column name variations
                    schema_name = None
                    for attr in ['SCHEMA_NAME', 'schema_name', 'Schema_Name']:
                        try:
                            schema_name = getattr(row, attr, None)
                            if schema_name:
                                break
                        except AttributeError:
                            continue
                    
                    # If attribute access fails, try dictionary-style access
                    if not schema_name and hasattr(row, '_mapping'):
                        mapping = row._mapping
                        for key in ['SCHEMA_NAME', 'schema_name', 'Schema_Name']:
                            if key in mapping:
                                schema_name = mapping[key]
                                break
                    
                    # If still no luck, try index access (first column)
                    if not schema_name:
                        try:
                            schema_name = row[0]
                        except (IndexError, TypeError):
                            pass
                    
                    if schema_name:
                        schemas.append(schema_name)
                
                self.logger.info(f"Found {len(schemas)} schemas for database {database_filter} (fallback)")
                return schemas
                
            except Exception as fallback_error:
                self.logger.error("Fallback query also failed", error=str(fallback_error))
                raise

    async def get_available_views(
        self,
        schema_filter: str,  # Made mandatory
        database_filter: str,  # Added mandatory database filter
    ) -> List[ViewInfo]:
        """
        Get list of available database views with mandatory filters.
        
        Parameters:
        - schema_filter: Schema name to filter views (required)
        - database_filter: Database name to filter views (required)
        """
        self.logger.info(
            "Getting available views", 
            schema_filter=schema_filter,
            database_filter=database_filter,
        )
        
        try:
            # Use a simpler, faster query without column count to avoid hanging
            query = f"""
            SELECT 
                TABLE_NAME as view_name,
                TABLE_SCHEMA as schema_name,
                TABLE_CATALOG as database_name,
                CREATED as created_date,
                LAST_ALTERED as last_modified
            FROM {database_filter}.INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_CATALOG = :database_filter
            AND TABLE_SCHEMA = :schema_filter
            ORDER BY TABLE_NAME
            LIMIT 1000
            """
            
            params = {
                "schema_filter": schema_filter,
                "database_filter": database_filter
            }
            
            results = self.db_manager.execute_query(query, params)
            
            views = []
            for row in results:
                views.append(ViewInfo(
                    view_name=row.view_name,
                    schema_name=row.schema_name,
                    database_name=row.database_name,
                    column_count=0,  # Skip expensive column count query
                    created_date=row.created_date,
                    last_modified=row.last_modified,
                ))
            
            self.logger.info(f"Found {len(views)} views matching filters")
            return views
            
        except Exception as e:
            self.logger.error("Failed to get available views", error=str(e))
            # Simplified fallback query
            try:
                self.logger.info("Trying simplified fallback query")
                query = """
                SELECT 
                    TABLE_NAME as view_name,
                    TABLE_SCHEMA as schema_name,
                    TABLE_CATALOG as database_name
                FROM INFORMATION_SCHEMA.VIEWS
                WHERE TABLE_CATALOG = :database_filter
                AND TABLE_SCHEMA = :schema_filter
                ORDER BY TABLE_NAME
                LIMIT 500
                """
                
                results = self.db_manager.execute_query(query, params)
                
                views = []
                for row in results:
                    views.append(ViewInfo(
                        view_name=row.view_name,
                        schema_name=row.schema_name,
                        database_name=row.database_name,
                        column_count=0,
                        created_date=None,
                        last_modified=None,
                    ))
                
                self.logger.info(f"Found {len(views)} views matching filters (fallback)")
                return views
                
            except Exception as fallback_error:
                self.logger.error("Fallback query also failed", error=str(fallback_error))
                # Return empty list instead of raising exception
                return []
    
    async def export_results(
        self,
        results: List[ColumnLineageResult],
        format: str,
        include_metadata: bool = True,
    ) -> AsyncGenerator[bytes, None]:
        """Export lineage results in specified format."""
        self.logger.info("Exporting results", format=format, count=len(results))
        
        if format.lower() == "csv":
            yield await self._export_csv(results, include_metadata)
        elif format.lower() == "json":
            yield await self._export_json(results, include_metadata)
        elif format.lower() == "excel":
            yield await self._export_excel(results, include_metadata)
        else:
            raise ValueError(f"Unsupported export format: {format}")
    
    async def _discover_views(
        self,
        database_filter: str,
        schema_filter: str,
        include_system_views: bool = False,
        max_views: Optional[int] = None,
    ) -> List[ViewInfo]:
        """Discover views from database."""
        self.logger.info(
            "Discovering views",
            database_filter=database_filter,
            schema_filter=schema_filter,
            include_system_views=include_system_views,
            max_views=max_views
        )
        
        query = f"""
        SELECT 
            TABLE_NAME as view_name,
            TABLE_SCHEMA as schema_name,
            TABLE_CATALOG as database_name
        FROM {database_filter}.INFORMATION_SCHEMA.VIEWS
        WHERE TABLE_CATALOG = :database_filter
        AND TABLE_SCHEMA = :schema_filter
        """
        
        params = {
            "database_filter": database_filter,
            "schema_filter": schema_filter
        }
        
        if not include_system_views:
            query += " AND TABLE_NAME NOT LIKE 'CANVAS_%'"
        
        query += " ORDER BY TABLE_NAME"
        
        if max_views:
            query += f" LIMIT {max_views}"
        
        self.logger.info("Executing view discovery query", query=query, params=params)
        
        try:
            results = self.db_manager.execute_query(query, params)
            self.logger.info(f"View discovery query returned {len(results)} results")
            
            views = []
            for i, row in enumerate(results):
                self.logger.debug(f"Processing row {i}: {row}")
                
                # Handle different column name cases
                view_name = None
                schema_name = None
                database_name = None
                
                # Try different attribute access methods
                for attr in ['view_name', 'TABLE_NAME']:
                    try:
                        view_name = getattr(row, attr, None)
                        if view_name:
                            break
                    except AttributeError:
                        continue
                
                for attr in ['schema_name', 'TABLE_SCHEMA']:
                    try:
                        schema_name = getattr(row, attr, None)
                        if schema_name:
                            break
                    except AttributeError:
                        continue
                
                for attr in ['database_name', 'TABLE_CATALOG']:
                    try:
                        database_name = getattr(row, attr, None)
                        if database_name:
                            break
                    except AttributeError:
                        continue
                
                # Fallback to index access
                if not view_name:
                    try:
                        view_name = row[0]
                    except (IndexError, TypeError):
                        pass
                
                if view_name:
                    view_info = ViewInfo(
                        view_name=view_name,
                        schema_name=schema_name or schema_filter,
                        database_name=database_name or database_filter,
                        column_count=0,  # Will be populated later if needed
                    )
                    views.append(view_info)
                    self.logger.debug(f"Added view: {view_name}")
                else:
                    self.logger.warning(f"Could not extract view name from row {i}")
            
            self.logger.info(f"Discovered {len(views)} views")
            return views
            
        except Exception as e:
            self.logger.error("Failed to discover views", error=str(e))
            raise
    
    async def _get_view_ddl(self, view_name: str, database_name: str = None, schema_name: str = None) -> Optional[str]:
        """Get DDL for a specific view."""
        try:
            self.logger.info("Getting DDL for view", view_name=view_name, database_name=database_name, schema_name=schema_name)
            
            # Try with full qualification first if we have database and schema
            if database_name and schema_name:
                try:
                    qualified_name = f"{database_name}.{schema_name}.{view_name}"
                    query = f"SELECT GET_DDL('VIEW', '{qualified_name}') as ddl"
                    self.logger.debug("Trying fully qualified DDL query", query=query)
                    result = self.db_manager.execute_query(query)
                    
                    if result and len(result) > 0:
                        ddl = getattr(result[0], 'ddl', result[0][0] if len(result[0]) > 0 else None)
                        if ddl:
                            self.logger.info("Successfully retrieved DDL with full qualification")
                            return ddl
                except Exception as e:
                    self.logger.warning("Failed to get DDL with full qualification", error=str(e))
            
            # Try with schema qualification only
            if schema_name:
                try:
                    qualified_name = f"{schema_name}.{view_name}"
                    query = f"SELECT GET_DDL('VIEW', '{qualified_name}') as ddl"
                    self.logger.debug("Trying schema qualified DDL query", query=query)
                    result = self.db_manager.execute_query(query)
                    
                    if result and len(result) > 0:
                        ddl = getattr(result[0], 'ddl', result[0][0] if len(result[0]) > 0 else None)
                        if ddl:
                            self.logger.info("Successfully retrieved DDL with schema qualification")
                            return ddl
                except Exception as e:
                    self.logger.warning("Failed to get DDL with schema qualification", error=str(e))
            
            # Try without schema qualification
            try:
                query = f"SELECT GET_DDL('VIEW', '{view_name}') as ddl"
                self.logger.debug("Trying unqualified DDL query", query=query)
                result = self.db_manager.execute_query(query)
                
                if result and len(result) > 0:
                    ddl = getattr(result[0], 'ddl', result[0][0] if len(result[0]) > 0 else None)
                    if ddl:
                        self.logger.info("Successfully retrieved DDL without qualification")
                        return ddl
            except Exception as e:
                self.logger.warning("Failed to get DDL without qualification", error=str(e))
            
            # If all attempts failed, log and return None
            self.logger.error(
                "Could not retrieve DDL for view after all attempts", 
                view_name=view_name,
                database_name=database_name,
                schema_name=schema_name
            )
            return None
            
        except Exception as e:
            self.logger.error("Failed to get DDL", view_name=view_name, error=str(e))
            return None
    
    async def _export_csv(
        self, 
        results: List[ColumnLineageResult], 
        include_metadata: bool
    ) -> bytes:
        """Export results as CSV."""
        output = io.StringIO()
        
        fieldnames = [
            "View_Name", "View_Column", "Column_Type",
            "Source_Table", "Source_Column", "Expression_Type",
            "Confidence_Score"
        ]
        
        if include_metadata:
            fieldnames.append("Metadata")
        
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        
        for result in results:
            row = {
                "View_Name": result.view_name,
                "View_Column": result.view_column,
                "Column_Type": result.column_type.value,
                "Source_Table": result.source_table,
                "Source_Column": result.source_column,
                "Expression_Type": result.expression_type.value if result.expression_type else "",
                "Confidence_Score": result.confidence_score,
            }
            
            if include_metadata:
                row["Metadata"] = json.dumps(result.metadata)
            
            writer.writerow(row)
        
        return output.getvalue().encode("utf-8")
    
    async def _export_json(
        self, 
        results: List[ColumnLineageResult], 
        include_metadata: bool
    ) -> bytes:
        """Export results as JSON."""
        data = []
        for result in results:
            item = result.model_dump()
            if not include_metadata:
                item.pop("metadata", None)
            data.append(item)
        
        return json.dumps(data, indent=2, default=str).encode("utf-8")
    
    async def _export_excel(
        self, 
        results: List[ColumnLineageResult], 
        include_metadata: bool
    ) -> bytes:
        """Export results as Excel."""
        # Convert to DataFrame
        data = []
        for result in results:
            row = {
                "View_Name": result.view_name,
                "View_Column": result.view_column,
                "Column_Type": result.column_type.value,
                "Source_Table": result.source_table,
                "Source_Column": result.source_column,
                "Expression_Type": result.expression_type.value if result.expression_type else "",
                "Confidence_Score": result.confidence_score,
            }
            
            if include_metadata:
                row["Metadata"] = json.dumps(result.metadata)
            
            data.append(row)
        
        df = pd.DataFrame(data)
        
        # Write to Excel
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Column_Lineage", index=False)
        
        return output.getvalue()
    
    async def _auto_save_results_to_csv(self, job_id: UUID, results: List[ColumnLineageResult]) -> None:
        """Auto-save analysis results to CSV file when job completes."""
        try:
            from api.core.config import get_settings
            from pathlib import Path
            
            settings = get_settings()
            
            # Check if auto-save is enabled
            if not settings.AUTO_SAVE_RESULTS:
                self.logger.debug("Auto-save disabled, skipping CSV export", job_id=str(job_id))
                return
            
            # Create results directory if it doesn't exist
            results_dir = Path(settings.RESULTS_DIRECTORY)
            results_dir.mkdir(exist_ok=True)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"lineage_analysis_{str(job_id)[:8]}_{timestamp}.csv"
            filepath = results_dir / filename
            
            self.logger.info("Auto-saving results to CSV", job_id=str(job_id), filepath=str(filepath))
            
            # Generate CSV content
            csv_content = await self._export_csv(results, include_metadata=True)
            
            # Write to file
            with open(filepath, 'wb') as f:
                f.write(csv_content)
            
            self.logger.info(
                "Results auto-saved successfully", 
                job_id=str(job_id), 
                filepath=str(filepath),
                results_count=len(results),
                file_size_bytes=len(csv_content)
            )
            
        except Exception as e:
            self.logger.error("Failed to auto-save results to CSV", job_id=str(job_id), error=str(e))
            # Don't raise the exception - auto-save failure shouldn't break the analysis
    
    async def _auto_save_results_to_database(
        self, 
        results: List[ColumnLineageResult], 
        database_name: str, 
        schema_name: str
    ) -> None:
        """Auto-save analysis results to Snowflake table in the same database and schema."""
        try:
            from api.core.config import get_settings
            
            settings = get_settings()
            
            # Check if database auto-save is enabled
            if not settings.AUTO_SAVE_TO_DATABASE:
                self.logger.debug("Auto-save to database disabled")
                return
            
            if not results:
                self.logger.info("No results to save to database")
                return
            
            # Table name for storing lineage results
            table_name = settings.AUTO_SAVE_TARGET_TABLE
            full_table_name = f"{database_name}.{schema_name}.{table_name}"
            
            self.logger.info(
                "Auto-saving results to database table", 
                table_name=full_table_name,
                results_count=len(results)
            )
            
            # Create table if it doesn't exist
            await self._create_lineage_results_table(database_name, schema_name, table_name)
            
            # Insert results into table
            await self._insert_lineage_results(results, database_name, schema_name, table_name)
            
            self.logger.info(
                "Results auto-saved to database successfully", 
                table_name=full_table_name,
                results_count=len(results)
            )
            
        except Exception as e:
            self.logger.error("Failed to auto-save results to database", error=str(e))
            # Don't raise the exception - auto-save failure shouldn't break the analysis
    
    async def _create_lineage_results_table(self, database_name: str, schema_name: str, table_name: str) -> None:
        """Create the lineage results table if it doesn't exist."""
        try:
            full_table_name = f"{database_name}.{schema_name}.{table_name}"
            
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                VIEW_NAME VARCHAR(255) NOT NULL,
                VIEW_COLUMN VARCHAR(255) NOT NULL,
                COLUMN_TYPE VARCHAR(50) NOT NULL,
                SOURCE_TABLE VARCHAR(500) NOT NULL,
                SOURCE_COLUMN VARCHAR(255) NOT NULL,
                EXPRESSION_TYPE VARCHAR(50),
                ANALYSIS_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
            
            self.logger.debug("Creating lineage results table", sql=create_table_sql)
            self.db_manager.execute_query(create_table_sql)
            
            self.logger.info("Lineage results table created/verified", table_name=full_table_name)
            
        except Exception as e:
            self.logger.error("Failed to create lineage results table", error=str(e))
            raise
    
    async def _insert_lineage_results(
        self, 
        results: List[ColumnLineageResult], 
        database_name: str, 
        schema_name: str, 
        table_name: str
    ) -> None:
        """Insert lineage results into the database table."""
        try:
            full_table_name = f"{database_name}.{schema_name}.{table_name}"
            
            # Truncate table before inserting new results
            self.logger.info("Truncating lineage results table before insert", table_name=full_table_name)
            truncate_sql = f"TRUNCATE TABLE {full_table_name}"
            
            # Execute truncate with explicit transaction handling
            try:
                self.db_manager.execute_query(truncate_sql)
                self.logger.info("Table truncated successfully", table_name=full_table_name)
            except Exception as truncate_error:
                self.logger.error("Failed to truncate table", table_name=full_table_name, error=str(truncate_error))
                raise
            
            # Prepare batch insert data
            insert_data = []
            for result in results:
                insert_data.append({
                    'view_name': result.view_name,
                    'view_column': result.view_column,
                    'column_type': result.column_type.value,
                    'source_table': result.source_table,
                    'source_column': result.source_column,
                    'expression_type': result.expression_type.value if result.expression_type else None
                })
            
            # Insert in batches to avoid query size limits
            batch_size = 100
            total_inserted = 0
            
            for i in range(0, len(insert_data), batch_size):
                batch = insert_data[i:i + batch_size]
                
                # Build VALUES clause for batch insert
                values_clauses = []
                current_timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                
                for row in batch:
                    expression_type = f"'{row['expression_type']}'" if row['expression_type'] else "NULL"
                    values_clause = f"('{row['view_name']}', '{row['view_column']}', '{row['column_type']}', '{row['source_table']}', '{row['source_column']}', {expression_type}, '{current_timestamp}', '{current_timestamp}')"
                    values_clauses.append(values_clause)
                
                insert_sql = f"""
                INSERT INTO {full_table_name} 
                (VIEW_NAME, VIEW_COLUMN, COLUMN_TYPE, SOURCE_TABLE, SOURCE_COLUMN, EXPRESSION_TYPE, ANALYSIS_TIMESTAMP, CREATED_AT)
                VALUES {', '.join(values_clauses)}
                """
                
                self.logger.debug(f"Inserting batch {i//batch_size + 1}", batch_size=len(batch))
                self.db_manager.execute_query(insert_sql)
                total_inserted += len(batch)
            
            self.logger.info(
                "Lineage results inserted successfully", 
                table_name=full_table_name,
                total_inserted=total_inserted
            )
            
        except Exception as e:
            self.logger.error("Failed to insert lineage results", error=str(e))
            raise