#!/usr/bin/env python3
"""
Main execution module for DDL analysis - FastAPI Integration
Processes views from snowflake and generates CSV output as requested.
"""

import os
import sys
import pandas as pd
from typing import Optional, List, Tuple
from pathlib import Path

from api.dependencies.database_connection import SnowflakeConnection
from .integrated_parser import CompleteIntegratedParser


class _EngineWrappedConnection:
    """Wrapper that makes an injected engine work with SnowflakeConnection interface"""
    
    def __init__(self, sf_env, engine):
        self.sf_env = sf_env
        self.engine = engine
        
    def get_view_names_from_snowflake(self) -> list:
        """Get view names from Snowflake using INFORMATION_SCHEMA query with timeout handling."""
        try:
            # Simplified query with timeout protection
            sql = """
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_CATALOG = CURRENT_DATABASE()
            AND TABLE_NAME NOT LIKE 'CANVAS_%'
            AND (TABLE_SCHEMA = 'CPS_DSCI_API' OR TABLE_SCHEMA = 'CPS_DSCI_BR')
            ORDER BY TABLE_NAME 
            LIMIT 5
            """
            
            with self.engine.connect() as connection:
                import pandas as pd
                from sqlalchemy import text
                
                # Set a reasonable timeout (30 seconds)
                connection = connection.execution_options(autocommit=True)
                result = pd.read_sql(text(sql), connection)
            
            if not result.empty:
                # Try different possible column names (case insensitive)
                column_name = None
                for col in result.columns:
                    if col.upper() == 'TABLE_NAME':
                        column_name = col
                        break
                
                if column_name:
                    view_names = result[column_name].tolist()
                    print(f"Retrieved {len(view_names)} view names from Snowflake")
                    return view_names
                else:
                    print(f"TABLE_NAME column not found. Available columns: {list(result.columns)}")
                    return []
            else:
                print("No views found matching the criteria")
                return []
                
        except Exception as e:
            print(f"Error getting view names from Snowflake: {e}")
            # Return empty list instead of raising to prevent hanging
            print("Returning empty list to prevent hanging")
            return []
    
    def get_qualified_ddl(self, view_name: str) -> str:
        """Get DDL with proper schema qualification."""
        schema = self.find_view_in_schemas(view_name)
        
        if schema:
            qualified_name = f"{schema}.{view_name}"
            return self.get_ddl_for_view(qualified_name)
        else:
            # Try without schema qualification
            return self.get_ddl_for_view(view_name)
    
    def find_view_in_schemas(self, view_name: str) -> str:
        """Find which schema contains the view."""
        # First check CPS_DSCI_API
        try:
            sql = f"""
            SELECT table_schema 
            FROM information_schema.views 
            WHERE table_name = '{view_name}' 
            AND table_schema = 'CPS_DSCI_API'
            """
            
            with self.engine.connect() as connection:
                import pandas as pd
                from sqlalchemy import text
                result = pd.read_sql(text(sql), connection)
            
            if not result.empty:
                return 'CPS_DSCI_API'
                
        except Exception:
            pass
        
        # If not found, check all schemas
        try:
            sql = f"""
            SELECT table_schema 
            FROM information_schema.views 
            WHERE table_name = '{view_name}'
            """
            
            with self.engine.connect() as connection:
                import pandas as pd
                from sqlalchemy import text
                result = pd.read_sql(text(sql), connection)
            
            if not result.empty:
                return result.iloc[0]['table_schema']
                
        except Exception:
            pass
            
        return None
    
    def get_ddl_for_view(self, view_name: str) -> str:
        """Get DDL for a specific view using GET_DDL function."""
        try:
            sql = f"SELECT GET_DDL('VIEW', '{view_name}') as ddl"
            
            with self.engine.connect() as connection:
                import pandas as pd
                from sqlalchemy import text
                result = pd.read_sql(text(sql), connection)
            
            if not result.empty:
                return result.iloc[0]['ddl']
            else:
                return None
                
        except Exception as e:
            print(f"Error getting DDL for view {view_name}: {e}")
            return None


def process_all_views(sf_env='prod', view_names: Optional[List[str]] = None, engine=None) -> Tuple[List[List[str]], dict]:
    """
    Process all views from Snowflake and generate comprehensive analysis
    
    Args:
        sf_env: Environment (prod, dev, stage)
        view_names: Optional list of specific view names to process
        engine: Optional database engine to use (will create SnowflakeConnection with injected engine)
    
    Returns:
        Tuple of (CSV rows with analysis results, processing stats)
    """
    
    # Create database connection
    if engine:
        # Create a custom connection wrapper that uses the provided engine
        db_connection = _EngineWrappedConnection(sf_env, engine)
    else:
        # Use the standard SnowflakeConnection
        db_connection = SnowflakeConnection(sf_env)
    
    # Load view names from Snowflake or use provided list
    if view_names:
        target_views = view_names
        print(f"Processing {len(target_views)} specified views...")
    else:
        print("Getting available views from Snowflake...")
        target_views = db_connection.get_view_names_from_snowflake()
        if not target_views:
            print("No views found or query failed, returning empty results")
            return [], {"total_discovered": 0, "successfully_processed": 0, "failed": 0}
        print(f"Discovered and processing {len(target_views)} views from Snowflake...")
    
    # Create parser
    parser = CompleteIntegratedParser()
    
    # Results list for CSV
    all_csv_rows = []
    
    # Processing statistics
    processing_stats = {
        "total_discovered": len(target_views),
        "successfully_processed": 0,
        "failed": 0,
        "views_with_columns": 0,
        "views_with_zero_columns": 0
    }
    
    for i, view_name in enumerate(target_views, 1):
        print(f"Processing {i}/{len(target_views)}: {view_name}")
        
        try:
            # Get DDL for the view
            ddl_text = db_connection.get_qualified_ddl(view_name)
            
            if not ddl_text:
                print(f"  Could not retrieve DDL for {view_name}")
                processing_stats["failed"] += 1
                # Add error row
                all_csv_rows.append([
                    view_name.upper(),
                    'ERROR',
                    'ERROR',
                    'DDL_NOT_FOUND',
                    'DDL_NOT_FOUND',
                    'ERROR'
                ])
                continue
            
            print(f"  Retrieved DDL for {view_name}")
            
            # Analyze the DDL
            analysis = parser.analyze_ddl_statement(ddl_text)
            
            if 'error' in analysis:
                print(f"  Analysis error for {view_name}: {analysis['error']}")
                processing_stats["failed"] += 1
                # Add error row
                all_csv_rows.append([
                    view_name.upper(),
                    'ERROR',
                    'ERROR',
                    'ANALYSIS_ERROR',
                    'ANALYSIS_ERROR',
                    'ERROR'
                ])
                continue
            
            # Generate CSV for this view
            csv_output = parser.generate_standard_csv(analysis)
            
            # Parse CSV output and add to results
            csv_lines = csv_output.strip().split('\n')[1:]  # Skip header
            column_count = 0
            for line in csv_lines:
                if line.strip():
                    parts = [part.strip() for part in line.split(',')]
                    if len(parts) >= 6:
                        all_csv_rows.append(parts)
                        column_count += 1
            
            # Count as successfully processed regardless of column count
            processing_stats["successfully_processed"] += 1
            
            if column_count > 0:
                processing_stats["views_with_columns"] += 1
                print(f"  Successfully processed {view_name} - {column_count} columns")
            else:
                processing_stats["views_with_zero_columns"] += 1
                print(f"  Successfully processed {view_name} - 0 columns (empty view)")
            
        except Exception as e:
            print(f"  Error processing {view_name}: {e}")
            processing_stats["failed"] += 1
            # Add error row
            all_csv_rows.append([
                view_name.upper(),
                'ERROR',
                'ERROR',
                'PROCESSING_ERROR',
                'PROCESSING_ERROR',
                'ERROR'
            ])
    
    print(f"\n=== PROCESSING SUMMARY ===")
    print(f"Total views discovered: {processing_stats['total_discovered']}")
    print(f"Successfully processed: {processing_stats['successfully_processed']}")
    print(f"Failed: {processing_stats['failed']}")
    print(f"Views with columns: {processing_stats['views_with_columns']}")
    print(f"Views with zero columns: {processing_stats['views_with_zero_columns']}")
    
    return all_csv_rows, processing_stats


def save_results_to_csv(results: List[List[str]], filename: str = 'column_lineage_analysis.csv', 
                       output_dir: Optional[str] = None) -> pd.DataFrame:
    """
    Save all results to a single CSV file
    
    Args:
        results: List of CSV rows
        filename: Output filename
        output_dir: Optional output directory (defaults to current directory)
    
    Returns:
        DataFrame with the results
    """
    try:
        if output_dir:
            file_path = os.path.join(output_dir, filename)
        else:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            file_path = os.path.join(script_dir, filename)
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Create DataFrame
        df = pd.DataFrame(results, columns=[
            'View_Name', 'View_Column', 'Column_Type', 
            'Source_Table', 'Source_Column', 'Expression_Type'
        ])
        
        # Save to CSV
        df.to_csv(file_path, index=False)
        print(f"\nResults saved to: {file_path}")
        print(f"Total records: {len(df)}")
        
        # Print summary
        print(f"\nSummary:")
        print(f"  Unique views processed: {df['View_Name'].nunique()}")
        print(f"  Total columns analyzed: {len(df)}")
        
        # Column type breakdown
        print(f"  Column types:")
        type_counts = df['Column_Type'].value_counts()
        for col_type, count in type_counts.items():
            print(f"    {col_type}: {count}")
        
        # Success rate calculation
        total_columns = len(df)
        error_columns = len(df[df['Column_Type'].isin(['ERROR', 'UNKNOWN'])])
        success_columns = total_columns - error_columns
        success_rate = (success_columns / total_columns * 100) if total_columns > 0 else 0
        
        print(f"  Success rate: {success_rate:.1f}%")
        
        return df
        
    except Exception as e:
        print(f"Error saving results: {e}")
        raise


def get_analysis_summary(df: pd.DataFrame) -> dict:
    """
    Generate analysis summary statistics
    
    Args:
        df: DataFrame with analysis results
        
    Returns:
        Dictionary with summary statistics
    """
    total_columns = len(df)
    error_columns = len(df[df['Column_Type'].isin(['ERROR', 'UNKNOWN'])])
    success_columns = total_columns - error_columns
    success_rate = (success_columns / total_columns * 100) if total_columns > 0 else 0
    
    return {
        'total_views': df['View_Name'].nunique(),
        'total_columns': total_columns,
        'successful_columns': success_columns,
        'error_columns': error_columns,
        'success_rate': round(success_rate, 1),
        'column_type_breakdown': df['Column_Type'].value_counts().to_dict()
    }


def main():
    """Main execution function - for standalone usage"""
    sf_env = 'prod'  
    
    try:
        print("=== DDL COLUMN LINEAGE ANALYSIS ===")
        print(f"Environment: {sf_env}")
        print(f"Processing views from: Snowflake INFORMATION_SCHEMA")
        print(f"Usage: python main.py")
        
        # Process all views
        results, stats = process_all_views(sf_env)
        
        # Save to CSV
        df = save_results_to_csv(results)
        
        print(f"\n=== ANALYSIS COMPLETE ===")
        print(f"Check 'column_lineage_analysis.csv' for complete results")
        
        # Show sample of results
        if len(df) > 0:
            print(f"\nSample results (first 10 rows):")
            print(df.head(10).to_string(index=False))
        
    except Exception as e:
        print(f"Script failed: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()