#!/usr/bin/env python3
"""
Enhanced CSV Generator for Frontend-Backend API Analysis
This module provides the main functionality for analyzing frontend and backend repositories
and generating comprehensive CSV reports with table-column mappings.
"""

import os
import sys
import csv
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

# Import the complete analysis functionality
from action_to_table import (
    CompleteFrontendAnalyzer, 
    CompleteBackendAnalyzer, 
    APIMapper,
    FrontendCall,
    BackendRoute,
    Mapping
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EnhancedCSVGenerator:
    """Enhanced CSV generator with comprehensive frontend-backend analysis."""
    
    def __init__(self, frontend_path: str, backend_path: str):
        self.frontend_path = Path(frontend_path)
        self.backend_path = Path(backend_path)
        
        # Validate paths
        if not self.frontend_path.exists():
            raise ValueError(f"Frontend path does not exist: {frontend_path}")
        if not self.backend_path.exists():
            raise ValueError(f"Backend path does not exist: {backend_path}")
        
        logger.info(f"Initialized EnhancedCSVGenerator")
        logger.info(f"  Frontend: {self.frontend_path}")
        logger.info(f"  Backend: {self.backend_path}")
    
    def generate_enhanced_csv(self, output_file: str = "enhanced_analysis_results.csv") -> str:
        """Generate enhanced CSV with comprehensive analysis."""
        logger.info("Starting enhanced CSV generation...")
        
        try:
            # Step 1: Analyze frontend
            logger.info("Step 1: Analyzing frontend...")
            frontend_analyzer = CompleteFrontendAnalyzer(str(self.frontend_path))
            frontend_calls = frontend_analyzer.extract_frontend_calls()
            logger.info(f"Found {len(frontend_calls)} frontend API calls")
            
            # Step 2: Analyze backend
            logger.info("Step 2: Analyzing backend...")
            backend_analyzer = CompleteBackendAnalyzer(str(self.backend_path))
            backend_routes = backend_analyzer.extract_backend_routes()
            logger.info(f"Found {len(backend_routes)} backend routes")
            
            # Show table analysis summary
            routes_with_tables = sum(1 for route in backend_routes if route.tables)
            logger.info(f"Routes with tables: {routes_with_tables}")
            
            # Step 3: Create mappings
            logger.info("Step 3: Creating API mappings...")
            mapper = APIMapper(frontend_calls, backend_routes)
            mappings = mapper.create_mappings()
            
            # Step 4: Generate enhanced CSV
            logger.info("Step 4: Generating enhanced CSV...")
            self._write_enhanced_csv(mappings, output_file)
            
            # Step 5: Print summary
            self._print_summary(mappings)
            
            logger.info(f"Enhanced CSV generation completed: {output_file}")
            return output_file
            
        except Exception as e:
            logger.error(f"Enhanced CSV generation failed: {e}")
            raise
    
    def _write_enhanced_csv(self, mappings: List[Mapping], output_file: str):
        """Write enhanced CSV with comprehensive data."""
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Enhanced header with all the fields from your original code
            writer.writerow([
                "Frontend File", "Frontend Function", "HTTP Method", "Frontend URL",
                "Backend File", "Backend Function", "Backend Route", "Tables",
                "Response Model", "Response Fields", "Nested Fields", "Table Column Details",
                "Column Count", "Relationship Type", "Stored Procedures", "Flow Calls",
                "Confidence Score", "Analysis Metadata"
            ])
            
            for mapping in mappings:
                # Format response model info for CSV
                response_model = ""
                columns_str = ""
                nested_columns_str = ""
                table_column_details = ""
                column_count = 0
                relationship_type = "unknown"
                stored_procedures_str = ""
                flow_calls_str = ""
                confidence_score = 1.0
                
                if mapping.backend and mapping.backend.response_model_info:
                    response_model = (mapping.backend.response_model_info.get('response_model', '') or 
                                    mapping.backend.response_model_info.get('return_type', ''))
                    
                    # Format columns as "name,name,name" (only column names)
                    columns = mapping.backend.response_model_info.get('columns', [])
                    columns_str = ",".join([col['name'] for col in columns if isinstance(col, dict) and 'name' in col])
                    column_count += len([col for col in columns if isinstance(col, dict) and 'name' in col])
                    
                    # Format nested columns (only column names from nested fields)
                    nested_columns = mapping.backend.response_model_info.get('nested_columns', [])
                    nested_parts = []
                    for nested in nested_columns:
                        if isinstance(nested, dict):
                            nested_fields = nested.get('nested_fields', [])
                            fields_str = ",".join([field['name'] for field in nested_fields if isinstance(field, dict) and 'name' in field])
                            if fields_str:  # Only add if there are actual fields
                                nested_parts.append(f"{nested['name']}:[{fields_str}]")
                                column_count += len([field for field in nested_fields if isinstance(field, dict) and 'name' in field])
                    nested_columns_str = ";".join(nested_parts)
                
                # Enhanced table column details
                if mapping.backend and mapping.backend.tables:
                    tables_str = ",".join(table.upper() for table in mapping.backend.tables)
                    
                    # Create detailed table-column mapping
                    table_details = []
                    for table in mapping.backend.tables:
                        table_upper = table.upper()
                        # Try to get actual database columns for this table
                        try:
                            from action_to_table import get_table_columns
                            db_columns = get_table_columns(table_upper)
                            if db_columns:
                                table_details.append(f"{table_upper}:[{','.join(db_columns)}]")
                            else:
                                table_details.append(f"{table_upper}:[columns_unknown]")
                        except:
                            table_details.append(f"{table_upper}:[columns_unknown]")
                    
                    table_column_details = ";".join(table_details)
                    
                    # Determine relationship type
                    if len(mapping.backend.tables) == 1:
                        relationship_type = "single_table"
                    elif len(mapping.backend.tables) > 1:
                        relationship_type = "multi_table_join"
                else:
                    tables_str = ""
                    relationship_type = "no_tables"
                
                # Handle stored procedures and flow calls
                if mapping.backend:
                    if mapping.backend.stored_procedures:
                        stored_procedures_str = ",".join(mapping.backend.stored_procedures)
                    if mapping.backend.flow_calls:
                        flow_calls_str = ",".join(mapping.backend.flow_calls)
                
                if not mapping.backend:
                    relationship_type = "unmatched"
                
                # Calculate confidence score based on mapping quality
                confidence_score = self._calculate_confidence_score(mapping)
                
                # Create analysis metadata
                metadata = {
                    "frontend_file": mapping.frontend.file,
                    "frontend_line": mapping.frontend.line_number,
                    "backend_file": mapping.backend.file if mapping.backend else "",
                    "backend_line": mapping.backend.line_number if mapping.backend else 0,
                    "has_tables": bool(mapping.backend and mapping.backend.tables),
                    "has_response_model": bool(mapping.backend and mapping.backend.response_model_info),
                    "analysis_timestamp": "2024-01-01T00:00:00Z"  # You can use actual timestamp
                }
                
                writer.writerow([
                    mapping.frontend.file, 
                    mapping.frontend.function_name, 
                    mapping.frontend.method, 
                    mapping.frontend.url_pattern,
                    mapping.backend.file if mapping.backend else "",
                    mapping.backend.function_name if mapping.backend else "",
                    mapping.backend.route_pattern if mapping.backend else "",
                    tables_str,
                    response_model,
                    columns_str,
                    nested_columns_str,
                    table_column_details,
                    str(column_count),
                    relationship_type,
                    stored_procedures_str,
                    flow_calls_str,
                    str(confidence_score),
                    json.dumps(metadata)
                ])
    
    def _calculate_confidence_score(self, mapping: Mapping) -> float:
        """Calculate confidence score for the mapping."""
        if not mapping.backend:
            return 0.0
        
        score = 0.5  # Base score for having a backend match
        
        # Boost score for exact method match
        if mapping.frontend.method == mapping.backend.method:
            score += 0.2
        
        # Boost score for having database tables
        if mapping.backend.tables:
            score += 0.2
        
        # Boost score for having response model info
        if mapping.backend.response_model_info:
            score += 0.1
        
        return min(1.0, score)
    
    def _print_summary(self, mappings: List[Mapping]):
        """Print analysis summary."""
        total = len(mappings)
        matched = sum(1 for m in mappings if m.backend)
        unmatched = total - matched
        with_tables = sum(1 for m in mappings if m.backend and m.backend.tables)
        with_response_models = sum(1 for m in mappings if m.backend and m.backend.response_model_info and 
                                  (m.backend.response_model_info.get('response_model') or m.backend.response_model_info.get('return_type')))
        
        logger.info("\nAPI MAPPING SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Total Frontend Calls: {total}")
        logger.info(f"Matched:              {matched} ({matched/total*100:.1f}%)")
        logger.info(f"Unmatched:            {unmatched} ({unmatched/total*100:.1f}%)")
        logger.info(f"With Tables:          {with_tables} ({with_tables/total*100:.1f}%)")
        logger.info(f"With Response Models: {with_response_models} ({with_response_models/total*100:.1f}%)")
        
        # Show some examples of table mappings
        logger.info("\nSample Table Mappings:")
        logger.info("-" * 30)
        count = 0
        for m in mappings:
            if m.backend and m.backend.tables and count < 5:
                tables_str = ", ".join(m.backend.tables[:3])  # Show first 3 tables
                if len(m.backend.tables) > 3:
                    tables_str += f" (+{len(m.backend.tables)-3} more)"
                logger.info(f"  {m.backend.route_pattern} -> {tables_str}")
                count += 1
        
        if count == 0:
            logger.info("  No table mappings found")


def main():
    """Main function for command line usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Enhanced Frontend-Backend Analysis")
    parser.add_argument("--frontend", required=True, help="Frontend project path")
    parser.add_argument("--backend", required=True, help="Backend project path")
    parser.add_argument("--output", default="enhanced_analysis_results.csv", help="Output CSV file")
    
    args = parser.parse_args()
    
    try:
        generator = EnhancedCSVGenerator(args.frontend, args.backend)
        output_file = generator.generate_enhanced_csv(args.output)
        print(f"Analysis completed successfully: {output_file}")
    except Exception as e:
        print(f"Analysis failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()