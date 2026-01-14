#!/usr/bin/env python3
"""
View Table Analyzer - Final Fixed Version

Excludes standalone schema names and fixes_needed, keeps only SCHEMA.TABLE format.
"""

import json
import pandas as pd
from sqlalchemy import create_engine, text
from common import sec
from typing import Dict, List, Any, Optional, Set
import re


class ViewTableAnalyzer:
    """Analyzer for extracting table dependencies from database views."""
    
    def __init__(self, sf_env: str = 'prod'):
        """Initialize analyzer with Snowflake environment."""
        self.sf_env = sf_env
        self.engine = None
        
    def check_env(self, env: str) -> str:
        """Check and return the correct connection name for environment."""
        if env == "dev":
            return "dev_cps_dsci_etl_svc"
        elif env == "stage":
            return "stg_cps_dsci_etl_svc"
        elif env == "prod":
            return "prd_cps_dsci_etl_svc"
        else:
            return env

    def get_correct_schema(self, env: str) -> str:
        """Get the correct schema based on environment."""
        if env == 'prod':
            return 'CPS_DSCI_API'
        else:
            return 'CPS_DSCI_BR'

    def create_sf_connection_engine(self):
        """Create Snowflake connection engine."""
        try:
            cn = self.check_env(self.sf_env)
            correct_schema = self.get_correct_schema(self.sf_env)
            self.engine = create_engine(
                sec.get_sf_pw(cn, 'CPS_DSCI_ETL_EXT1_WH', correct_schema)
            )
            print(f"Connected to Snowflake environment: {self.sf_env}")
        except Exception as e:
            print(f"Failed to create Snowflake connection: {e}")
            raise

    def get_all_views(self) -> pd.DataFrame:
        """Get all views from CPS_DSCI schemas, excluding CANVAS ThoughtSpot views."""
        sql_query = """
        SELECT TABLE_SCHEMA, TABLE_NAME, VIEW_DEFINITION
        FROM INFORMATION_SCHEMA.VIEWS
        WHERE TABLE_SCHEMA IN ('CPS_DSCI_API', 'CPS_DSCI_BR')
        AND TABLE_NAME NOT LIKE 'CANVAS_%_THOUGHT_SPOT_V'
        AND TABLE_NAME NOT LIKE 'CANVAS_%_THOUGHT_SPOT'
        ORDER BY TABLE_SCHEMA, TABLE_NAME;
        """
        
        print("Fetching views from CPS_DSCI schemas...")
        try:
            with self.engine.connect() as connection:
                df = pd.read_sql(text(sql_query), connection)
            print(f"Found {len(df)} views")
            return df
        except Exception as e:
            print(f"Error getting views: {e}")
            raise

    def get_view_ddl(self, schema: str, view_name: str) -> Optional[str]:
        """Get DDL for a specific view using GET_DDL function."""
        ddl_query = f"SELECT GET_DDL('view', 'CPS_DB.{schema}.{view_name}') as ddl"
        
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(ddl_query))
                row = result.fetchone()
                return row[0] if row else None
        except Exception as e:
            print(f"Error getting DDL for {schema}.{view_name}: {e}")
            return None

    def extract_cte_aliases(self, ddl: str) -> Set[str]:
        """Extract all CTE (WITH clause) aliases from DDL."""
        cte_aliases = set()
        
        # Pattern to find WITH clause and extract all CTE names
        with_pattern = r'WITH\s+(.*?)(?=\s+SELECT\s+\*|\s+SELECT\s+[^(]|\s+FROM|\s+INSERT|\s+UPDATE|\s+DELETE|$)'
        with_match = re.search(with_pattern, ddl, re.IGNORECASE | re.DOTALL)
        
        if with_match:
            with_section = with_match.group(1)
            
            # Extract individual CTE names
            cte_pattern = r'(\w+)\s+AS\s*\('
            cte_matches = re.findall(cte_pattern, with_section, re.IGNORECASE)
            
            for cte_name in cte_matches:
                cte_aliases.add(cte_name.upper())
        
        return cte_aliases

    def extract_tables_from_ddl(self, ddl: str) -> List[str]:
        """Extract actual table names from DDL, excluding CTE aliases."""
        if not ddl:
            return []
        
        # First, extract all CTE aliases to exclude them
        cte_aliases = self.extract_cte_aliases(ddl)
        
        tables = []
        
        # Extract tables from IDENTIFIER() functions
        identifier_patterns = [
            r'IDENTIFIER\s*\(\s*[\'"]([^\'\"]+)[\'"]\s*\)',
            r'IDENTIFIER\s*\(\s*([A-Z_][A-Z0-9_]*(?:\.[A-Z_][A-Z0-9_]*)*)\s*\)',
            r'IDENTIFIER\s*\(\s*[\'"][^\'\"]*[\'"]\s*\|\|\s*[\'"]([^\'\"]+)[\'"]\s*\)'
        ]
        
        for pattern in identifier_patterns:
            matches = re.findall(pattern, ddl, re.IGNORECASE)
            for match in matches:
                clean_match = match.strip()
                if clean_match and clean_match.upper() not in ['CPS_DB', 'INFORMATION_SCHEMA']:
                    tables.append(clean_match)
        
        # Extract tables from all parts of the query
        all_tables = self.extract_tables_from_query(ddl)
        tables.extend(all_tables)
        
        # Remove duplicates and filter out CTE aliases and other non-table references
        unique_tables = list(set(tables))
        filtered_tables = self.filter_actual_tables(unique_tables, cte_aliases)
        
        return sorted(filtered_tables)

    def extract_tables_from_query(self, query: str) -> List[str]:
        """Extract table names from any part of the query."""
        tables = []
        
        # Comprehensive patterns to match table references
        table_patterns = [
            # Schema.Table format with optional alias
            r'FROM\s+([A-Z_][A-Z0-9_]*\.[A-Z_][A-Z0-9_]*)(?:\s+(?:AS\s+)?[A-Z_][A-Z0-9_]*)?',
            r'JOIN\s+([A-Z_][A-Z0-9_]*\.[A-Z_][A-Z0-9_]*)(?:\s+(?:AS\s+)?[A-Z_][A-Z0-9_]*)?',
            # Table only format with optional alias
            r'FROM\s+([A-Z_][A-Z0-9_]*)(?:\s+(?:AS\s+)?[A-Z_][A-Z0-9_]*)?',
            r'JOIN\s+([A-Z_][A-Z0-9_]*)(?:\s+(?:AS\s+)?[A-Z_][A-Z0-9_]*)?',
            # Quoted table names with optional alias
            r'FROM\s+"([^"]+)"(?:\s+(?:AS\s+)?[A-Z_][A-Z0-9_]*)?',
            r'JOIN\s+"([^"]+)"(?:\s+(?:AS\s+)?[A-Z_][A-Z0-9_]*)?',
            # Tables in subqueries within IN clauses
            r'IN\s*\(\s*SELECT\s+[^)]*FROM\s+([A-Z_][A-Z0-9_]*(?:\.[A-Z_][A-Z0-9_]*)?)',
            # Tables in EXISTS clauses
            r'EXISTS\s*\(\s*SELECT\s+[^)]*FROM\s+([A-Z_][A-Z0-9_]*(?:\.[A-Z_][A-Z0-9_]*)?)'
        ]
        
        for pattern in table_patterns:
            matches = re.findall(pattern, query, re.IGNORECASE)
            tables.extend(matches)
        
        return tables

    def filter_actual_tables(self, tables: List[str], cte_aliases: Set[str]) -> List[str]:
        """Filter out aliases, CTEs, schema names, and keep only actual table names."""
        sql_keywords = {
            'SELECT', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'UNION', 'WITH', 
            'IDENTIFIER', 'CPS_DB', 'INFORMATION_SCHEMA', 'VALUES', 'CASE',
            'WHEN', 'THEN', 'ELSE', 'END', 'AND', 'OR', 'NOT', 'IN', 'EXISTS',
            'NULL', 'TRUE', 'FALSE', 'CURRENT_DATE', 'CURRENT_TIME'
        }
        
        # Schema names to exclude (standalone schema references)
        schema_names = {
            'CPS_DSCI_API', 'CPS_DSCI_BR', 'CPS_DSCI_STG', 'CPS_DSCI_WI', 
            'CPS_DSCI_ARCHIVE', 'CPS_DSCI_ETL', 'CPS_DB'
        }
        
        # Common alias names and CTE patterns to exclude
        common_aliases = {
            'RELATED', 'SUB', 'RESOLVE', 'HIER', 'SO', 'AS', 'T', 'TB', 'TBL',
            # Common CTE names
            'ANY_INCOMPLETE', 'ANY_COMPLETE', 'INCOMPLETE', 'COMPLETE',
            'TEMP', 'TMP', 'STAGING', 'STG', 'WORK', 'WRK',
            'FIXES_NEEDED'
        }
        
        # Patterns for common aliases and temporary names
        alias_patterns = [
            r'^[A-Z]$',  
            r'^[A-Z]\d+$',  
            r'^(T|TB|TBL)\d*$', 
            r'^ANY_\w+$', 
            r'^(TEMP|TMP|STAGING|STG|WORK|WRK)_?\w*$', 
            r'^(RELATED|SUB|RESOLVE|HIER|SO)$'  
        ]
        
        filtered_tables = []
        
        for table in tables:
            if not table or not table.strip():
                continue
                
            table_upper = table.upper().strip()
            
           
            if table_upper in sql_keywords:
                continue

            
            if table_upper in schema_names:
                continue
        
            if table_upper in cte_aliases:
                continue
                
          
            if table_upper in common_aliases:
                continue
          
            is_alias = any(re.match(pattern, table_upper) for pattern in alias_patterns)
            if is_alias:
                continue
                
           
            if len(table) <= 2:
                continue
            
         
            if '.' in table and len(table) > 5:
                # Schema.Table format - most likely real tables
                filtered_tables.append(table)
            elif len(table) > 4 and '_' in table and table_upper not in common_aliases and table_upper not in schema_names:
                # Table names with underscores - likely real tables (but not schema names)
                filtered_tables.append(table)
            elif len(table) > 8 and table_upper not in common_aliases and table_upper not in schema_names:
                # Long names that don't match common aliases or schema names - likely real tables
                filtered_tables.append(table)
           
        
        return filtered_tables

    def find_columns(self, df: pd.DataFrame) -> Dict[str, str]:
        """Find the correct column names in the DataFrame."""
        column_map = {}
        
        for col in df.columns:
            col_upper = col.upper()
            if 'SCHEMA' in col_upper and 'TABLE' in col_upper:
                column_map['schema'] = col
            elif 'NAME' in col_upper and 'TABLE' in col_upper:
                column_map['name'] = col
        
        return column_map

    def analyze_view_dependencies(self, views_df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze view dependencies and extract table information."""
        analysis_results = {}
        
        column_map = self.find_columns(views_df)
        
        if 'schema' not in column_map or 'name' not in column_map:
            print("Could not find required columns")
            return analysis_results
        
        schema_col = column_map['schema']
        name_col = column_map['name']
        
        total_views = len(views_df)
        processed_count = 0
        
        for _, view_row in views_df.iterrows():
            schema = view_row[schema_col]
            view_name = view_row[name_col]
            
            if ('CANVAS_' in view_name and 'THOUGHT_SPOT' in view_name):
                continue
            
            processed_count += 1

            ddl = self.get_view_ddl(schema, view_name)
            
            if ddl:
                tables = self.extract_tables_from_ddl(ddl)
                
                view_info = {
                    "schema": schema,
                    "view_name": view_name,
                    "tables": tables,
                    "table_count": len(tables),
                    "category": "Database View"
                }
                
                key = f"{schema}.{view_name}"
                analysis_results[key] = view_info
        
        return analysis_results

    def save_results_to_json(self, analysis_results: Dict[str, Any]) -> str:
        """Save analysis results to single JSON file."""
        # Create format: {view_name: [{"tables": [...]}]}
        simplified_output = {}
        
        for view_key, view_info in analysis_results.items():
            view_name = view_info["view_name"]
            simplified_output[view_name] = [
                {
                    "tables": view_info["tables"]
                }
            ]
        
        # Save to single file
        output_file = 'view_to_tables.json'
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(simplified_output, f, indent=2, ensure_ascii=False)
        
        print(f"Results saved to: {output_file}")
        return output_file


    def run_analysis(self) -> str:
        """Run complete view table analysis."""
        try:
            print("STARTING VIEW TABLE ANALYSIS ")
            
            self.create_sf_connection_engine()
            views_df = self.get_all_views()
            
            if views_df.empty:
                print("No views found")
                return ""
            
            analysis_results = self.analyze_view_dependencies(views_df)
            
            if not analysis_results:
                print("No analysis results")
                return ""
            
            output_file = self.save_results_to_json(analysis_results)
            print("ANALYSIS COMPLETE")
            return output_file
            
        except Exception as e:
            print(f"Analysis failed: {e}")
            raise


def main():
    """Main function."""
    import sys
    
    sf_env = sys.argv[1] if len(sys.argv) > 1 else 'prod'
    
    try:
        analyzer = ViewTableAnalyzer(sf_env)
        output_file = analyzer.run_analysis()
        
        if output_file:
            print(f"Results: {output_file}")
        
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())