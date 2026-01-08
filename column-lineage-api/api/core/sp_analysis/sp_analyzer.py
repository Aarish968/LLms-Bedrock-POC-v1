import os
import json
import csv
import re
import logging
import hashlib
from typing import List, Optional, Dict, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from sqlalchemy import create_engine, text
import time
from pathlib import Path

os.environ["AWS_PROFILE"] = "bedrock"

# LangChain Imports
from langchain_core.prompts import ChatPromptTemplate
from langchain_aws import ChatBedrock
from pydantic import BaseModel, Field

# Boto3 is implicitly used by ChatBedrock, but we often need it for setup or configuration
import boto3
from botocore.config import Config

# Import existing database connection utilities
from api.dependencies.database import get_database_engine, DatabaseManager

# --- Configure Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sp_analyzer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- 1. Enhanced Structured Output Schema using Pydantic ---
class TableColumnRelationship(BaseModel):
    """Specific table-column relationship in the stored procedure."""
    table_name: str = Field(description="The actual table name (resolved from variables if needed and also resolve with cte_names to get actual table name only nd ignore considering cte as table names)")
    column_name: str = Field(description="The specific column name, or * for table-level operations")
    relationship_types: str = Field(description="Comma-separated list of relationship types: USED_AS_SELECT, USED_AS_JOIN, USED_AS_FILTER, USED_AS_GROUP_BY, USED_AS_ORDER_BY, USED_AS_HAVING, PROCEDURE_CREATES_TABLE, USED_AS_UPDATE, USES_TABLE, USED_AS_INSERT, USED_AS_MERGE, USED_AS_DELETE")

class StoredProcedureAnalysis(BaseModel):
    """Analysis results for the stored procedure."""
    sp_name: str = Field(description="Name of the stored procedure")
    sp_schema: str = Field(description="Schema of the stored procedure")
    sp_language: str = Field(description="SQL, PYTHON, or MIXED")
    relationships: List[TableColumnRelationship] = Field(description="List of ALL table-column relationships found in the procedure")
    variables_detected: Dict[str, str] = Field(description="Variable to table name mappings detected")
    temp_tables_created: List[str] = Field(description="List of temporary tables created")
    cursors_detected: List[str] = Field(description="List of cursor names detected")

# --- 2. Snowflake Connection Management ---
def get_sf_connection_engine():
    """Get Snowflake connection engine using existing database infrastructure."""
    try:
        # Use the existing database engine from your infrastructure
        engine = get_database_engine()
        
        if engine is None:
            logger.warning("Database engine is None - running in mock mode")
            return None
        
        # Test the connection
        with engine.connect() as conn:
            from sqlalchemy import text
            result = conn.execute(text("SELECT CURRENT_VERSION()"))
            version = result.fetchone()[0]
            logger.info(f"Successfully connected to Snowflake version: {version}")
        
        return engine
        
    except Exception as e:
        logger.error(f"Failed to get Snowflake connection: {e}")
        raise

def get_database_manager():
    """Get DatabaseManager instance for query execution."""
    try:
        db_manager = DatabaseManager()
        
        if db_manager.mock_mode:
            logger.warning("DatabaseManager is in mock mode - no actual database connection")
        else:
            logger.info("DatabaseManager initialized successfully")
        
        return db_manager
        
    except Exception as e:
        logger.error(f"Failed to initialize DatabaseManager: {e}")
        raise

def fetch_stored_procedures(sf_environment: str = None) -> List[Dict]:
    """Fetch stored procedure definitions from Snowflake using existing database infrastructure."""
    # Note: sf_environment parameter is kept for backward compatibility but ignored
    # The environment is now handled by the existing database infrastructure
    db_manager = get_database_manager()
    
    # If in mock mode, return sample data for testing
    if db_manager.mock_mode:
        logger.info("Running in mock mode - returning sample stored procedure data")
        return [
            {
                'procedure_name': 'SAMPLE_PROCEDURE_1',
                'procedure_definition': 'CREATE OR REPLACE PROCEDURE SAMPLE_PROCEDURE_1() RETURNS STRING LANGUAGE SQL AS $$ BEGIN RETURN \'Hello World\'; END; $$',
                'procedure_schema': 'CPS_DSCI_API'
            },
            {
                'procedure_name': 'SAMPLE_PROCEDURE_2', 
                'procedure_definition': 'CREATE OR REPLACE PROCEDURE SAMPLE_PROCEDURE_2() RETURNS STRING LANGUAGE SQL AS $$ BEGIN SELECT * FROM SAMPLE_TABLE; END; $$',
                'procedure_schema': 'CPS_DSCI_API'
            }
        ]
    
    sql = """
    SELECT
        procedure_name,
        procedure_definition,
        procedure_schema
    FROM CPS_DB.INFORMATION_SCHEMA.PROCEDURES
    WHERE procedure_schema LIKE '%CPS_DSCI%'
    LIMIT 10
    """
    
    try:
        # Use DatabaseManager's execute_query method
        result_rows = db_manager.execute_query(sql)
        
        if not result_rows:
            logger.warning("No stored procedures found")
            return []
        
        # Convert result rows to list of dictionaries
        procedures = []
        for row in result_rows:
            # Handle different possible column orders/names
            if len(row) >= 3:
                procedures.append({
                    'procedure_name': str(row[0]),
                    'procedure_definition': str(row[1]),
                    'procedure_schema': str(row[2])
                })
        
        logger.info(f"Fetched {len(procedures)} stored procedures from Snowflake")
        return procedures
        
    except Exception as e:
        logger.error(f"Error fetching stored procedures: {e}")
        # In case of error, return empty list instead of raising
        logger.warning("Returning empty procedure list due to fetch error")
        return []

# --- 3. Configure LangChain and Bedrock ---
def get_bedrock_llm():
    """Initializes and returns the ChatBedrock model for Claude 3.5 Sonnet."""
    logger.debug("Initializing ChatBedrock for Claude Sonnet...")
   
    model_id = "us.anthropic.claude-sonnet-4-20250514-v1:0"
   
    config = Config(
        retries={'max_attempts': 10, 'mode': 'standard'},
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )
   
    boto3_session = boto3.Session()
    bedrock_client = boto3_session.client(
        service_name="bedrock-runtime",
        config=config
    )
   
    llm = ChatBedrock(
        model_id=model_id,
        client=bedrock_client,
        model_kwargs={
            "temperature": 0.0,  # Set to 0 for deterministic results
            "max_tokens": 16384,
            "top_p": 0.1,  # Add top_p for more consistent results
            "top_k": 1     # Add top_k for maximum determinism
        }
    )
   
    return llm

# --- 4. Enhanced Variable Resolution Logic ---
def extract_variable_mappings(sp_definition: str) -> Dict[str, str]:
    """Extract variable to table name mappings from all declaration styles."""
    variable_mapping = {}
    
    # Pattern 1: VARIABLE_NAME := 'TABLE_NAME';
    pattern1 = re.compile(r"(\w+)\s*:=\s*['\"]([A-Z_][A-Z0-9_\.]*)['\"]", re.IGNORECASE)
    
    # Pattern 2: LET VARIABLE_NAME TYPE := value;
    pattern2 = re.compile(r"LET\s+(\w+)\s+\w+\s*:=\s*['\"]([A-Z_][A-Z0-9_\.]*)['\"]", re.IGNORECASE)
    
    # Pattern 3: SET VARIABLE_NAME='value';
    pattern3 = re.compile(r"SET\s+(\w+)\s*=\s*['\"]([A-Z_][A-Z0-9_\.]*)['\"]", re.IGNORECASE)
    
    # Pattern 4: Dynamic table names with RANDSTR
    pattern4 = re.compile(r"(\w+)\s*:=\s*['\"]?([A-Z_][A-Z0-9_\.]*)['\"]?\s*\|\|\s*.*?RANDSTR", re.IGNORECASE)
    
    for pattern in [pattern1, pattern2, pattern3, pattern4]:
        for match in pattern.finditer(sp_definition):
            variable_name, table_name = match.groups()
            # Filter for table-like names
            if len(table_name) > 3 and ('_' in table_name or '.' in table_name):
                variable_mapping[variable_name] = table_name
    
    return variable_mapping

def detect_procedure_language(sp_definition: str) -> str:
    """Detect the language(s) used in the stored procedure."""
    has_python = bool(re.search(r"LANGUAGE\s+PYTHON", sp_definition, re.IGNORECASE))
    has_sql = bool(re.search(r"LANGUAGE\s+SQL", sp_definition, re.IGNORECASE))
    
    if has_python and has_sql:
        return "MIXED"
    elif has_python:
        return "PYTHON"
    else:
        return "SQL"

def extract_temp_tables(sp_definition: str) -> List[str]:
    """Extract temporary table names created in the procedure."""
    temp_tables = []
    
    # Pattern for CREATE TEMPORARY TABLE
    pattern = re.compile(r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMPORARY|TEMP)\s+TABLE\s+(?:IDENTIFIER\s*\(\s*:?(\w+)\s*\)|([A-Z_][A-Z0-9_\.]*)|([A-Z_][A-Z0-9_\.]*\s*\|\|\s*.*?))", re.IGNORECASE)
    
    for match in pattern.finditer(sp_definition):
        table_name = match.group(1) or match.group(2) or match.group(3)
        if table_name:
            temp_tables.append(table_name.strip())
    
    return temp_tables

def detect_cursors(sp_definition: str) -> List[str]:
    """Extract cursor names from the procedure."""
    cursors = []
    
    # Pattern for LET cursor_name CURSOR FOR
    pattern = re.compile(r"LET\s+(\w+)\s+CURSOR\s+FOR", re.IGNORECASE)
    
    for match in pattern.finditer(sp_definition):
        cursor_name = match.group(1)
        cursors.append(cursor_name)
    
    return cursors

def has_dynamic_sql(sp_definition: str) -> bool:
    """Check if the procedure contains dynamic SQL."""
    dynamic_patterns = [
        r"EXECUTE\s+IMMEDIATE",
        r"DYNAMIC_SQL\s*:=",
        r"\|\|\s*['\"].*?['\"]",  # String concatenation
        r"IDENTIFIER\s*\(\s*\?"  # Parameterized identifiers
    ]
    
    for pattern in dynamic_patterns:
        if re.search(pattern, sp_definition, re.IGNORECASE):
            return True
    
    return False

# --- 5. Enhanced Analysis Function ---
def analyze_stored_procedure(sp_definition: str, sp_name: str, sp_schema: str) -> Optional[StoredProcedureAnalysis]:
    """Analyze stored procedure and extract table-column relationships with enhanced pattern recognition."""
    try:
        # Create a hash of the procedure definition for caching
        procedure_hash = hashlib.md5(f"{sp_name}_{sp_schema}_{sp_definition}".encode()).hexdigest()
        cache_file = Path(f"sp_analysis_cache_{procedure_hash}.json")
        
        # Check if we have cached results
        if cache_file.exists():
            try:
                with open(cache_file, 'r') as f:
                    cached_data = json.load(f)
                logger.info(f"Using cached analysis for '{sp_name}' (hash: {procedure_hash[:8]})")
                return StoredProcedureAnalysis(**cached_data)
            except Exception as e:
                logger.warning(f"Failed to load cached analysis for '{sp_name}': {e}")
        
        llm = get_bedrock_llm()
        
        # Pre-analysis to gather metadata
        variable_mapping = extract_variable_mappings(sp_definition)
        sp_language = detect_procedure_language(sp_definition)
        temp_tables = extract_temp_tables(sp_definition)
        cursors = detect_cursors(sp_definition)
        has_dynamic = has_dynamic_sql(sp_definition)

        # Create a comprehensive system prompt with explicit instructions for consistency
        system_prompt = (
            "You are an expert SQL analyst specializing in Snowflake stored procedures. "
            "Analyze the procedure and extract ALL table-column relationships with extreme thoroughness and CONSISTENCY.\n\n"
            "CRITICAL: You must be DETERMINISTIC and CONSISTENT in your analysis. "
            "Always extract the SAME relationships for the SAME stored procedure code.\n\n"
            "The goal is to get all the actual tables and their columns used in the procedure, carefully resolving variables, CTEs, dynamic SQL, cursors, and complex constructs.\n\n"

            "VARIABLE RESOLUTION PATTERNS:\n"
            "1. VARIABLE_NAME := 'TABLE_NAME';\n"
            "2. LET VARIABLE_NAME TYPE := 'TABLE_NAME';\n"
            "3. SET VARIABLE_NAME='TABLE_NAME';\n"
            "4. Dynamic names with RANDSTR: TABLE_PREFIX || RANDSTR()\n\n"
            
            "RELATIONSHIP TYPES TO EXTRACT:\n"
            "- USED_AS_SELECT: Columns in SELECT clauses (including CTEs)\n"
            "- USED_AS_JOIN: Columns in JOIN ON conditions\n"
            "- USED_AS_FILTER: Columns in WHERE, HAVING, IF conditions\n"
            "- USED_AS_GROUP_BY: Columns in GROUP BY clauses\n"
            "- USED_AS_ORDER_BY: Columns in ORDER BY clauses\n"
            "- USED_AS_HAVING: Columns in HAVING clauses\n"
            "- USED_AS_UPDATE: Columns in UPDATE SET clauses\n"
            "- USED_AS_INSERT: Columns in INSERT statements\n"
            "- USED_AS_MERGE: Columns in MERGE operations (ON, WHEN clauses)\n"
            "- USED_AS_DELETE: Tables/columns in DELETE operations\n"
            "- PROCEDURE_CREATES_TABLE: Tables created by CREATE statements\n"
            "- USES_TABLE: General table references\n"
            
            "CONSISTENCY RULES:\n"
            "1. ALWAYS extract relationships in the SAME ORDER (alphabetical by table_name, then column_name)\n"
            "2. For each unique table-column combination, consolidate ALL relationship types into a single entry\n"
            "3. Use comma-separated values in relationship_types field (e.g., 'USED_AS_SELECT,USED_AS_FILTER,USED_AS_JOIN')\n"
            "4. Do NOT create duplicate entries for the same table-column pair\n"
            "5. Be EXHAUSTIVE but CONSISTENT - extract ALL relationships but in the SAME way every time\n\n"
            
            "Make sure to capture all the columns and tables involved in the procedure. Also deal with variable resolutions and other kinds to get all the column details as specified.\n\n"
            
            "SPECIAL PATTERNS TO HANDLE CONSISTENTLY:\n"
            "1. IDENTIFIER(:VARIABLE) - Always resolve variable to actual table name\n"
            "2. IDENTIFIER(?) with USING() - Always extract parameterized table references\n"
            "3. Complex CTEs with multiple levels - Always process in declaration order\n"
            "4. LATERAL FLATTEN operations - Always extract consistently\n"
            "5. JSON path expressions (value:field, PARSED_JSON:field) - Always extract field names\n"
            "6. Cursor operations (LET cursor FOR, OPEN cursor USING) - Always extract cursor query columns\n"
            "7. Dynamic SQL with string concatenation - Always extract table/column references\n"
            "8. Temporary tables with random names - Always identify consistently\n"
            "9. MERGE operations with complex ON conditions - Always extract ON and UPDATE columns\n"
            "10. Python embedded procedures - Always handle consistently\n\n"
            
            "CRITICAL INSTRUCTIONS FOR CONSISTENCY:\n"
            "1. Be EXHAUSTIVE but DETERMINISTIC - missing relationships is worse than duplicates, but results must be consistent\n"
            "2. For IDENTIFIER(:VAR), always resolve to actual table name from variable mappings\n"
            "3. Extract every column reference, even in complex expressions, but do it consistently\n"
            "4. For CTEs, always treat them as temporary tables with PROCEDURE_CREATES_TABLE\n"
            "5. For JSON paths, always extract the field names as column references\n"
            "6. For cursors, always extract all columns used in the cursor query\n"
            "7. For dynamic SQL, always extract table/column references from the SQL string\n"
            "8. For MERGE statements, always extract ON condition columns and UPDATE columns\n"
            "9. Always include context information (CTE name, cursor name, etc.)\n"
            "10. Always handle mixed SQL/Python procedures the same way\n\n"
            
            "DETERMINISTIC OUTPUT REQUIREMENT:\n"
            "Your analysis must produce IDENTICAL results for IDENTICAL input. "
            "Sort all relationships alphabetically by table_name, then by column_name for consistency.\n\n"
            
            "Return ONLY valid JSON matching the schema. Be comprehensive, thorough, and CONSISTENT."
        )

        prompt = ChatPromptTemplate.from_messages([
            ("system", system_prompt),
            ("human", 
             "Extract ALL table-column relationships from this stored procedure:\n\n"
             "PROCEDURE NAME: {sp_name}\n"
             "PROCEDURE SCHEMA: {sp_schema}\n"
             "LANGUAGE: {sp_language}\n"
             "HAS DYNAMIC SQL: {has_dynamic}\n\n"
             "VARIABLE MAPPINGS DETECTED:\n{variables}\n\n"
             "TEMPORARY TABLES DETECTED:\n{temp_tables}\n\n"
             "CURSORS DETECTED:\n{cursors}\n\n"
             "STORED PROCEDURE CODE:\n```sql\n{sp_definition}\n```\n\n"
             "Analyze this procedure comprehensively and return ALL relationships found. "
             "Pay special attention to:\n"
             "- Variable resolution for IDENTIFIER() functions\n"
             "- Complex CTE structures\n"
             "- JSON path expressions\n"
             "- Dynamic SQL construction\n"
             "- Cursor operations\n"
             "- MERGE statement complexities\n"
             "- Temporary table creation and usage\n\n"
             "REMEMBER: Be DETERMINISTIC and produce IDENTICAL results for IDENTICAL input!"
            ),
        ])

        structured_llm = llm.with_structured_output(StoredProcedureAnalysis)
        chain = prompt | structured_llm

        logger.info(f"Analyzing {sp_language} procedure '{sp_name}' with {len(variable_mapping)} variables...")
        logger.debug(f"Detected: {len(temp_tables)} temp tables, dynamic SQL: {has_dynamic}")
       
        result = chain.invoke({
            "sp_definition": sp_definition,
            "sp_name": sp_name,
            "sp_schema": sp_schema,
            "sp_language": sp_language,
            "has_dynamic": has_dynamic,
            "variables": json.dumps(variable_mapping, indent=2),
            "temp_tables": json.dumps(temp_tables, indent=2),
            "cursors": json.dumps(cursors, indent=2)
        })
        
        # Sort relationships for consistency
        if result and result.relationships:
            result.relationships = sorted(result.relationships, key=lambda x: (x.table_name, x.column_name))
        
        # Cache the result for future use
        try:
            with open(cache_file, 'w') as f:
                json.dump(result.model_dump(), f, indent=2)
            logger.info(f"Cached analysis for '{sp_name}' (hash: {procedure_hash[:8]})")
        except Exception as e:
            logger.warning(f"Failed to cache analysis for '{sp_name}': {e}")
       
        logger.info(f"Analysis complete for '{sp_name}': {len(result.relationships)} relationships extracted")
        return result
       
    except Exception as e:
        logger.error(f"Analysis failed for '{sp_name}': {e}")
        return None

def consolidate_relationships(relationships: List[TableColumnRelationship]) -> List[TableColumnRelationship]:
    """Consolidate duplicate table-column pairs into single entries with combined relationship types."""
    consolidated = {}
    
    for rel in relationships:
        key = (rel.table_name, rel.column_name)
        
        if key in consolidated:
            # Combine relationship types
            existing_types = set(consolidated[key].relationship_types.split(','))
            new_types = set(rel.relationship_types.split(','))
            combined_types = existing_types.union(new_types)
            # Sort relationship types for consistency
            consolidated[key].relationship_types = ','.join(sorted(combined_types))
        else:
            consolidated[key] = rel
    
    # Sort results by table_name, then column_name for deterministic output
    sorted_results = []
    for key in sorted(consolidated.keys()):
        sorted_results.append(consolidated[key])
    
    return sorted_results
def save_relationships_csv(all_analyses: List[StoredProcedureAnalysis], filename: str, append_mode: bool = False):
    """Save all relationships to CSV with enhanced metadata and consolidated entries."""
    headers = ["SP_NAME", "SP_SCHEMA", "SP_LANGUAGE", "TABLE_NAME", "COLUMN_NAME", "RELATIONSHIP_TYPES"]
    
    # Create backup filename with timestamp
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    backup_filename = f"{filename.replace('.csv', '')}_{timestamp}.csv"
    
    try:
        # Determine file mode
        file_mode = "a" if append_mode and os.path.exists(filename) else "w"
        write_headers = not (append_mode and os.path.exists(filename))
        
        with open(filename, mode=file_mode, newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            
            # Write headers only if new file or not appending
            if write_headers:
                writer.writerow(headers)
            
            total_relationships = 0
            for analysis in all_analyses:
                if analysis:
                    # Consolidate relationships before saving
                    consolidated_relationships = consolidate_relationships(analysis.relationships)
                    total_relationships += len(consolidated_relationships)
                    
                    for rel in consolidated_relationships:
                        writer.writerow([
                            analysis.sp_name.upper(),
                            analysis.sp_schema.upper(),
                            analysis.sp_language.upper(),
                            rel.table_name.upper(),
                            rel.column_name.upper(),
                            rel.relationship_types.upper()
                        ])
        
        mode_text = "appended to" if append_mode else "saved"
        logger.info(f"CSV {mode_text}: {filename} ({total_relationships} total consolidated relationships)")
        
        # Also save backup
        import shutil
        shutil.copy2(filename, backup_filename)
        logger.info(f"Backup saved: {backup_filename}")
        
    except Exception as e:
        logger.error(f"Error saving CSV: {e}")
        # Try to save to backup location
        try:
            with open(backup_filename, mode="w", newline="", encoding="utf-8") as file:
                writer = csv.writer(file)
                writer.writerow(headers)
                
                total_relationships = 0
                for analysis in all_analyses:
                    if analysis:
                        consolidated_relationships = consolidate_relationships(analysis.relationships)
                        total_relationships += len(consolidated_relationships)
                        
                        for rel in consolidated_relationships:
                            writer.writerow([
                                analysis.sp_name.upper(),
                                analysis.sp_schema.upper(),
                                analysis.sp_language.upper(),
                                rel.table_name.upper(),
                                rel.column_name.upper(),
                                rel.relationship_types.upper()
                            ])
            
            logger.info(f"Backup CSV saved: {backup_filename} ({total_relationships} total relationships)")
        except Exception as backup_error:
            logger.error(f"Failed to save backup CSV: {backup_error}")
            raise

# --- 6. Parallel Processing Functions ---
def analyze_single_procedure(procedure_data: Dict) -> Optional[StoredProcedureAnalysis]:
    """Analyze a single stored procedure - wrapper for parallel execution."""
    try:
        return analyze_stored_procedure(
            procedure_data['procedure_definition'],
            procedure_data['procedure_name'],
            procedure_data['procedure_schema']
        )
    except Exception as e:
        logger.error(f"Failed to analyze procedure {procedure_data['procedure_name']}: {e}")
        return None
def analyze_procedures_parallel(procedures: List[Dict], max_workers: int = 4, timeout_per_procedure: int = 300) -> List[StoredProcedureAnalysis]:
    """Analyze multiple stored procedures in parallel for optimal runtime."""
    logger.info(f"Starting parallel analysis of {len(procedures)} procedures with {max_workers} workers")
    logger.info(f"Timeout per procedure: {timeout_per_procedure} seconds")
    
    results = []
    completed_count = 0
    failed_procedures = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_procedure = {
            executor.submit(analyze_single_procedure, proc): proc 
            for proc in procedures
        }
        
        # Process completed tasks with timeout handling
        try:
            for future in as_completed(future_to_procedure, timeout=timeout_per_procedure):
                procedure = future_to_procedure[future]
                completed_count += 1
                
                try:
                    result = future.result(timeout=30)  # 30 second timeout for result retrieval
                    if result:
                        results.append(result)
                        logger.info(f"Completed {completed_count}/{len(procedures)}: {procedure['procedure_name']} - {len(result.relationships)} relationships")
                    else:
                        logger.warning(f"Completed {completed_count}/{len(procedures)}: {procedure['procedure_name']} - FAILED")
                        failed_procedures.append(procedure['procedure_name'])
                except Exception as e:
                    logger.error(f"Exception in procedure {procedure['procedure_name']}: {e}")
                    failed_procedures.append(procedure['procedure_name'])
                    
        except KeyboardInterrupt:
            logger.warning("Analysis interrupted by user. Saving partial results...")
            # Cancel remaining futures
            for future in future_to_procedure:
                future.cancel()
        except Exception as e:
            logger.error(f"Unexpected error during parallel processing: {e}")
    
    logger.info(f"Parallel analysis complete: {len(results)} successful, {len(failed_procedures)} failed")
    if failed_procedures:
        logger.info(f"Failed procedures: {', '.join(failed_procedures)}")
    
    return results

# --- 7. Main Execution Functions ---
def analyze_all_procedures(sf_environment: str = None, max_workers: int = 4, output_file: str = "sp_analysis_results.csv", resume_from_partial: bool = True):
    """Main function to fetch and analyze all stored procedures using existing database infrastructure."""
    # Note: sf_environment parameter is kept for backward compatibility but ignored
    # The environment is now handled by the existing database infrastructure
    start_time = time.time()
    results = []
    
    try:
        logger.info("=== STORED PROCEDURE ANALYSIS STARTED ===")
        logger.info(f"Max workers: {max_workers}")
        logger.info(f"Output file: {output_file}")
        logger.info(f"Resume from partial: {resume_from_partial}")
        
        # Fetch procedures from Snowflake using existing infrastructure
        logger.info("Fetching stored procedures from Snowflake...")
        procedures = fetch_stored_procedures()
        
        if not procedures:
            logger.warning("No stored procedures found!")
            return
        
        # Check for existing results and filter if resuming
        if resume_from_partial:
            completed_procedures = check_existing_results(output_file)
            if completed_procedures:
                procedures = filter_remaining_procedures(procedures, completed_procedures)
                if not procedures:
                    logger.info("All procedures already completed!")
                    return
        
        # Analyze procedures in parallel
        logger.info(f"Analyzing {len(procedures)} stored procedures...")
        try:
            results = analyze_procedures_parallel(procedures, max_workers, timeout_per_procedure=600)
        except KeyboardInterrupt:
            logger.warning("Analysis interrupted by user")
        except Exception as e:
            logger.error(f"Error during parallel analysis: {e}")
        
        # Save results even if incomplete
        if results:
            logger.info(f"Saving {len(results)} results to CSV...")
            # Use append mode if resuming and file exists
            append_mode = resume_from_partial and os.path.exists(output_file)
            save_relationships_csv(results, output_file, append_mode=append_mode)
        else:
            logger.error("No results to save!")
            return
        
        # Generate summary statistics
        total_relationships = sum(len(consolidate_relationships(r.relationships)) for r in results)
        total_tables = len(set(rel.table_name for r in results for rel in consolidate_relationships(r.relationships)))
        
        elapsed_time = time.time() - start_time
        
        logger.info("=== ANALYSIS SUMMARY ===")
        logger.info(f"Procedures processed: {len(results)}/{len(procedures)}")
        logger.info(f"Total relationships: {total_relationships}")
        logger.info(f"Unique tables referenced: {total_tables}")
        logger.info(f"Total execution time: {elapsed_time:.2f} seconds")
        if len(results) > 0:
            logger.info(f"Average time per procedure: {elapsed_time/len(results):.2f} seconds")
        
        # Show relationship type distribution
        rel_types = {}
        for result in results:
            for rel in consolidate_relationships(result.relationships):
                for rel_type in rel.relationship_types.split(','):
                    rel_types[rel_type.strip()] = rel_types.get(rel_type.strip(), 0) + 1
        
        logger.info("Relationship type distribution:")
        for rel_type, count in sorted(rel_types.items()):
            logger.info(f"  {rel_type}: {count}")
        
        logger.info("=== ANALYSIS COMPLETE ===")
    except KeyboardInterrupt:
        logger.warning("Analysis interrupted by user. Saving partial results...")
        if results:
            append_mode = resume_from_partial and os.path.exists(output_file)
            save_relationships_csv(results, output_file, append_mode=append_mode)
            logger.info(f"Partial results saved with {len(results)} procedures")
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        if results:
            logger.info("Attempting to save partial results...")
            append_mode = resume_from_partial and os.path.exists(output_file)
            save_relationships_csv(results, output_file, append_mode=append_mode)
        raise
def check_existing_results(output_file: str) -> List[str]:
    """Check if there are existing results and return list of completed procedures."""
    completed_procedures = []
    
    try:
        if os.path.exists(output_file):
            with open(output_file, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    if row['SP_NAME'] not in completed_procedures:
                        completed_procedures.append(row['SP_NAME'])
            
            logger.info(f"Found existing results with {len(completed_procedures)} completed procedures")
            return completed_procedures
    except Exception as e:
        logger.warning(f"Could not read existing results: {e}")
    
    return completed_procedures

def filter_remaining_procedures(all_procedures: List[Dict], completed_procedures: List[str]) -> List[Dict]:
    """Filter out already completed procedures."""
    remaining = []
    for proc in all_procedures:
        if proc['procedure_name'].upper() not in [cp.upper() for cp in completed_procedures]:
            remaining.append(proc)
    
    logger.info(f"Filtered procedures: {len(remaining)} remaining out of {len(all_procedures)} total")
    return remaining

# --- 8. Main Entry Point ---
if __name__ == "__main__":
    # Configuration
    MAX_WORKERS = 4 
    OUTPUT_FILE = "sp_analysis_complete.csv"
    
    try:
        # Run the complete analysis with better error handling
        analyze_all_procedures(
            max_workers=MAX_WORKERS,
            output_file=OUTPUT_FILE,
            resume_from_partial=True
        )
    except KeyboardInterrupt:
        logger.info("Script interrupted by user. Check for partial results in CSV files.")
    except Exception as e:
        logger.error(f"Script failed with error: {e}")
        logger.info("Check log files and partial results for debugging.")
