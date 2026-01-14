"""
SQL Metadata Parser - Simple Version
Efficiently parses SQL stored procedures and extracts metadata matching the refined schema structure.
Outputs to 4 separate CSV files (no pandas dependency): entities.csv, relationship_types.csv, relationships.csv, operations.csv
"""

import logging
import uuid
import csv
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Any
from pathlib import Path
from datetime import datetime
import json
import sqlglot
from sqlglot import exp

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class Entity:
    """Entity matching the entities table schema"""
    entity_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    entity_name: str = ""
    entity_type: str = ""  # TABLE, COLUMN, VIEW, PROCEDURE, BACKEND, THOUGHTSPOT_ASSET
    qualified_name: str = ""
    parent_entity_id: Optional[str] = None
    description: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class RelationshipType:
    """Relationship type matching relationship_types table schema"""
    relationship_type_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    category: str = ""  # STRUCTURE, USAGE, TRANSFORMATION, CONSUMPTION
    description: str = ""


@dataclass
class Relationship:
    """Relationship matching relationships table schema"""
    relationship_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    source_entity_id: str = ""
    target_entity_id: str = ""
    relationship_type_id: str = ""
    relationship_detail: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class Operation:
    """Operation matching operations table schema"""
    operation_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    operation_type: str = ""  # SELECT, UPDATE, INSERT, DELETE, EXECUTE
    source_entity_id: str = ""
    target_entity_id: Optional[str] = None
    performed_at: str = field(default_factory=lambda: datetime.now().isoformat())
    performed_by: Optional[str] = None
    sql_snippet: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class SQLMetadataParser:
    """Main class for extracting SQLz metadata and lineage information"""
    
    def __init__(self):
        self.entities: Dict[str, Entity] = {}
        self.relationship_types: Dict[str, RelationshipType] = {}
        self.relationships: List[Relationship] = []
        self.operations: List[Operation] = []
        self.entity_lookup: Dict[str, str] = {}  # qualified_name -> entity_id
        
        # Initialize standard relationship types
        self._init_relationship_types()
    
    def _init_relationship_types(self):
        """Initialize standard relationship types with categories"""
        standard_types = [
            # STRUCTURE category - schema relationships
            ("CONTAINS_COLUMN", "STRUCTURE", "Entity contains a column"),
            ("CONTAINS_PARAMETER", "STRUCTURE", "Procedure contains a parameter"),
            ("SCHEMA_CONTAINS_TABLE", "STRUCTURE", "Schema contains a table"),
            ("DATABASE_CONTAINS_SCHEMA", "STRUCTURE", "Database contains a schema"),
            
            # USAGE category - data access relationships
            ("READS_FROM", "USAGE", "Entity reads data from another entity"),
            ("WRITES_TO", "USAGE", "Entity writes data to another entity"),
            ("JOINS_WITH", "USAGE", "Table joins with another table"),
            ("REFERENCES", "USAGE", "Entity references another entity"),
            
            # TRANSFORMATION category - data transformation relationships
            ("DERIVES_FROM", "TRANSFORMATION", "Entity derives data from another entity"),
            ("AGGREGATES", "TRANSFORMATION", "Entity aggregates data from another entity"),
            ("FILTERS", "TRANSFORMATION", "Entity filters data from another entity"),
            ("TRANSFORMS", "TRANSFORMATION", "Entity transforms data from another entity"),
            
            # CONSUMPTION category - external usage
            ("BACKEND_INVOKES", "CONSUMPTION", "Backend service invokes entity"),
            ("THOUGHTSPOT_USES", "CONSUMPTION", "ThoughtSpot asset uses entity"),
            ("API_EXPOSES", "CONSUMPTION", "API exposes entity data"),
            ("REPORT_USES", "CONSUMPTION", "Report uses entity data")
        ]
        
        for name, category, description in standard_types:
            rel_type = RelationshipType(
                name=name,
                category=category,
                description=description
            )
            self.relationship_types[name] = rel_type
    
    def _get_or_create_entity(self, qualified_name: str, entity_type: str, 
                             entity_name: str = "", parent_id: Optional[str] = None, 
                             description: str = "", **metadata) -> str:
        """Get existing entity or create new one"""
        if qualified_name in self.entity_lookup:
            return self.entity_lookup[qualified_name]
        
        # Extract entity name if not provided
        if not entity_name:
            entity_name = qualified_name.split('.')[-1]
        
        entity = Entity(
            entity_name=entity_name,
            entity_type=entity_type,
            qualified_name=qualified_name,
            parent_entity_id=parent_id,
            description=description,
            metadata=metadata
        )
        
        self.entities[entity.entity_id] = entity
        self.entity_lookup[qualified_name] = entity.entity_id
        return entity.entity_id
    
    def _add_relationship(self, source_id: str, target_id: str, rel_type_name: str, 
                         detail: str = "", **metadata):
        """Add a relationship between entities"""
        if rel_type_name not in self.relationship_types:
            logger.warning(f"Unknown relationship type: {rel_type_name}")
            return
        
        relationship = Relationship(
            source_entity_id=source_id,
            target_entity_id=target_id,
            relationship_type_id=self.relationship_types[rel_type_name].relationship_type_id,
            relationship_detail=detail,
            metadata=metadata
        )
        self.relationships.append(relationship)
    
    def _add_operation(self, op_type: str, source_id: str, target_id: Optional[str] = None,
                      performed_by: str = "SQL_PARSER", sql_snippet: str = "", **metadata):
        """Add an operation record"""
        operation = Operation(
            operation_type=op_type,
            source_entity_id=source_id,
            target_entity_id=target_id,
            performed_by=performed_by,
            sql_snippet=sql_snippet,
            metadata=metadata
        )
        self.operations.append(operation)
    
    def _extract_table_references(self, node: exp.Expression) -> Set[str]:
        """Extract all table references from an AST node"""
        tables = set()
        
        for table_node in node.find_all(exp.Table):
            table_name = self._get_qualified_name(table_node)
            if table_name:
                tables.add(table_name)
        
        return tables
    
    def _get_qualified_name(self, table_node: exp.Table) -> Optional[str]:
        """Get qualified table name from table node"""
        if not table_node:
            return None
        
        parts = []
        if table_node.catalog:
            parts.append(table_node.catalog)
        if table_node.db:
            parts.append(table_node.db)
        if table_node.name:
            parts.append(table_node.name)
        
        return '.'.join(parts) if parts else None
    
    def _analyze_select_statement(self, select_node: exp.Select, context: str = ""):
        """Analyze SELECT statement and extract comprehensive lineage"""
        # Extract source tables
        source_tables = self._extract_table_references(select_node)
        source_ids = []
        
        for table_name in source_tables:
            table_id = self._get_or_create_entity(table_name, "TABLE")
            source_ids.append(table_id)
        
        # Analyze ALL columns (SELECT, WHERE, GROUP BY, ORDER BY, etc.)
        all_columns = self._extract_all_columns(select_node)
        for col_info in all_columns:
            col_qualified = col_info['qualified_name']
            col_id = self._get_or_create_entity(col_qualified, "COLUMN")
            
            # Link column to its parent table
            if col_info['table_name']:
                table_id = self._get_or_create_entity(col_info['table_name'], "TABLE")
                self._add_relationship(table_id, col_id, "CONTAINS_COLUMN")
        
        # Analyze JOINs with detailed conditions
        for join in select_node.find_all(exp.Join):
            if join.this and isinstance(join.this, exp.Table):
                joined_table = self._get_qualified_name(join.this)
                if joined_table:
                    joined_table_id = self._get_or_create_entity(joined_table, "TABLE")
                    
                    for source_id in source_ids:
                        join_condition = str(join.on) if join.on else ""
                        join_type = join.kind if join.kind else "INNER"
                        
                        self._add_relationship(
                            source_id, joined_table_id, "JOINS_WITH",
                            detail=f"{join_type} JOIN",
                            join_condition=join_condition
                        )
        
        # Analyze WHERE clause with column dependencies
        if select_node.where:
            where_columns = self._extract_columns_from_expression(select_node.where)
            for col_name in where_columns:
                col_id = self._get_or_create_entity(col_name, "COLUMN")
                for source_id in source_ids:
                    self._add_relationship(
                        source_id, col_id, "FILTERS",
                        detail="WHERE clause",
                        filter_condition=str(select_node.where)
                    )
        
        # Analyze GROUP BY
        if select_node.group:
            group_columns = self._extract_columns_from_expression(select_node.group)
            for col_name in group_columns:
                col_id = self._get_or_create_entity(col_name, "COLUMN")
                for source_id in source_ids:
                    self._add_relationship(
                        source_id, col_id, "AGGREGATES",
                        detail="GROUP BY",
                        group_by_columns=str(select_node.group)
                    )
        
        # Analyze functions (window, aggregate, scalar)
        self._analyze_functions(select_node, source_ids, context)
        
        # Record SELECT operations
        for source_id in source_ids:
            self._add_operation(
                "SELECT", source_id,
                sql_snippet=str(select_node)[:500],
                context=context
            )
    
    def _analyze_insert_statement(self, insert_node: exp.Insert, context: str = ""):
        """Analyze INSERT statement"""
        if insert_node.this:
            target_table = self._get_qualified_name(insert_node.this)
            if target_table:
                target_id = self._get_or_create_entity(target_table, "TABLE")
                
                # Check for INSERT ... SELECT
                if insert_node.expression and isinstance(insert_node.expression, exp.Select):
                    source_tables = self._extract_table_references(insert_node.expression)
                    for source_table in source_tables:
                        source_id = self._get_or_create_entity(source_table, "TABLE")
                        self._add_relationship(
                            source_id, target_id, "WRITES_TO",
                            detail="INSERT ... SELECT"
                        )
                        
                        # Record operation
                        self._add_operation("SELECT", source_id, target_id, sql_snippet=str(insert_node.expression))
                
                # Record INSERT operation
                self._add_operation("INSERT", target_id, sql_snippet=str(insert_node), context=context)
    
    def _analyze_update_statement(self, update_node: exp.Update, context: str = ""):
        """Analyze UPDATE statement"""
        if update_node.this:
            target_table = self._get_qualified_name(update_node.this)
            if target_table:
                target_id = self._get_or_create_entity(target_table, "TABLE")
                self._add_operation("UPDATE", target_id, sql_snippet=str(update_node), context=context)
    
    def _analyze_delete_statement(self, delete_node: exp.Delete, context: str = ""):
        """Analyze DELETE statement"""
        if delete_node.this:
            target_table = self._get_qualified_name(delete_node.this)
            if target_table:
                target_id = self._get_or_create_entity(target_table, "TABLE")
                self._add_operation("DELETE", target_id, sql_snippet=str(delete_node), context=context)
    
    def _analyze_create_statement(self, create_node: exp.Create, context: str = ""):
        """Analyze CREATE statement (tables, views, procedures)"""
        if create_node.kind == "TABLE" and create_node.this:
            table_name = str(create_node.this.this) if hasattr(create_node.this, 'this') else str(create_node.this)
            table_id = self._get_or_create_entity(table_name, "TABLE", description="Created table")
            
            # Extract columns if schema is defined
            if hasattr(create_node.this, 'expressions'):
                for column_def in create_node.this.expressions:
                    if isinstance(column_def, exp.ColumnDef):
                        column_name = f"{table_name}.{column_def.this}"
                        col_id = self._get_or_create_entity(column_name, "COLUMN", parent_id=table_id)
                        self._add_relationship(table_id, col_id, "CONTAINS_COLUMN")
            
            self._add_operation("CREATE", table_id, sql_snippet=str(create_node), context=context)
        
        elif create_node.kind == "VIEW" and create_node.this:
            view_name = str(create_node.this.this) if hasattr(create_node.this, 'this') else str(create_node.this)
            view_id = self._get_or_create_entity(view_name, "VIEW", description="Created view")
            
            # Analyze view definition
            if create_node.expression:
                source_tables = self._extract_table_references(create_node.expression)
                for source_table in source_tables:
                    source_id = self._get_or_create_entity(source_table, "TABLE")
                    self._add_relationship(source_id, view_id, "DERIVES_FROM", detail="View definition")
            
            self._add_operation("CREATE", view_id, sql_snippet=str(create_node), context=context)
    
    def _analyze_cte(self, with_node: exp.With, context: str = ""):
        """Analyze Common Table Expressions"""
        for cte in with_node.expressions:
            if isinstance(cte, exp.CTE):
                cte_name = str(cte.alias)
                cte_id = self._get_or_create_entity(cte_name, "VIEW", description="Common Table Expression")
                
                if cte.this:
                    source_tables = self._extract_table_references(cte.this)
                    for source_table in source_tables:
                        source_id = self._get_or_create_entity(source_table, "TABLE")
                        self._add_relationship(source_id, cte_id, "DERIVES_FROM", detail="CTE definition")
    
    def _analyze_statement(self, statement: exp.Expression, context: str = ""):
        """Analyze a single SQL statement"""
        try:
            if isinstance(statement, exp.Select):
                self._analyze_select_statement(statement, context)
            elif isinstance(statement, exp.Insert):
                self._analyze_insert_statement(statement, context)
            elif isinstance(statement, exp.Update):
                self._analyze_update_statement(statement, context)
            elif isinstance(statement, exp.Delete):
                self._analyze_delete_statement(statement, context)
            elif isinstance(statement, exp.Create):
                self._analyze_create_statement(statement, context)
            elif isinstance(statement, exp.With):
                self._analyze_cte(statement, context)
            
            # Handle nested WITH clauses
            for with_node in statement.find_all(exp.With):
                self._analyze_cte(with_node, context)
            
            # Handle subqueries recursively
            for subquery in statement.find_all(exp.Subquery):
                if subquery.this:
                    self._analyze_statement(subquery.this, f"{context}_subquery")
                    
        except Exception as e:
            logger.error(f"Error analyzing statement in {context}: {e}")
    
    def _extract_all_columns(self, node: exp.Expression) -> List[Dict[str, str]]:
        """Extract all column references with their context"""
        columns = []
        
        for col_node in node.find_all(exp.Column):
            table_name = col_node.table if col_node.table else "unknown"
            col_name = col_node.name if col_node.name else str(col_node)
            qualified_name = f"{table_name}.{col_name}" if table_name != "unknown" else col_name
            
            columns.append({
                'qualified_name': qualified_name,
                'column_name': col_name,
                'table_name': table_name if table_name != "unknown" else None
            })
        
        return columns
    
    def _extract_columns_from_expression(self, expr: exp.Expression) -> Set[str]:
        """Extract column names from any expression"""
        columns = set()
        for col_node in expr.find_all(exp.Column):
            table_name = col_node.table if col_node.table else "unknown"
            col_name = col_node.name if col_node.name else str(col_node)
            qualified_name = f"{table_name}.{col_name}" if table_name != "unknown" else col_name
            columns.add(qualified_name)
        return columns
    
    def _analyze_functions(self, node: exp.Expression, source_ids: List[str], context: str):
        """Analyze all types of functions in the query"""
        # Window functions
        window_functions = ["ROW_NUMBER", "RANK", "DENSE_RANK", "LEAD", "LAG", "NTILE", "FIRST_VALUE", "LAST_VALUE"]
        
        # Aggregate functions
        aggregate_functions = ["SUM", "COUNT", "AVG", "MIN", "MAX", "MEDIAN", "STDDEV", "VARIANCE"]
        
        # String functions
        string_functions = ["CONCAT", "SUBSTRING", "UPPER", "LOWER", "TRIM", "LENGTH"]
        
        # Date functions
        date_functions = ["CURRENT_TIMESTAMP", "CURRENT_DATE", "DATE_TRUNC", "DATEADD", "DATEDIFF"]
        
        # Find all function calls
        for func_node in node.find_all(exp.Anonymous):
            func_name = func_node.this.upper() if hasattr(func_node, 'this') else str(func_node).upper()
            
            if func_name in window_functions:
                for source_id in source_ids:
                    self._add_operation(
                        "WINDOW_FUNCTION", source_id,
                        sql_snippet=str(func_node),
                        function_name=func_name,
                        context=context
                    )
            elif func_name in aggregate_functions:
                for source_id in source_ids:
                    self._add_operation(
                        "AGGREGATE", source_id,
                        sql_snippet=str(func_node),
                        function_name=func_name,
                        context=context
                    )
            elif func_name in string_functions:
                for source_id in source_ids:
                    self._add_operation(
                        "STRING_FUNCTION", source_id,
                        sql_snippet=str(func_node),
                        function_name=func_name,
                        context=context
                    )
            elif func_name in date_functions:
                for source_id in source_ids:
                    self._add_operation(
                        "DATE_FUNCTION", source_id,
                        sql_snippet=str(func_node),
                        function_name=func_name,
                        context=context
                    )
    
    def _analyze_variables_and_parameters(self, sql_content: str, context: str):
        """Analyze variables, parameters, and SET statements"""
        lines = sql_content.split('\n')
        
        for line_num, line in enumerate(lines):
            line = line.strip()
            
            # Analyze SET statements
            if line.upper().startswith('SET '):
                var_name = self._extract_variable_name(line)
                if var_name:
                    var_id = self._get_or_create_entity(var_name, "VARIABLE", description="SQL Variable")
                    self._add_operation(
                        "SET", var_id,
                        sql_snippet=line,
                        context=f"{context}_line_{line_num}",
                        line_number=line_num
                    )
            
            # Analyze parameter references ($PARAM)
            import re
            param_pattern = r'\$([A-Za-z_][A-Za-z0-9_]*)'
            params = re.findall(param_pattern, line)
            for param in params:
                param_id = self._get_or_create_entity(f"${param}", "PARAMETER", description="SQL Parameter")
                self._add_operation(
                    "PARAMETER_REFERENCE", param_id,
                    sql_snippet=line,
                    context=f"{context}_line_{line_num}",
                    line_number=line_num
                )
    
    def _extract_variable_name(self, set_statement: str) -> Optional[str]:
        """Extract variable name from SET statement"""
        import re
        match = re.match(r'SET\s+([A-Za-z_][A-Za-z0-9_]*)', set_statement, re.IGNORECASE)
        return match.group(1) if match else None
    
    def _analyze_procedure_structure(self, sql_content: str, context: str):
        """Analyze stored procedure structure and metadata"""
        lines = sql_content.split('\n')
        
        # Extract comments and procedure metadata
        comments = []
        for line_num, line in enumerate(lines):
            line = line.strip()
            if line.startswith('--'):
                comments.append({
                    'line_number': line_num,
                    'comment': line[2:].strip(),
                    'type': 'single_line'
                })
        
        # Create procedure entity if this looks like a procedure
        if comments or any('SET ' in line.upper() for line in lines[:10]):
            proc_name = context.replace('_stmt_0', '') if '_stmt_0' in context else context
            proc_id = self._get_or_create_entity(
                proc_name, "PROCEDURE", 
                description=f"Stored procedure with {len(comments)} comments",
                comment_count=len(comments),
                total_lines=len(lines)
            )
            
            # Link comments to procedure
            for comment in comments:
                comment_id = self._get_or_create_entity(
                    f"{proc_name}_comment_{comment['line_number']}", "COMMENT",
                    parent_id=proc_id,
                    description=comment['comment']
                )
                self._add_relationship(proc_id, comment_id, "CONTAINS_PARAMETER")
    
    def parse_sql_content(self, sql_content: str, source_context: str = ""):
        """Parse SQL content and extract comprehensive metadata"""
        try:
            # First, analyze procedure structure and variables
            self._analyze_procedure_structure(sql_content, source_context)
            self._analyze_variables_and_parameters(sql_content, source_context)
            
            # Parse SQL using sqlglot with Snowflake dialect
            statements = sqlglot.parse(sql_content, dialect="snowflake")
            
            for i, statement in enumerate(statements):
                if statement:
                    context = f"{source_context}_stmt_{i}" if source_context else f"stmt_{i}"
                    self._analyze_statement(statement, context)
                    
        except Exception as e:
            logger.error(f"Error parsing SQL content from {source_context}: {e}")
            # Fallback: try to extract basic information even if parsing fails
            self._fallback_analysis(sql_content, source_context)
    
    def _fallback_analysis(self, sql_content: str, source_context: str):
        """Fallback analysis when sqlglot parsing fails"""
        import re
        
        # Extract table names using regex
        table_patterns = [
            r'FROM\s+([A-Za-z_][A-Za-z0-9_]*)',
            r'JOIN\s+([A-Za-z_][A-Za-z0-9_]*)',
            r'UPDATE\s+([A-Za-z_][A-Za-z0-9_]*)',
            r'INSERT\s+INTO\s+([A-Za-z_][A-Za-z0-9_]*)',
            r'DELETE\s+FROM\s+([A-Za-z_][A-Za-z0-9_]*)',
        ]
        
        for pattern in table_patterns:
            matches = re.findall(pattern, sql_content, re.IGNORECASE)
            for table_name in matches:
                table_id = self._get_or_create_entity(table_name, "TABLE")
                self._add_operation(
                    "REFERENCE", table_id,
                    sql_snippet=f"Fallback extraction: {table_name}",
                    context=f"{source_context}_fallback"
                )
    
    def parse_sql_file(self, file_path: str):
        """Parse a single SQL file with enhanced analysis"""
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                sql_content = file.read()
            
            file_name = Path(file_path).stem
            
            # Create a file entity
            file_id = self._get_or_create_entity(
                file_name, "SQL_FILE",
                description=f"SQL file: {file_path}",
                file_path=str(file_path),
                file_size=len(sql_content)
            )
            
            self.parse_sql_content(sql_content, file_name)
            logger.info(f"Successfully parsed {file_path} ({len(sql_content)} characters)")
            
        except Exception as e:
            logger.error(f"Error parsing file {file_path}: {e}")
    
    def _extract_all_columns(self, node: exp.Expression) -> List[Dict[str, str]]:
        """Extract all column references with their context"""
        columns = []
        
        for col_node in node.find_all(exp.Column):
            table_name = col_node.table if col_node.table else "unknown"
            col_name = col_node.name if col_node.name else str(col_node)
            qualified_name = f"{table_name}.{col_name}" if table_name != "unknown" else col_name
            
            columns.append({
                'qualified_name': qualified_name,
                'column_name': col_name,
                'table_name': table_name if table_name != "unknown" else None
            })
        
        return columns
    
    def _extract_columns_from_expression(self, expr: exp.Expression) -> Set[str]:
        """Extract column names from any expression"""
        columns = set()
        for col_node in expr.find_all(exp.Column):
            table_name = col_node.table if col_node.table else "unknown"
            col_name = col_node.name if col_node.name else str(col_node)
            qualified_name = f"{table_name}.{col_name}" if table_name != "unknown" else col_name
            columns.add(qualified_name)
        return columns
    
    def _analyze_functions(self, node: exp.Expression, source_ids: List[str], context: str):
        """Analyze all types of functions in the query"""
        # Window functions
        window_functions = ["ROW_NUMBER", "RANK", "DENSE_RANK", "LEAD", "LAG", "NTILE", "FIRST_VALUE", "LAST_VALUE"]
        
        # Aggregate functions
        aggregate_functions = ["SUM", "COUNT", "AVG", "MIN", "MAX", "MEDIAN", "STDDEV", "VARIANCE"]
        
        # Find all function calls
        for func_node in node.find_all(exp.Anonymous):
            func_name = func_node.this.upper() if hasattr(func_node, 'this') else str(func_node).upper()
            
            if func_name in window_functions:
                for source_id in source_ids:
                    self._add_operation(
                        "WINDOW_FUNCTION", source_id,
                        sql_snippet=str(func_node),
                        function_name=func_name,
                        context=context
                    )
            elif func_name in aggregate_functions:
                for source_id in source_ids:
                    self._add_operation(
                        "AGGREGATE", source_id,
                        sql_snippet=str(func_node),
                        function_name=func_name,
                        context=context
                    )
    
    def _analyze_variables_and_parameters(self, sql_content: str, context: str):
        """Analyze variables and parameters - store as PROCEDURE metadata"""
        lines = sql_content.split('\n')
        variables = []
        parameters = []
        
        for line_num, line in enumerate(lines):
            line = line.strip()
            
            # Analyze SET statements
            if line.upper().startswith('SET '):
                var_name = self._extract_variable_name(line)
                if var_name:
                    variables.append({
                        'name': var_name,
                        'line': line_num,
                        'statement': line
                    })
            
            # Analyze parameter references ($PARAM)
            import re
            param_pattern = r'\$([A-Za-z_][A-Za-z0-9_]*)'
            params = re.findall(param_pattern, line)
            for param in params:
                parameters.append({
                    'name': param,
                    'line': line_num,
                    'context': line
                })
        
        # Create procedure entity with variables and parameters as metadata
        if variables or parameters or len(lines) > 10:  # Any substantial SQL file
            proc_name = context.replace('_stmt_0', '') if '_stmt_0' in context else context
            proc_id = self._get_or_create_entity(
                proc_name, "PROCEDURE", 
                description=f"Stored procedure with {len(variables)} variables and {len(parameters)} parameters",
                variables=variables,
                parameters=parameters,
                total_lines=len(lines),
                comments_count=len([l for l in lines if l.strip().startswith('--')])
            )
            
            # Record operations for variables and parameters
            for var in variables:
                self._add_operation(
                    "SET", proc_id,
                    sql_snippet=var['statement'],
                    context=f"{context}_var_{var['name']}",
                    variable_name=var['name']
                )
            
            for param in parameters:
                self._add_operation(
                    "PARAMETER_REFERENCE", proc_id,
                    sql_snippet=param['context'],
                    context=f"{context}_param_{param['name']}",
                    parameter_name=param['name']
                )
    
    def _extract_variable_name(self, set_statement: str) -> Optional[str]:
        """Extract variable name from SET statement"""
        import re
        match = re.match(r'SET\s+([A-Za-z_][A-Za-z0-9_]*)', set_statement, re.IGNORECASE)
        return match.group(1) if match else None
    
    def _fallback_analysis(self, sql_content: str, source_context: str):
        """Fallback analysis when sqlglot parsing fails"""
        import re
        
        # Extract table names using regex
        table_patterns = [
            r'FROM\s+([A-Za-z_][A-Za-z0-9_]*)',
            r'JOIN\s+([A-Za-z_][A-Za-z0-9_]*)',
            r'UPDATE\s+([A-Za-z_][A-Za-z0-9_]*)',
            r'INSERT\s+INTO\s+([A-Za-z_][A-Za-z0-9_]*)',
            r'DELETE\s+FROM\s+([A-Za-z_][A-Za-z0-9_]*)',
        ]
        
        for pattern in table_patterns:
            matches = re.findall(pattern, sql_content, re.IGNORECASE)
            for table_name in matches:
                table_id = self._get_or_create_entity(table_name, "TABLE")
                self._add_operation(
                    "REFERENCE", table_id,
                    sql_snippet=f"Fallback extraction: {table_name}",
                    context=f"{source_context}_fallback"
                )
    
    def parse_directory(self, directory_path: str, file_pattern: str = "*.sql"):
        """Parse all SQL files in a directory"""
        path = Path(directory_path)
        sql_files = list(path.glob(file_pattern))
        
        logger.info(f"Found {len(sql_files)} SQL files to parse")
        
        for sql_file in sql_files:
            self.parse_sql_file(str(sql_file))
    
    def export_to_csv_files(self, output_dir: str = "output"):
        """Export metadata to 4 separate CSV files matching the schema"""
        try:
            # Create output directory if it doesn't exist
            output_path = Path(output_dir)
            output_path.mkdir(exist_ok=True)
            
            # 1. Entities CSV file
            entities_file = output_path / "entities.csv"
            with open(entities_file, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['entity_id', 'entity_name', 'entity_type', 'qualified_name', 
                             'parent_entity_id', 'description', 'metadata', 'created_at', 'updated_at']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for entity in self.entities.values():
                    writer.writerow({
                        'entity_id': entity.entity_id,
                        'entity_name': entity.entity_name,
                        'entity_type': entity.entity_type,
                        'qualified_name': entity.qualified_name,
                        'parent_entity_id': entity.parent_entity_id,
                        'description': entity.description,
                        'metadata': json.dumps(entity.metadata) if entity.metadata else None,
                        'created_at': entity.created_at,
                        'updated_at': entity.updated_at
                    })
            logger.info(f"Exported entities to {entities_file}")
            
            # 2. Relationship Types CSV file
            rel_types_file = output_path / "relationship_types.csv"
            with open(rel_types_file, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['relationship_type_id', 'name', 'category', 'description']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for rel_type in self.relationship_types.values():
                    writer.writerow({
                        'relationship_type_id': rel_type.relationship_type_id,
                        'name': rel_type.name,
                        'category': rel_type.category,
                        'description': rel_type.description
                    })
            logger.info(f"Exported relationship types to {rel_types_file}")
            
            # 3. Relationships CSV file
            relationships_file = output_path / "relationships.csv"
            with open(relationships_file, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['relationship_id', 'source_entity_id', 'target_entity_id', 
                             'relationship_type_id', 'relationship_detail', 'metadata', 'created_at']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for rel in self.relationships:
                    writer.writerow({
                        'relationship_id': rel.relationship_id,
                        'source_entity_id': rel.source_entity_id,
                        'target_entity_id': rel.target_entity_id,
                        'relationship_type_id': rel.relationship_type_id,
                        'relationship_detail': rel.relationship_detail,
                        'metadata': json.dumps(rel.metadata) if rel.metadata else None,
                        'created_at': rel.created_at
                    })
            logger.info(f"Exported relationships to {relationships_file}")
            
            # 4. Operations CSV file
            operations_file = output_path / "operations.csv"
            with open(operations_file, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['operation_id', 'operation_type', 'source_entity_id', 'target_entity_id',
                             'performed_at', 'performed_by', 'sql_snippet', 'metadata']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for op in self.operations:
                    writer.writerow({
                        'operation_id': op.operation_id,
                        'operation_type': op.operation_type,
                        'source_entity_id': op.source_entity_id,
                        'target_entity_id': op.target_entity_id,
                        'performed_at': op.performed_at,
                        'performed_by': op.performed_by,
                        'sql_snippet': op.sql_snippet,
                        'metadata': json.dumps(op.metadata) if op.metadata else None
                    })
            logger.info(f"Exported operations to {operations_file}")
            
            logger.info(f"Successfully exported all metadata to {output_dir}/ directory")
            
        except Exception as e:
            logger.error(f"Error exporting to CSV files: {e}")
    
    def get_summary_stats(self) -> Dict[str, int]:
        """Get summary statistics of extracted metadata"""
        entity_types = {}
        for entity in self.entities.values():
            entity_types[entity.entity_type] = entity_types.get(entity.entity_type, 0) + 1
        
        relationship_categories = {}
        for rel_type in self.relationship_types.values():
            relationship_categories[rel_type.category] = relationship_categories.get(rel_type.category, 0) + 1
        
        operation_types = {}
        for op in self.operations:
            operation_types[op.operation_type] = operation_types.get(op.operation_type, 0) + 1
        
        return {
            'total_entities': len(self.entities),
            'total_relationships': len(self.relationships),
            'total_operations': len(self.operations),
            'total_relationship_types': len(self.relationship_types),
            'entity_breakdown': entity_types,
            'relationship_categories': relationship_categories,
            'operation_breakdown': operation_types
        }


def main():
    """Main function to run SQL metadata parsing"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python sql_parser_simple.py <sql_files_directory> [output_directory]")
        print("Example: python sql_parser_simple.py ./sql_files ./output")
        return
    
    # Get input directory from command line
    input_directory = sys.argv[1]
    output_directory = sys.argv[2] if len(sys.argv) > 2 else "output"
    
    # Validate input directory exists
    if not Path(input_directory).exists():
        logger.error(f"Input directory does not exist: {input_directory}")
        return
    
    # Initialize parser
    parser = SQLMetadataParser()
    
    # Parse all SQL files in the directory
    logger.info(f"Starting SQL metadata parsing from: {input_directory}")
    parser.parse_directory(input_directory)
    
    # Print summary statistics
    stats = parser.get_summary_stats()
    print("\n" + "="*50)
    print("SQL METADATA PARSING SUMMARY")
    print("="*50)
    print(f"Total Entities: {stats['total_entities']}")
    print(f"Total Relationships: {stats['total_relationships']}")
    print(f"Total Operations: {stats['total_operations']}")
    print(f"Total Relationship Types: {stats['total_relationship_types']}")
    
    print("\nEntity Breakdown:")
    for entity_type, count in stats['entity_breakdown'].items():
        print(f"  {entity_type}: {count}")
    
    print("\nRelationship Categories:")
    for category, count in stats['relationship_categories'].items():
        print(f"  {category}: {count}")
    
    print("\nOperation Breakdown:")
    for op_type, count in stats['operation_breakdown'].items():
        print(f"  {op_type}: {count}")
    
    # Export to separate CSV files
    parser.export_to_csv_files(output_directory)
    print(f"\nAnalysis complete! Results exported to {output_directory}/ directory")
    print("Generated files:")
    print(f"  - {output_directory}/entities.csv")
    print(f"  - {output_directory}/relationship_types.csv")
    print(f"  - {output_directory}/relationships.csv")
    print(f"  - {output_directory}/operations.csv")


if __name__ == "__main__":
    main()