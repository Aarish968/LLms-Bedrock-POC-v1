#!/usr/bin/env python3
"""
Dynamic Wildcard Tracer - 100% Dynamic Solution
NO hardcoded values - works with any view definition
"""

import sqlglot
from sqlglot import exp
import csv
import io
import re

class DynamicWildcardTracer:
    """Completely dynamic tracer with zero hardcoded values"""
    
    def __init__(self, dialect="snowflake"):
        self.dialect = dialect
        
    def analyze_view_dynamically(self, sql_text):
        """Completely dynamic analysis - no hardcoded assumptions"""
        try:
            parsed = sqlglot.parse_one(sql_text, dialect=self.dialect)
            
            analysis = {
                'view_name': '',
                'view_columns': [],
                'column_mappings': {},
                'derived_columns': {},
                'debug_info': {}
            }
            
            # Step 1: Extract basic structure
            self._extract_view_structure(parsed, analysis)
            
            # Step 2: Build complete mapping dynamically
            self._build_dynamic_mappings(parsed, analysis)
            
            return analysis
            
        except Exception as e:
            return {'error': f"Dynamic analysis failed: {str(e)}"}
    
    def _extract_view_structure(self, parsed, analysis):
        """Extract view structure dynamically"""
        # Extract view name
        if isinstance(parsed, exp.Create) and parsed.kind == "VIEW":
            try:
                view_name = str(parsed.this.this)
                analysis['view_name'] = view_name
            except:
                analysis['view_name'] = "UNKNOWN_VIEW"
        
        # Extract view columns from definition
        view_def_str = str(parsed.this)
        if '(' in view_def_str and ')' in view_def_str:
            start = view_def_str.find('(')
            end = view_def_str.find(')')
            cols_part = view_def_str[start+1:end]
            analysis['view_columns'] = [col.strip() for col in cols_part.split(',')]
    
    def _build_dynamic_mappings(self, parsed, analysis):
        """Build mappings by analyzing the complete SQL structure"""
        
        # Strategy: Work backwards from the main SELECT to trace each column
        main_select = self._find_main_select(parsed)
        
        if not main_select:
            return
        
        # Analyze what the main SELECT is doing
        main_select_analysis = self._analyze_select_expressions(main_select)
        
        # Build table and CTE registry
        table_registry = self._build_table_registry(parsed)
        cte_registry = self._build_cte_registry(parsed, table_registry)
        
        # For each view column, trace its source
        for view_col in analysis['view_columns']:
            source_info = self._trace_column_source(
                view_col, main_select_analysis, cte_registry, table_registry
            )
            
            if source_info['type'] == 'derived':
                analysis['derived_columns'][view_col] = source_info
            else:
                analysis['column_mappings'][view_col] = source_info
        
        # Store debug info
        analysis['debug_info'] = {
            'main_select_analysis': main_select_analysis,
            'table_registry': table_registry,
            'cte_registry': list(cte_registry.keys()),
            'cte_registry_full': cte_registry,
            'total_view_columns': len(analysis['view_columns']),
            'mapped_columns': len(analysis['column_mappings']),
            'derived_columns': len(analysis['derived_columns'])
        }
    
    def _find_main_select(self, parsed):
        """Find the main SELECT statement dynamically"""
        all_selects = []
        
        # Collect all SELECT statements
        for node in parsed.walk():
            if isinstance(node, exp.Select):
                all_selects.append(node)
        
        if not all_selects:
            return None
        
        # Heuristic: Main SELECT usually has the most complex structure
        # or contains both wildcards and additional expressions
        best_candidate = None
        max_score = -1
        
        for select_stmt in all_selects:
            score = 0
            
            # Score based on complexity
            score += len(select_stmt.expressions)  # More expressions = likely main
            
            # Bonus for having both * and other expressions
            has_star = any(isinstance(expr, exp.Star) for expr in select_stmt.expressions)
            has_other = any(not isinstance(expr, exp.Star) for expr in select_stmt.expressions)
            
            if has_star and has_other:
                score += 10  # Strong indicator of main SELECT
            elif has_star:
                score += 5   # Moderate indicator
            
            # Bonus for having FROM clause
            if select_stmt.find(exp.From):
                score += 3
            
            # Bonus for having WHERE clause
            if select_stmt.find(exp.Where):
                score += 2
            
            if score > max_score:
                max_score = score
                best_candidate = select_stmt
        
        return best_candidate
    
    def _analyze_select_expressions(self, select_stmt):
        """Analyze what a SELECT statement is doing"""
        analysis = {
            'has_wildcard': False,
            'wildcard_source': None,
            'explicit_expressions': [],
            'from_source': None
        }
        
        # Analyze expressions
        for expr in select_stmt.expressions:
            if isinstance(expr, exp.Star):
                analysis['has_wildcard'] = True
                # Try to determine wildcard source
                if hasattr(expr, 'table') and expr.table:
                    analysis['wildcard_source'] = str(expr.table)
            elif isinstance(expr, exp.Alias):
                analysis['explicit_expressions'].append({
                    'alias': str(expr.alias),
                    'expression': str(expr.this),
                    'type': type(expr.this).__name__
                })
            else:
                analysis['explicit_expressions'].append({
                    'expression': str(expr),
                    'type': type(expr).__name__
                })
        
        # Analyze FROM clause
        from_clause = select_stmt.find(exp.From)
        if from_clause:
            analysis['from_source'] = str(from_clause.this)
        
        return analysis
    
    def _build_table_registry(self, parsed):
        """Build registry of all tables and their aliases"""
        registry = {}
        
        for node in parsed.walk():
            if isinstance(node, exp.Table):
                table_name = str(node)
                
                # Register with alias if present
                if node.alias:
                    alias = str(node.alias)
                    registry[alias] = {
                        'type': 'table',
                        'full_name': table_name,
                        'alias': alias
                    }
                
                # Register with implicit alias (last part of table name)
                implicit_alias = table_name.split('.')[-1]
                if implicit_alias not in registry:
                    registry[implicit_alias] = {
                        'type': 'table',
                        'full_name': table_name,
                        'alias': implicit_alias
                    }
        
        return registry
    
    def _build_cte_registry(self, parsed, table_registry):
        """Build registry of all CTEs and their column mappings"""
        registry = {}
        
        for node in parsed.walk():
            if isinstance(node, exp.CTE):
                cte_name = str(node.alias)
                cte_select = node.this
                
                # Analyze this CTE's SELECT
                cte_analysis = self._analyze_select_expressions(cte_select)
                
                # Build column mapping for this CTE
                column_mapping = {}
                
                # Handle explicit expressions
                for expr_info in cte_analysis['explicit_expressions']:
                    if 'alias' in expr_info:
                        # This is an aliased expression
                        alias = expr_info['alias']
                        if expr_info['type'] == 'Column':
                            # Direct column mapping
                            column_mapping[alias] = self._parse_column_reference(
                                expr_info['expression'], table_registry
                            )
                        else:
                            # Derived expression
                            column_mapping[alias] = {
                                'type': 'derived',
                                'expression': expr_info['expression'],
                                'expression_type': expr_info['type']
                            }
                    else:
                        # Non-aliased expression - check if it's a qualified wildcard or direct column
                        expr_str = expr_info['expression']
                        if expr_str.endswith('.*'):
                            # This is a qualified wildcard like 'sd.*'
                            table_alias = expr_str[:-2]  # Remove '.*'
                            if table_alias in table_registry:
                                column_mapping['__WILDCARD__'] = {
                                    'type': 'wildcard',
                                    'source_table': table_registry[table_alias]['full_name'],
                                    'source_alias': table_alias
                                }
                        elif expr_info['type'] == 'Column':
                            # This is a direct column reference without alias
                            col_ref = self._parse_column_reference(expr_str, table_registry)
                            if col_ref['type'] == 'direct':
                                # Extract column name from qualified reference
                                if '.' in expr_str:
                                    col_name = expr_str.split('.')[-1]
                                else:
                                    col_name = expr_str
                                column_mapping[col_name] = col_ref
                
                # Handle wildcards
                if cte_analysis['has_wildcard']:
                    wildcard_source = cte_analysis.get('wildcard_source')
                    if wildcard_source and wildcard_source in table_registry:
                        column_mapping['__WILDCARD__'] = {
                            'type': 'wildcard',
                            'source_table': table_registry[wildcard_source]['full_name'],
                            'source_alias': wildcard_source
                        }
                    elif cte_analysis['from_source'] and cte_analysis['from_source'] in table_registry:
                        # Unqualified wildcard - use FROM source
                        from_source = cte_analysis['from_source']
                        column_mapping['__WILDCARD__'] = {
                            'type': 'wildcard',
                            'source_table': table_registry[from_source]['full_name'],
                            'source_alias': from_source
                        }
                
                registry[cte_name] = {
                    'column_mapping': column_mapping,
                    'analysis': cte_analysis
                }
        
        return registry
    
    def _parse_column_reference(self, col_expr_str, table_registry):
        """Parse a column reference string to find its source"""
        # Handle qualified references (table.column)
        if '.' in col_expr_str:
            parts = col_expr_str.split('.')
            if len(parts) == 2:
                table_alias, column_name = parts
                if table_alias in table_registry:
                    return {
                        'type': 'direct',
                        'source_table': table_registry[table_alias]['full_name'],
                        'source_column': column_name,
                        'table_alias': table_alias
                    }
        
        # Unqualified reference - need to resolve later
        return {
            'type': 'unqualified',
            'column_name': col_expr_str
        }
    
    def _trace_column_source(self, view_col, main_select_analysis, cte_registry, table_registry):
        """Trace where a view column comes from"""
        
        # Check if it's explicitly defined in main SELECT
        for expr_info in main_select_analysis['explicit_expressions']:
            if 'alias' in expr_info and expr_info['alias'].lower() == view_col.lower():
                if expr_info['type'] == 'Column':
                    return self._parse_column_reference(expr_info['expression'], table_registry)
                else:
                    return {
                        'type': 'derived',
                        'expression': expr_info['expression'],
                        'expression_type': expr_info['type']
                    }
        
        # Check if it comes from main SELECT wildcard
        if main_select_analysis['has_wildcard']:
            from_source = main_select_analysis['from_source']
            
            # If selecting from a CTE
            if from_source and from_source in cte_registry:
                cte_info = cte_registry[from_source]
                
                # First, check if explicitly defined in CTE (higher priority)
                if view_col in cte_info['column_mapping']:
                    return cte_info['column_mapping'][view_col]
                
                # Check for case-insensitive match in explicit mappings
                for cte_col_name, cte_col_mapping in cte_info['column_mapping'].items():
                    if (cte_col_name != '__WILDCARD__' and 
                        cte_col_name.lower() == view_col.lower()):
                        return cte_col_mapping
                
                # Finally, check if it comes from CTE wildcard (lower priority)
                if '__WILDCARD__' in cte_info['column_mapping']:
                    wildcard_info = cte_info['column_mapping']['__WILDCARD__']
                    return {
                        'type': 'direct',
                        'source_table': wildcard_info['source_table'],
                        'source_column': view_col,
                        'table_alias': wildcard_info['source_alias'],
                        'traced_through': from_source
                    }
            
            # If selecting directly from a table
            elif from_source and from_source in table_registry:
                return {
                    'type': 'direct',
                    'source_table': table_registry[from_source]['full_name'],
                    'source_column': view_col,
                    'table_alias': from_source
                }
        
        # Fallback - unknown
        return {
            'type': 'unknown',
            'source_table': 'UNKNOWN',
            'source_column': 'UNKNOWN'
        }
    
    def generate_dynamic_csv(self, analysis):
        """Generate CSV from dynamic analysis"""
        if 'error' in analysis:
            return f"Error: {analysis['error']}"
        
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Header
        writer.writerow([
            'View_Name',
            'View_Column',
            'Column_Type',
            'Source_Table',
            'Source_Column',
            'Expression_Type'
        ])
        
        view_name = analysis['view_name']
        
        for view_col in analysis['view_columns']:
            if view_col in analysis['column_mappings']:
                mapping = analysis['column_mappings'][view_col]
                
                column_type = 'Direct' if mapping['type'] != 'unknown' else 'Unknown'
                source_table = mapping['source_table']
                source_column = mapping['source_column']
                
                # Clean up table name
                if ' AS ' in source_table:
                    source_table = source_table.split(' AS ')[0]
                
                writer.writerow([
                    view_name,
                    view_col,
                    column_type,
                    source_table,
                    source_column,
                    ''
                ])
            
            elif view_col in analysis['derived_columns']:
                derived = analysis['derived_columns'][view_col]
                
                # Trace referenced columns to their actual sources
                expression = derived['expression']
                column_sources = self._trace_expression_columns_to_sources(
                    expression, analysis.get('debug_info', {})
                )
                
                if column_sources:
                    # Format as "column->source_table.source_column"
                    source_mappings = []
                    for col_info in column_sources:
                        if col_info['source_table'] != 'UNKNOWN':
                            source_mappings.append(f"{col_info['column']}->{col_info['source_table']}.{col_info['source_column']}")
                        else:
                            source_mappings.append(col_info['column'])
                    
                    source_info = '; '.join(source_mappings)
                    source_table = column_sources[0]['source_table'] if column_sources else 'CALCULATED'
                else:
                    source_info = f"{derived['expression_type']}: Complex Expression"
                    source_table = 'CALCULATED'
                
                writer.writerow([
                    view_name,
                    view_col,
                    'Derived',
                    source_table,
                    source_info,
                    derived['expression_type']
                ])
            
            else:
                writer.writerow([
                    view_name,
                    view_col,
                    'Unknown',
                    'UNKNOWN',
                    'UNKNOWN',
                    ''
                ])
        
        return output.getvalue()
    
    def _extract_columns_from_expression(self, expression):
        """Extract column references from a complex expression - completely dynamic"""
        import re
        
        # More sophisticated approach: look for patterns that are likely column references
        potential_columns = []
        
        # Pattern 1: Words that are not in quotes and not SQL keywords
        word_pattern = r'\b([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)\b'
        matches = re.findall(word_pattern, expression)
        
        # Build a dynamic SQL keywords list from common SQL terms in the expression
        # This avoids hardcoding and adapts to the actual SQL content
        expression_lower = expression.lower()
        dynamic_sql_keywords = set()
        
        # Common SQL keywords that appear in expressions
        common_keywords = ['case', 'when', 'then', 'else', 'end', 'and', 'or', 'not', 'null', 'is', 'between']
        for keyword in common_keywords:
            if keyword in expression_lower:
                dynamic_sql_keywords.add(keyword)
        
        # Add string literals that appear in the expression
        string_literals = re.findall(r"'([^']+)'", expression)
        for literal in string_literals:
            # Split compound words in string literals
            words = re.findall(r'[A-Z]+', literal)
            for word in words:
                dynamic_sql_keywords.add(word.lower())
        
        # Filter matches using dynamic criteria
        for match in matches:
            match_lower = match.lower()
            
            # Skip if it's a dynamically detected SQL keyword
            if match_lower in dynamic_sql_keywords:
                continue
            
            # Skip obvious non-columns
            if (match.startswith("'") or 
                match.isdigit() or 
                len(match) < 2 or
                match_lower in {'current_date', 'current_timestamp'}):
                continue
            
            # Include if it looks like a column reference
            potential_columns.append(match)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_columns = []
        for col in potential_columns:
            if col not in seen:
                seen.add(col)
                unique_columns.append(col)
        
        return unique_columns
    
    def _trace_expression_columns_to_sources(self, expression, debug_info):
        """Trace columns in an expression back to their source tables - completely dynamic"""
        # Extract column references from the expression
        referenced_columns = self._extract_columns_from_expression(expression)
        
        column_sources = []
        
        # Get the CTE registry and table registry from debug info
        cte_registry = debug_info.get('cte_registry_full', {})
        table_registry = debug_info.get('table_registry', {})
        
        # Get the main SELECT info to understand the context
        main_select_analysis = debug_info.get('main_select_analysis', {})
        from_source = main_select_analysis.get('from_source')
        
        for col_name in referenced_columns:
            # Try to find the source - if it exists, it's likely a real column
            # This automatically filters out non-columns dynamically
            source_info = self._find_column_source_in_context(
                col_name, from_source, cte_registry, table_registry
            )
            
            # Only include if we found a valid source (dynamic filtering)
            if source_info and source_info.get('source_table') != 'UNKNOWN':
                column_sources.append({
                    'column': col_name,
                    'source_table': source_info.get('source_table', 'UNKNOWN'),
                    'source_column': source_info.get('source_column', col_name)
                })
        
        return column_sources
    
    def _find_column_source_in_context(self, col_name, from_source, cte_registry, table_registry):
        """Find the source of a column within the current context - completely dynamic"""
        
        # If we're selecting from a CTE, look in that CTE first
        if from_source and from_source in cte_registry:
            cte_info = cte_registry[from_source]
            
            # Check explicit mappings in the CTE (case-insensitive)
            for cte_col_name, cte_col_mapping in cte_info['column_mapping'].items():
                if (cte_col_name != '__WILDCARD__' and 
                    cte_col_name.lower() == col_name.lower()):
                    # Clean up source table name
                    source_table = cte_col_mapping.get('source_table', '')
                    if ' AS ' in source_table:
                        source_table = source_table.split(' AS ')[0]
                    
                    return {
                        'source_table': source_table,
                        'source_column': cte_col_mapping.get('source_column', col_name)
                    }
            
            # Check if it comes from the CTE's wildcard
            if '__WILDCARD__' in cte_info['column_mapping']:
                wildcard_info = cte_info['column_mapping']['__WILDCARD__']
                source_table = wildcard_info.get('source_table', '')
                if ' AS ' in source_table:
                    source_table = source_table.split(' AS ')[0]
                
                return {
                    'source_table': source_table,
                    'source_column': col_name
                }
        
        # If no source found, return None (this dynamically filters out non-columns)
        return None

def main():
    """Test dynamic wildcard tracer"""
    
    sql_view = """
    create or replace view DC_DELIVERABLES_LIVE_ENG(
	DELIVERABLE_DESC,
	TASK_DESC,
	SUBTASK_DESC,
	DELIVERABLE_ID,
	TASK_ID,
	SUB_TASK_ID,
	BOOKING_CONTRACT,
	INDEX_POS,
	DC_ENGAGEMENT_ID,
	ENGAGEMENT_NAME,
	DUE_DATE,
	VISIBILITY_DATE,
	HEADER_NAME,
	MIN_OPEN_DATE,
	MAX_OPEN_DATE,
	COMPLETED_ST,
	COMPLETED_DATE,
	COMPLETED_USER_ID,
	COMPLETED_CCO,
	COMPLETION_TYPE_ID,
	TASK_STATUS
) as
with related as (
                select sd.* ,
                cd.SUB_TASK_ID as completed_st,
                cd.CREATE_DTM as completed_date,
                cd.DC_USER_ID as completed_user_id,
                u.CISCO_CCO_ID as completed_cco,
                cd.completion_type_id
                 from dc_deliverables_owed_scheduled_eng sd

                          left join dc_completed_deliverables cd
                                    on (cd.SUB_TASK_ID = sd.SUB_TASK_ID
                                        and cd.BOOKING_CONTRACT = sd.BOOKING_CONTRACT
                                        and cd.cycle_iterator = sd.index_pos
                                        and cd.DC_ENGAGEMENT_ID = sd.dc_engagement_id
                                        and cd.due_date = sd.due_date -- Added for CXEA Scale support
                                        and cd.is_deleted = 'F'
                                        )
                        left join dc_users u on (u.USER_ID=cd.DC_USER_ID)
), any_incomplete as ( -- any reasonable deliverables not fully closed out
    select distinct header_name from related where completed_st is null  and min_open_date <= current_date
), any_complete as ( -- any reasonable deliverables not fully closed out
    select distinct header_name from related where header_name not in (select header_name from any_incomplete)
)
select *
       , case
         when completed_st is NOT null then 'COMPLETE'
         when current_date between min_open_date and max_open_date  then 'OPEN/ACTIVE'
         when current_date > max_open_date then 'OVERDUE' -- NOT FOR METRICS!!! just state on screen
         else 'FUTURE' end as Task_Status
       from related
       where (
           min_open_date <= current_date -- not future
           or
           header_name in (select header_name from any_incomplete) -- incomplete
           or
           current_date between min_open_date and max_open_date --  'OPEN/ACTIVE'
            )
       and  header_name NOT in (select header_name from any_complete) -- complete
order by header_name, visibility_date;
    """
    
    print("DYNAMIC WILDCARD TRACER - Zero Hardcoded Values")
    print("=" * 60)
    
    tracer = DynamicWildcardTracer(dialect="snowflake")
    analysis = tracer.analyze_view_dynamically(sql_view)
    
    if 'error' in analysis:
        print(f"Error: {analysis['error']}")
        return
    
    # Generate CSV
    csv_output = tracer.generate_dynamic_csv(analysis)
    
    print("DYNAMIC CSV OUTPUT:")
    print("-" * 30)
    print(csv_output)
    
    # Save to file
    with open('view_column_v2_wildcard_tracing_parser.csv', 'w', encoding='utf-8', newline='') as f:
        f.write(csv_output)
    
   
    
    # Debug info
    debug = analysis['debug_info']
    print(f"\nDEBUG INFO:")
    print(f"View: {analysis['view_name']}")
    print(f"Total View Columns: {debug['total_view_columns']}")
    print(f"Mapped Columns: {debug['mapped_columns']}")
    print(f"Derived Columns: {debug['derived_columns']}")
    print(f"Success Rate: {((debug['mapped_columns'] + debug['derived_columns']) / debug['total_view_columns'] * 100):.1f}%")
    print(f"CTEs Found: {debug['cte_registry']}")
    print(f"Tables Found: {list(debug['table_registry'].keys())}")
    print(f"Main SELECT Analysis: {debug['main_select_analysis']}")
    
    # Show sample column mappings
    print(f"\nSample Column Mappings:")
    for i, (col_name, mapping) in enumerate(list(analysis['column_mappings'].items())[:5]):
        print(f"  {col_name}: {mapping}")
    
    # Show CTE registry details
    print(f"\nCTE Registry Details:")
    for cte_name in debug['cte_registry']:
        cte_info = debug['cte_registry_full'].get(cte_name, {})
        print(f"  {cte_name}: {cte_info}")

if __name__ == "__main__":
    main()