#!/usr/bin/env python3
"""
Robust CTE Tracer - Enhanced version of working_csv_mapper with better CTE tracing
Builds on the successful approach but adds recursive column resolution
"""

import sqlglot
from sqlglot import exp
from collections import defaultdict
import csv
import io

class RobustCTETracer:
    """Robust CTE tracer building on the working approach"""
    
    def __init__(self, dialect="snowflake"):
        self.dialect = dialect
        
    def analyze_view_robust(self, sql_text):
        """
        Robust analysis building on the working approach
        """
        try:
            parsed = sqlglot.parse_one(sql_text, dialect=self.dialect)
            
            # Use the same successful structure as working version
            analysis = {
                'view_name': self._extract_view_name(parsed),
                'view_columns': [],
                'source_tables': [],
                'column_mappings': {},
                'derived_columns': {},
                'cte_definitions': {},
                'table_aliases': {},
                'cte_column_details': {}  # New: detailed CTE column analysis
            }
            
            # Step 1: Use the proven working approach
            self._extract_tables_and_aliases(parsed, analysis)
            self._extract_ctes(parsed, analysis)
            self._analyze_column_lineage(parsed, analysis)
            self._extract_view_columns(parsed, analysis)
            
            # Step 2: Enhanced CTE analysis
            self._analyze_cte_columns_detailed(parsed, analysis)
            
            # Step 3: Enhance derived columns with CTE tracing
            self._enhance_derived_columns_with_cte_tracing(analysis)
            
            return analysis
            
        except Exception as e:
            return {'error': f"Robust analysis failed: {str(e)}"}
    
    def _extract_view_name(self, parsed):
        """Extract the view name from CREATE VIEW statement"""
        if isinstance(parsed, exp.Create) and parsed.kind == "VIEW":
            view_def = str(parsed.this)
            if '(' in view_def:
                return view_def.split('(')[0].strip()
            return view_def
        return "UNKNOWN_VIEW"
    
    def _extract_tables_and_aliases(self, parsed, analysis):
        """Extract all table references and their aliases (same as working version)"""
        for node in parsed.walk():
            if isinstance(node, exp.Table):
                table_name = str(node)
                analysis['source_tables'].append(table_name)
                
                if node.alias:
                    alias = node.alias
                    analysis['table_aliases'][alias.lower()] = table_name
                else:
                    implicit_alias = table_name.split('.')[-1].lower()
                    analysis['table_aliases'][implicit_alias] = table_name
    
    def _extract_ctes(self, parsed, analysis):
        """Extract Common Table Expressions (same as working version)"""
        for node in parsed.walk():
            if isinstance(node, exp.CTE):
                cte_name = node.alias
                analysis['cte_definitions'][cte_name] = {
                    'name': cte_name,
                    'definition': str(node.this)
                }
                analysis['table_aliases'][cte_name.lower()] = f"CTE_{cte_name}"
    
    def _analyze_column_lineage(self, parsed, analysis):
        """Analyze column lineage (same as working version)"""
        for node in parsed.walk():
            if isinstance(node, exp.Select):
                self._analyze_select_statement(node, analysis)
    
    def _analyze_select_statement(self, select_node, analysis):
        """Analyze a single SELECT statement (same as working version)"""
        for expr in select_node.expressions:
            if isinstance(expr, exp.Alias):
                column_alias = expr.alias
                source_expr = expr.this
                
                if isinstance(source_expr, exp.Column):
                    self._map_direct_column(column_alias, source_expr, analysis)
                else:
                    self._map_derived_column(column_alias, source_expr, analysis)
            
            elif isinstance(expr, exp.Column):
                column_name = expr.name
                self._map_direct_column(column_name, expr, analysis)
    
    def _map_direct_column(self, column_name, column_expr, analysis):
        """Map a direct column reference (same as working version)"""
        table_ref = column_expr.table
        source_column = column_expr.name
        
        if table_ref:
            actual_table = analysis['table_aliases'].get(table_ref.lower(), table_ref)
            
            analysis['column_mappings'][column_name] = {
                'type': 'direct',
                'source_table': actual_table,
                'source_column': source_column,
                'table_alias': table_ref
            }
    
    def _map_derived_column(self, column_name, expression, analysis):
        """Map a derived column - enhanced to capture unqualified columns"""
        referenced_columns = []
        unqualified_columns = []
        
        for node in expression.walk():
            if isinstance(node, exp.Column):
                table_ref = node.table
                col_name = node.name
                
                if table_ref:
                    # Qualified column reference
                    actual_table = analysis['table_aliases'].get(table_ref.lower(), table_ref)
                    referenced_columns.append({
                        'table': actual_table,
                        'column': col_name,
                        'alias': table_ref
                    })
                else:
                    # Unqualified column reference - capture for later resolution
                    unqualified_columns.append(col_name)
        
        analysis['derived_columns'][column_name] = {
            'expression': str(expression),
            'expression_type': type(expression).__name__,
            'referenced_columns': referenced_columns,
            'unqualified_columns': unqualified_columns  # New: store unqualified columns
        }
    
    def _extract_view_columns(self, parsed, analysis):
        """Extract view columns (same as working version)"""
        if isinstance(parsed, exp.Create) and parsed.kind == "VIEW":
            view_def = str(parsed.this)
            if '(' in view_def:
                col_part = view_def[view_def.find('(')+1:view_def.find(')')]
                analysis['view_columns'] = [col.strip() for col in col_part.split(',')]
    
    def _analyze_cte_columns_detailed(self, parsed, analysis):
        """NEW: Detailed analysis of what columns each CTE provides"""
        
        # Analyze each CTE's SELECT statement
        for cte_name, cte_info in analysis['cte_definitions'].items():
            cte_columns = {}
            
            # Find the CTE's SELECT node
            for node in parsed.walk():
                if isinstance(node, exp.CTE) and node.alias == cte_name:
                    select_node = node.this
                    
                    # Analyze the CTE's SELECT expressions
                    for expr in select_node.expressions:
                        if isinstance(expr, exp.Alias):
                            alias_name = expr.alias
                            source_expr = expr.this
                            
                            if isinstance(source_expr, exp.Column):
                                # Direct column in CTE
                                table_ref = source_expr.table
                                if table_ref and table_ref.lower() in analysis['table_aliases']:
                                    actual_table = analysis['table_aliases'][table_ref.lower()]
                                    cte_columns[alias_name] = {
                                        'type': 'direct',
                                        'source_table': actual_table,
                                        'source_column': source_expr.name,
                                        'table_alias': table_ref
                                    }
                                else:
                                    # Unqualified column - might be from another CTE
                                    cte_columns[alias_name] = {
                                        'type': 'unqualified_in_cte',
                                        'source_column': source_expr.name,
                                        'needs_resolution': True
                                    }
                            else:
                                # Derived column in CTE
                                referenced_cols = []
                                unqualified_cols = []
                                
                                for sub_node in source_expr.walk():
                                    if isinstance(sub_node, exp.Column):
                                        sub_table_ref = sub_node.table
                                        if sub_table_ref and sub_table_ref.lower() in analysis['table_aliases']:
                                            actual_table = analysis['table_aliases'][sub_table_ref.lower()]
                                            referenced_cols.append({
                                                'table': actual_table,
                                                'column': sub_node.name,
                                                'alias': sub_table_ref
                                            })
                                        else:
                                            # Unqualified column in derived expression
                                            unqualified_cols.append(sub_node.name)
                                
                                cte_columns[alias_name] = {
                                    'type': 'derived',
                                    'expression': str(source_expr),
                                    'expression_type': type(source_expr).__name__,
                                    'referenced_columns': referenced_cols,
                                    'unqualified_columns': unqualified_cols
                                }
                        
                        elif isinstance(expr, exp.Column):
                            # Direct column without alias in CTE
                            col_name = expr.name
                            table_ref = expr.table
                            if table_ref and table_ref.lower() in analysis['table_aliases']:
                                actual_table = analysis['table_aliases'][table_ref.lower()]
                                cte_columns[col_name] = {
                                    'type': 'direct',
                                    'source_table': actual_table,
                                    'source_column': col_name,
                                    'table_alias': table_ref
                                }
                            else:
                                # Unqualified column without alias
                                cte_columns[col_name] = {
                                    'type': 'unqualified_in_cte',
                                    'source_column': col_name,
                                    'needs_resolution': True
                                }
                    
                    break
            
            analysis['cte_column_details'][cte_name] = cte_columns
        
        # Step 2: Resolve unqualified columns in CTEs
        self._resolve_unqualified_columns_in_ctes(analysis)
    
    def _resolve_unqualified_columns_in_ctes(self, analysis):
        """Dynamically resolve unqualified column references within CTEs"""
        
        # Build CTE dependency order dynamically
        cte_order = self._determine_cte_dependency_order(analysis)
        
        for cte_name in cte_order:
            if cte_name not in analysis['cte_column_details']:
                continue
                
            cte_columns = analysis['cte_column_details'][cte_name]
            
            for col_name, col_info in cte_columns.items():
                if col_info.get('needs_resolution'):
                    # This is an unqualified column that needs resolution
                    source_col = col_info['source_column']
                    
                    # Look for this column in other CTEs or tables
                    resolved_source = self._resolve_unqualified_in_cte_context(
                        source_col, cte_name, analysis
                    )
                    
                    if resolved_source:
                        # Update the column info with resolved source
                        cte_columns[col_name] = resolved_source
                    
                elif col_info.get('type') == 'derived' and col_info.get('unqualified_columns'):
                    # Resolve unqualified columns in derived expressions
                    enhanced_refs = col_info.get('referenced_columns', []).copy()
                    
                    for unqual_col in col_info['unqualified_columns']:
                        resolved_source = self._resolve_unqualified_in_cte_context(
                            unqual_col, cte_name, analysis
                        )
                        
                        if resolved_source and resolved_source.get('type') == 'direct':
                            enhanced_refs.append({
                                'table': resolved_source['source_table'],
                                'column': resolved_source['source_column'],
                                'alias': resolved_source.get('table_alias', ''),
                                'resolved_from': unqual_col
                            })
                    
                    # Update the derived column with enhanced references
                    cte_columns[col_name]['enhanced_referenced_columns'] = enhanced_refs
    
    def _determine_cte_dependency_order(self, analysis):
        """Dynamically determine CTE dependency order by analyzing CTE definitions"""
        cte_names = list(analysis['cte_definitions'].keys())
        
        if len(cte_names) <= 1:
            return cte_names
        
        # Simple heuristic: CTEs that reference other CTEs should come after
        # those they reference. For nested CTEs, inner ones come first.
        
        dependencies = {}
        for cte_name in cte_names:
            dependencies[cte_name] = set()
            cte_def = analysis['cte_definitions'][cte_name]['definition']
            
            # Check if this CTE references other CTEs
            for other_cte in cte_names:
                if other_cte != cte_name and other_cte in cte_def:
                    dependencies[cte_name].add(other_cte)
        
        # Topological sort to get dependency order
        ordered = []
        remaining = set(cte_names)
        
        while remaining:
            # Find CTEs with no unresolved dependencies
            ready = []
            for cte in remaining:
                if not (dependencies[cte] & remaining):
                    ready.append(cte)
            
            if not ready:
                # Circular dependency or complex case - use original order
                ordered.extend(sorted(remaining))
                break
            
            # Add ready CTEs to order and remove from remaining
            ready.sort()  # For consistent ordering
            ordered.extend(ready)
            remaining -= set(ready)
        
        return ordered
    
    def _resolve_unqualified_in_cte_context(self, column_name, current_cte, analysis):
        """Dynamically resolve an unqualified column within a CTE context"""
        
        # Strategy 1: Look in other CTEs (in dependency order)
        cte_order = self._determine_cte_dependency_order(analysis)
        
        # Look in CTEs that come before the current one in dependency order
        current_index = cte_order.index(current_cte) if current_cte in cte_order else -1
        
        for i in range(current_index):
            other_cte = cte_order[i]
            if other_cte in analysis['cte_column_details']:
                other_cte_columns = analysis['cte_column_details'][other_cte]
                if column_name in other_cte_columns:
                    col_info = other_cte_columns[column_name]
                    if col_info.get('type') in ['direct', 'derived']:
                        return col_info
        
        # Strategy 2: Look in all other CTEs (fallback)
        for other_cte, cte_columns in analysis['cte_column_details'].items():
            if other_cte != current_cte and column_name in cte_columns:
                col_info = cte_columns[column_name]
                if col_info.get('type') in ['direct', 'derived']:
                    return col_info
        
        # Strategy 3: Look in table aliases for direct table references
        # This handles cases where the column might come from a joined table
        for alias, table_name in analysis['table_aliases'].items():
            if not table_name.startswith('CTE_'):
                # This is a real table - we assume the column exists there
                # In a real implementation, you might want to validate this
                return {
                    'type': 'direct',
                    'source_table': table_name,
                    'source_column': column_name,
                    'table_alias': alias,
                    'resolved_in_context': current_cte
                }
        
        return None
    
    def _enhance_derived_columns_with_cte_tracing(self, analysis):
        """NEW: Enhance derived columns by tracing unqualified column references through CTEs"""
        
        for col_name, derived_info in analysis['derived_columns'].items():
            enhanced_references = derived_info['referenced_columns'].copy()
            ultimate_tables = set()
            
            # Add already qualified references to ultimate tables
            for ref in derived_info['referenced_columns']:
                table_name = ref['table']
                if ' AS ' in table_name:
                    table_name = table_name.split(' AS ')[0]
                ultimate_tables.add(table_name)
            
            # Use the captured unqualified columns directly
            unqualified_columns = derived_info.get('unqualified_columns', [])
            
            # Trace unqualified columns through CTEs
            for unqual_col in unqualified_columns:
                traced_sources = self._trace_column_through_ctes(unqual_col, analysis)
                
                
                
                for source in traced_sources:
                    enhanced_references.append({
                        'table': source['table'],
                        'column': source['column'],
                        'alias': source.get('alias', ''),
                        'traced_from': unqual_col
                    })
                    
                    table_name = source['table']
                    if ' AS ' in table_name:
                        table_name = table_name.split(' AS ')[0]
                    ultimate_tables.add(table_name)
            
            # Update the derived column info
            analysis['derived_columns'][col_name]['enhanced_references'] = enhanced_references
            analysis['derived_columns'][col_name]['ultimate_source_tables'] = list(ultimate_tables)
    
    def _find_unqualified_columns_in_expression(self, expression_text, analysis):
        """Dynamically find unqualified column references in an expression"""
        import re
        
        potential_columns = []
        
        # Build a dynamic list of potential column names from CTE analysis
        all_possible_columns = set()
        
        # Add columns from all CTEs
        for cte_name, cte_columns in analysis.get('cte_column_details', {}).items():
            all_possible_columns.update(cte_columns.keys())
        
        # Add columns from direct mappings
        all_possible_columns.update(analysis.get('column_mappings', {}).keys())
        
        # Add columns from derived columns
        all_possible_columns.update(analysis.get('derived_columns', {}).keys())
        
        # Extract all identifiers from the expression
        identifier_pattern = r'(?<![\."\'])\b([a-zA-Z_][a-zA-Z0-9_]*)\b'
        identifiers = re.findall(identifier_pattern, expression_text)
        
        # Filter out SQL keywords and functions
        sql_keywords = {
            'case', 'when', 'then', 'else', 'end', 'and', 'or', 'not',
            'datediff', 'current_date', 'day', 'max', 'min', 'count', 'sum', 'avg',
            'select', 'from', 'where', 'join', 'on', 'as', 'distinct', 'group', 'by',
            'order', 'having', 'union', 'all', 'inner', 'left', 'right', 'outer',
            'null', 'is', 'in', 'exists', 'between', 'like', 'desc', 'asc',
            'create', 'view', 'table', 'insert', 'update', 'delete', 'drop',
            'alter', 'if', 'dateadd', 'current_timestamp', 'getdate'
        }
        
        # Check each identifier
        for identifier in identifiers:
            if (identifier.lower() not in sql_keywords and 
                len(identifier) > 1 and  # Avoid single letters
                identifier in all_possible_columns):  # Must be a known column
                potential_columns.append(identifier)
        
        return potential_columns
    
    def _trace_column_through_ctes(self, column_name, analysis):
        """Dynamically trace a column through the CTE hierarchy"""
        traced_sources = []
        
        # Determine which CTE is the "main" one (usually the last/outermost)
        cte_order = self._determine_cte_dependency_order(analysis)
        main_cte = cte_order[-1] if cte_order else None
        
        if main_cte and main_cte in analysis['cte_column_details']:
            main_cte_columns = analysis['cte_column_details'][main_cte]
            
            if column_name in main_cte_columns:
                col_info = main_cte_columns[column_name]
                
                if col_info['type'] == 'direct':
                    traced_sources.append({
                        'table': col_info['source_table'],
                        'column': col_info['source_column'],
                        'alias': col_info.get('table_alias', '')
                    })
                
                elif col_info['type'] == 'unqualified_in_cte':
                    # This was an unqualified column - look in other CTEs
                    for other_cte_name in reversed(cte_order[:-1]):  # Check in reverse dependency order
                        if other_cte_name in analysis['cte_column_details']:
                            other_cte_columns = analysis['cte_column_details'][other_cte_name]
                            if column_name in other_cte_columns:
                                other_col_info = other_cte_columns[column_name]
                                
                                if other_col_info['type'] == 'direct':
                                    traced_sources.append({
                                        'table': other_col_info['source_table'],
                                        'column': other_col_info['source_column'],
                                        'alias': other_col_info.get('table_alias', '')
                                    })
                                    break
                                    
                                elif other_col_info['type'] == 'derived':
                                    # Add all referenced columns from the derived expression
                                    for ref in other_col_info.get('referenced_columns', []):
                                        traced_sources.append({
                                            'table': ref['table'],
                                            'column': ref['column'],
                                            'alias': ref.get('alias', '')
                                        })
                                    # Also check enhanced references
                                    for ref in other_col_info.get('enhanced_referenced_columns', []):
                                        traced_sources.append({
                                            'table': ref['table'],
                                            'column': ref['column'],
                                            'alias': ref.get('alias', '')
                                        })
                                    break
                
                elif col_info['type'] == 'derived':
                    # Add all referenced columns from the derived expression
                    for ref in col_info.get('referenced_columns', []):
                        traced_sources.append({
                            'table': ref['table'],
                            'column': ref['column'],
                            'alias': ref.get('alias', '')
                        })
                    # Also check enhanced references
                    for ref in col_info.get('enhanced_referenced_columns', []):
                        traced_sources.append({
                            'table': ref['table'],
                            'column': ref['column'],
                            'alias': ref.get('alias', '')
                        })
        
        return traced_sources
    
    def generate_robust_csv(self, analysis):
        """Generate CSV with robust CTE tracing"""
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
            
            # Check direct mappings first
            if view_col in analysis['column_mappings']:
                mapping = analysis['column_mappings'][view_col]
                
                source_table = mapping['source_table']
                if ' AS ' in source_table:
                    source_table = source_table.split(' AS ')[0]
                
                writer.writerow([
                    view_name,
                    view_col,
                    'Direct',
                    source_table,
                    mapping['source_column'],
                    ''
                ])
            
            # Check enhanced derived columns
            elif view_col in analysis['derived_columns']:
                derived = analysis['derived_columns'][view_col]
                
                # Get ultimate source tables and columns
                ultimate_tables = set()
                source_columns = set()
                primary_source_table = None
                
                # Collect from enhanced references (traced columns)
                if 'enhanced_references' in derived:
                    for ref in derived['enhanced_references']:
                        table_name = ref['table']
                        if ' AS ' in table_name:
                            table_name = table_name.split(' AS ')[0]
                        ultimate_tables.add(table_name)
                        source_columns.add(ref['column'])
                        if not primary_source_table:
                            primary_source_table = table_name
                
                # Fallback to original referenced columns
                if not ultimate_tables:
                    for ref in derived.get('referenced_columns', []):
                        table_name = ref['table']
                        if ' AS ' in table_name:
                            table_name = table_name.split(' AS ')[0]
                        ultimate_tables.add(table_name)
                        source_columns.add(ref['column'])
                        if not primary_source_table:
                            primary_source_table = table_name
                
                # Fallback to ultimate_source_tables if available
                if not ultimate_tables and 'ultimate_source_tables' in derived:
                    for table in derived['ultimate_source_tables']:
                        if ' AS ' in table:
                            table = table.split(' AS ')[0]
                        ultimate_tables.add(table)
                        if not primary_source_table:
                            primary_source_table = table
                
                # Format the output
                source_table_display = primary_source_table if primary_source_table else 'CALCULATED'
                source_column_display = '; '.join(sorted(source_columns)) if source_columns else derived['expression_type']
                
                writer.writerow([
                    view_name,
                    view_col,
                    'Derived',
                    source_table_display,
                    source_column_display,
                    derived['expression_type']
                ])
            
            # Case-insensitive fallback
            else:
                found = False
                
                # Try case-insensitive search in direct mappings
                for col_name, mapping in analysis['column_mappings'].items():
                    if col_name.lower() == view_col.lower():
                        source_table = mapping['source_table']
                        if ' AS ' in source_table:
                            source_table = source_table.split(' AS ')[0]
                        
                        writer.writerow([
                            view_name,
                            view_col,
                            'Direct',
                            source_table,
                            mapping['source_column'],
                            ''
                        ])
                        found = True
                        break
                
                # Try case-insensitive search in derived columns
                if not found:
                    for col_name, derived in analysis['derived_columns'].items():
                        if col_name.lower() == view_col.lower():
                            # Get ultimate source tables and columns (same logic as above)
                            ultimate_tables = set()
                            source_columns = set()
                            primary_source_table = None
                            
                            # Collect from enhanced references
                            if 'enhanced_references' in derived:
                                for ref in derived['enhanced_references']:
                                    table_name = ref['table']
                                    if ' AS ' in table_name:
                                        table_name = table_name.split(' AS ')[0]
                                    ultimate_tables.add(table_name)
                                    source_columns.add(ref['column'])
                                    if not primary_source_table:
                                        primary_source_table = table_name
                            
                            # Fallback to original referenced columns
                            if not ultimate_tables:
                                for ref in derived.get('referenced_columns', []):
                                    table_name = ref['table']
                                    if ' AS ' in table_name:
                                        table_name = table_name.split(' AS ')[0]
                                    ultimate_tables.add(table_name)
                                    source_columns.add(ref['column'])
                                    if not primary_source_table:
                                        primary_source_table = table_name
                            
                            # Fallback to ultimate_source_tables
                            if not ultimate_tables and 'ultimate_source_tables' in derived:
                                for table in derived['ultimate_source_tables']:
                                    if ' AS ' in table:
                                        table = table.split(' AS ')[0]
                                    ultimate_tables.add(table)
                                    if not primary_source_table:
                                        primary_source_table = table
                            
                            # Format the output
                            source_table_display = primary_source_table if primary_source_table else 'CALCULATED'
                            source_column_display = '; '.join(sorted(source_columns)) if source_columns else derived['expression_type']
                            
                            writer.writerow([
                                view_name,
                                view_col,
                                'Derived',
                                source_table_display,
                                source_column_display,
                                derived['expression_type']
                            ])
                            found = True
                            break
                
                if not found:
                    writer.writerow([
                        view_name,
                        view_col,
                        'Unknown',
                        'UNKNOWN',
                        'UNKNOWN',
                        ''
                    ])
        
        return output.getvalue()

def main():
    """Demonstrate robust CTE tracing"""
    
    sql_view = """
     create or replace view DC_QUALIFIED_SIGNOFF(
	BOOKING_CONTRACT,
	IBV_METHOD,
	IBV_IDENTITY,
	IBV_EVENT,
	NOTES,
	QUALIFIED_IBV,
	DAYS_SINCE_LAST_SIGNOFF_EVENT,
	LAST_SIGNOFF_DATE
) as
 with so as ( -- this and qualified SO need to be crisp granularity of booking contract level across 2 events signoff and disconnect... so is it really 1?
            with mx_date as (-- resolve to tru last event
                select s.BOOKING_CONTRACT, max(s.CREATE_DTM) as last_signoff_date
                from CPS_DSCI_API.DC_WF_IB_SIGNOFF s
                group by BOOKING_CONTRACT
            ) -- get the unique last event details
            select distinct s.BOOKING_CONTRACT,
                   case
                       when s.SIGNOFF_METHOD_ID != 7 then 'Signed off'
                       when s.SIGNOFF_METHOD_ID = 7 then 'Defered Signed off'
                       else 'sign_off_overdue'
                       end           as signoff_type,
                     last_signoff_date,
                 m.SIGNOFF_METHOD as ibv_method ,
                i.SIGN_OFF_IDENTITY as ibv_identity,
              e.SIGNOFF_EVENT as ibv_event,
              s.NOTES
                from CPS_DSCI_API.DC_WF_IB_SIGNOFF s
                    join CPS_DSCI_API.DC_TYP_SIGNOFF_METHOD m on ( m.SIGNOFF_METHOD_ID=s.SIGNOFF_METHOD_ID)
                join CPS_DSCI_API.DC_TYP_SIGN_OFF_IDENTITY i on ( i.SIGN_OFF_IDENTITY_ID = s.SIGN_OFF_IDENTITY_ID)
                join CPS_DSCI_API.DC_TYP_SIGNOFF_EVENT e on ( e.SIGNOFF_EVENT_ID = s.signoff_event_id)
                join mx_date on ( mx_date.BOOKING_CONTRACT=s.BOOKING_CONTRACT and mx_date.last_signoff_date=s.CREATE_DTM)
                join CPS_DSCI_API.dc_BOOKINGS_CONTRACTS c
                          on (c.BOOKING_CONTRACT = s.BOOKING_CONTRACT and c.is_deleted = 'F')
            where current_date between c.AGREEMENT_START_DATE and dateadd(day, 30, c.AGREEMENT_END_DATE)
            and s.is_deleted = 'F'
    ) -- qualify the last event with current date for correct status
 select  distinct BOOKING_CONTRACT,ibv_method, ibv_identity,ibv_event,notes,
           case
                when DATEDIFF(day, last_signoff_date,current_date) > 90 then  'sign_off_overdue'  -- regardless of type after 90 your overdue
                else signoff_type
           end as qualified_ibv,
           DATEDIFF(day, last_signoff_date,current_date) as days_since_last_signoff_event,
           last_signoff_date
        from so;
    """
    
    print("ROBUST CTE TRACER - Enhanced Column Resolution")
    print("=" * 60)
    
    # Create robust tracer
    tracer = RobustCTETracer(dialect="snowflake")
    
    # Analyze the view
    analysis = tracer.analyze_view_robust(sql_view)
    
    if 'error' in analysis:
        print(f"Error: {analysis['error']}")
        return
    
    # Generate robust CSV
    csv_output = tracer.generate_robust_csv(analysis)
    
    print("ROBUST CSV OUTPUT:")
    print("-" * 30)
    print(csv_output)
    
    # Save to file
    with open('view_column_mapper_v1.csv', 'w', encoding='utf-8', newline='') as f:
        f.write(csv_output)
    
    # Show debug info
    print(f"\nDEBUG INFO:")
    print(f"View: {analysis['view_name']}")
    print(f"View Columns: {len(analysis['view_columns'])}")
    print(f"Direct Mappings: {len(analysis['column_mappings'])}")
    print(f"Derived Columns: {len(analysis['derived_columns'])}")
    print(f"CTE Column Details: {list(analysis['cte_column_details'].keys())}")
    
    # Show CTE analysis
    for cte_name, cte_cols in analysis['cte_column_details'].items():
        print(f"\n{cte_name} CTE columns: {list(cte_cols.keys())}")

if __name__ == "__main__":
    main()