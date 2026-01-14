#!/usr/bin/env python3
"""
View Column Mapper using SQLGlot
Maps view columns to their source tables with detailed analysis
"""

import sqlglot
from sqlglot import exp
from collections import defaultdict
import json

class ViewColumnMapper:
    """Maps view columns to their source tables using SQLGlot"""
    
    def __init__(self, dialect="snowflake"):
        self.dialect = dialect
        
    def analyze_view(self, sql_text):
        """
        Analyze a view and return detailed column-to-source mappings
        
        Returns:
            dict: Complete analysis with column mappings, source tables, etc.
        """
        try:
            parsed = sqlglot.parse_one(sql_text, dialect=self.dialect)
            
            # Initialize analysis results
            analysis = {
                'view_name': self._extract_view_name(parsed),
                'view_columns': [],
                'source_tables': [],
                'column_mappings': {},
                'derived_columns': {},
                'cte_definitions': {},
                'table_aliases': {},
                'join_relationships': []
            }
            
            # Step 1: Extract all table references and aliases
            self._extract_tables_and_aliases(parsed, analysis)
            
            # Step 2: Extract CTEs
            self._extract_ctes(parsed, analysis)
            
            # Step 3: Analyze column lineage
            self._analyze_column_lineage(parsed, analysis)
            
            # Step 4: Extract view column definitions
            self._extract_view_columns(parsed, analysis)
            
            return analysis
            
        except Exception as e:
            return {'error': f"Analysis failed: {str(e)}"}
    
    def _extract_view_name(self, parsed):
        """Extract the view name from CREATE VIEW statement"""
        if isinstance(parsed, exp.Create) and parsed.kind == "VIEW":
            return str(parsed.this)
        return "UNKNOWN_VIEW"
    
    def _extract_tables_and_aliases(self, parsed, analysis):
        """Extract all table references and their aliases"""
        for node in parsed.walk():
            if isinstance(node, exp.Table):
                table_name = str(node)
                analysis['source_tables'].append(table_name)
                
                # Handle aliases
                if node.alias:
                    alias = node.alias
                    analysis['table_aliases'][alias.lower()] = table_name
                else:
                    # Use last part of table name as implicit alias
                    implicit_alias = table_name.split('.')[-1].lower()
                    analysis['table_aliases'][implicit_alias] = table_name
    
    def _extract_ctes(self, parsed, analysis):
        """Extract Common Table Expressions"""
        for node in parsed.walk():
            if isinstance(node, exp.CTE):
                cte_name = node.alias
                analysis['cte_definitions'][cte_name] = {
                    'name': cte_name,
                    'definition': str(node.this)
                }
                analysis['table_aliases'][cte_name.lower()] = f"CTE_{cte_name}"
    
    def _analyze_column_lineage(self, parsed, analysis):
        """Analyze column lineage throughout the query"""
        
        # Find all SELECT statements
        for node in parsed.walk():
            if isinstance(node, exp.Select):
                self._analyze_select_statement(node, analysis)
    
    def _analyze_select_statement(self, select_node, analysis):
        """Analyze a single SELECT statement for column mappings"""
        
        for expr in select_node.expressions:
            if isinstance(expr, exp.Alias):
                # Aliased expression
                column_alias = expr.alias
                source_expr = expr.this
                
                if isinstance(source_expr, exp.Column):
                    # Direct column reference
                    self._map_direct_column(column_alias, source_expr, analysis)
                else:
                    # Derived/calculated column
                    self._map_derived_column(column_alias, source_expr, analysis)
            
            elif isinstance(expr, exp.Column):
                # Direct column without alias
                column_name = expr.name
                self._map_direct_column(column_name, expr, analysis)
    
    def _map_direct_column(self, column_name, column_expr, analysis):
        """Map a direct column reference to its source table"""
        table_ref = column_expr.table
        source_column = column_expr.name
        
        if table_ref:
            # Resolve alias to actual table name
            actual_table = analysis['table_aliases'].get(table_ref.lower(), table_ref)
            
            analysis['column_mappings'][column_name] = {
                'type': 'direct',
                'source_table': actual_table,
                'source_column': source_column,
                'table_alias': table_ref
            }
    
    def _map_derived_column(self, column_name, expression, analysis):
        """Map a derived/calculated column"""
        
        # Extract any column references within the expression
        referenced_columns = []
        for node in expression.walk():
            if isinstance(node, exp.Column):
                table_ref = node.table
                if table_ref:
                    actual_table = analysis['table_aliases'].get(table_ref.lower(), table_ref)
                    referenced_columns.append({
                        'table': actual_table,
                        'column': node.name,
                        'alias': table_ref
                    })
        
        analysis['derived_columns'][column_name] = {
            'expression': str(expression),
            'expression_type': type(expression).__name__,
            'referenced_columns': referenced_columns
        }
    
    def _extract_view_columns(self, parsed, analysis):
        """Extract the view column definitions from CREATE VIEW statement"""
        if isinstance(parsed, exp.Create) and parsed.kind == "VIEW":
            if hasattr(parsed.this, 'expressions') and parsed.this.expressions:
                # View has explicit column list
                for expr in parsed.this.expressions:
                    if isinstance(expr, exp.Identifier):
                        analysis['view_columns'].append(expr.name)
    
    def generate_mapping_report(self, analysis):
        """Generate a detailed mapping report"""
        if 'error' in analysis:
            return analysis
        
        report = {
            'view_name': analysis['view_name'],
            'summary': {
                'total_view_columns': len(analysis['view_columns']),
                'direct_mappings': len(analysis['column_mappings']),
                'derived_columns': len(analysis['derived_columns']),
                'source_tables': len(set(analysis['source_tables'])),
                'ctes_used': len(analysis['cte_definitions'])
            },
            'column_details': {}
        }
        
        # Map each view column to its source
        for view_col in analysis['view_columns']:
            if view_col in analysis['column_mappings']:
                mapping = analysis['column_mappings'][view_col]
                report['column_details'][view_col] = {
                    'type': 'direct',
                    'source': f"{mapping['source_table']}.{mapping['source_column']}"
                }
            elif view_col in analysis['derived_columns']:
                derived = analysis['derived_columns'][view_col]
                report['column_details'][view_col] = {
                    'type': 'derived',
                    'expression_type': derived['expression_type'],
                    'depends_on': [f"{ref['table']}.{ref['column']}" for ref in derived['referenced_columns']]
                }
            else:
                report['column_details'][view_col] = {
                    'type': 'unknown',
                    'note': 'Could not determine source'
                }
        
        return report
    
    def print_analysis(self, analysis):
        """Print a formatted analysis report"""
        if 'error' in analysis:
            print(f"Error: {analysis['error']}")
            return
        
        print(f"VIEW ANALYSIS: {analysis['view_name']}")
        print("=" * 60)
        
        print(f"\nSOURCE TABLES ({len(set(analysis['source_tables']))}):")
        for table in sorted(set(analysis['source_tables'])):
            if not table.startswith('CTE_') and table != analysis['view_name']:
                print(f"  • {table}")
        
        if analysis['cte_definitions']:
            print(f"\nCTEs ({len(analysis['cte_definitions'])}):")
            for cte_name in analysis['cte_definitions']:
                print(f"  • {cte_name}")
        
        print(f"\nVIEW COLUMNS ({len(analysis['view_columns'])}):")
        for col in analysis['view_columns']:
            if col in analysis['column_mappings']:
                mapping = analysis['column_mappings'][col]
                print(f"  {col} <- {mapping['source_table']}.{mapping['source_column']}")
            elif col in analysis['derived_columns']:
                derived = analysis['derived_columns'][col]
                print(f"  {col} <- DERIVED ({derived['expression_type']})")
                if derived['referenced_columns']:
                    deps = [f"{ref['table']}.{ref['column']}" for ref in derived['referenced_columns']]
                    print(f"    Depends on: {', '.join(deps)}")
            else:
                print(f"  {col} <- UNKNOWN SOURCE")

def main():
    """Demonstrate the ViewColumnMapper with your SQL view"""
    
    # Your SQL view
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
    with so as (
    with mx_date as (
    select s.BOOKING_CONTRACT, max(s.CREATE_DTM) as last_signoff_date
    from CPS_DSCI_API.DC_WF_IB_SIGNOFF s
    group by BOOKING_CONTRACT
    )
    select distinct s.BOOKING_CONTRACT,
    case
    when s.SIGNOFF_METHOD_ID != 7 then 'Signed off'
    when s.SIGNOFF_METHOD_ID = 7 then 'Defered Signed off'
    else 'sign_off_overdue'
    end as signoff_type,
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
    )
    select distinct BOOKING_CONTRACT,ibv_method, ibv_identity,ibv_event,notes,
    case
    when DATEDIFF(day, last_signoff_date,current_date) > 90 then 'sign_off_overdue'
    else signoff_type
    end as qualified_ibv,
    DATEDIFF(day, last_signoff_date,current_date) as days_since_last_signoff_event,
    last_signoff_date
    from so;
    """
    
    # Create mapper and analyze
    mapper = ViewColumnMapper(dialect="snowflake")
    analysis = mapper.analyze_view(sql_view)
    
    # Print detailed analysis
    mapper.print_analysis(analysis)
    
    # Generate and save mapping report
    report = mapper.generate_mapping_report(analysis)
    
    print(f"\n\nDETAILED MAPPING REPORT:")
    print("=" * 40)
    print(json.dumps(report, indent=2))
    
    # Save to file
    with open('view_column_mapper.json', 'w') as f:
        json.dump(analysis, f, indent=2, default=str)
    
    

if __name__ == "__main__":
    main()