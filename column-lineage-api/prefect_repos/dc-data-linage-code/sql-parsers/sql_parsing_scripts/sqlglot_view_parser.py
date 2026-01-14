#!/usr/bin/env python3
"""
SQL View Parser using SQLGlot
Extracts source tables and column mappings from SQL views
"""

import sqlglot
from sqlglot import exp
from collections import defaultdict
import json

def parse_view_with_sqlglot(sql_text, dialect="snowflake"):
    """
    Parse SQL view and extract column-to-source table mappings
    
    Args:
        sql_text (str): SQL view definition
        dialect (str): SQL dialect (snowflake, postgres, mysql, etc.)
    
    Returns:
        dict: Analysis results including tables, columns, and mappings
    """
    try:
        # Parse the SQL
        parsed = sqlglot.parse_one(sql_text, dialect=dialect)
        
        # Initialize result containers
        result = {
            'tables': [],
            'columns': [],
            'column_mappings': defaultdict(list),
            'derived_columns': [],
            'aliases': {},
            'ctes': []
        }
        
        # Extract table aliases
        def extract_table_aliases(node):
            """Extract table aliases from FROM/JOIN clauses"""
            if isinstance(node, exp.Table):
                table_name = str(node)
                alias = node.alias if node.alias else table_name.split('.')[-1].lower()
                result['aliases'][alias] = table_name
                if table_name not in result['tables']:
                    result['tables'].append(table_name)
            
            # Handle subqueries and CTEs
            elif isinstance(node, exp.Subquery):
                if node.alias:
                    result['aliases'][node.alias] = f"SUBQUERY_{node.alias}"
            
            # Recursively process child nodes
            for child in node.iter_child_nodes():
                extract_table_aliases(child)
        
        # Extract CTEs
        def extract_ctes(node):
            """Extract Common Table Expressions"""
            if isinstance(node, exp.With):
                for cte in node.expressions:
                    if isinstance(cte, exp.CTE):
                        cte_name = cte.alias
                        result['ctes'].append(cte_name)
                        result['aliases'][cte_name.lower()] = f"CTE_{cte_name}"
        
        # Extract column references and their sources
        def extract_column_sources(node, current_context=None):
            """Extract column references and map them to source tables"""
            
            if isinstance(node, exp.Column):
                column_name = node.name
                table_ref = node.table if node.table else current_context
                
                # Resolve table alias to actual table name
                if table_ref and table_ref.lower() in result['aliases']:
                    source_table = result['aliases'][table_ref.lower()]
                    result['column_mappings'][column_name].append(source_table)
                
                # Add to columns list if not already present
                column_info = {
                    'name': column_name,
                    'table': table_ref,
                    'resolved_table': result['aliases'].get(table_ref.lower() if table_ref else None)
                }
                if column_info not in result['columns']:
                    result['columns'].append(column_info)
            
            # Handle derived columns (CASE, functions, etc.)
            elif isinstance(node, (exp.Case, exp.Anonymous, exp.Function)):
                # This is a derived/calculated column
                if hasattr(node, 'alias') and node.alias:
                    result['derived_columns'].append({
                        'name': node.alias,
                        'expression': str(node),
                        'type': type(node).__name__
                    })
            
            # Handle SELECT statements to get context
            elif isinstance(node, exp.Select):
                # Extract FROM table for context
                if node.find(exp.From):
                    from_table = node.find(exp.From).this
                    if isinstance(from_table, exp.Table):
                        current_context = from_table.alias or from_table.name
                
                # Process projections (SELECT list)
                for projection in node.expressions:
                    if isinstance(projection, exp.Alias):
                        # Handle aliased expressions
                        alias_name = projection.alias
                        expression = projection.this
                        
                        if isinstance(expression, exp.Column):
                            table_ref = expression.table
                            source_table = result['aliases'].get(table_ref.lower() if table_ref else None)
                            if source_table:
                                result['column_mappings'][alias_name].append(source_table)
                        else:
                            # This is a derived column
                            result['derived_columns'].append({
                                'name': alias_name,
                                'expression': str(expression),
                                'type': type(expression).__name__
                            })
            
            # Recursively process child nodes
            for child in node.iter_child_nodes():
                extract_column_sources(child, current_context)
        
        # Start extraction
        extract_table_aliases(parsed)
        extract_ctes(parsed)
        extract_column_sources(parsed)
        
        # Clean up column mappings (remove duplicates)
        result['column_mappings'] = {k: list(set(v)) for k, v in result['column_mappings'].items()}
        
        return result
        
    except Exception as e:
        return {'error': f"Parsing failed: {str(e)}"}

def analyze_view_lineage(sql_text, dialect="snowflake"):
    """
    Comprehensive analysis of view column lineage
    """
    result = parse_view_with_sqlglot(sql_text, dialect)
    
    if 'error' in result:
        return result
    
    # Create a more detailed lineage report
    lineage_report = {
        'view_name': extract_view_name(sql_text),
        'source_tables': result['tables'],
        'total_columns': len(result['columns']) + len(result['derived_columns']),
        'source_columns': len(result['columns']),
        'derived_columns': len(result['derived_columns']),
        'column_lineage': {},
        'table_dependencies': result['tables'],
        'ctes_used': result['ctes']
    }
    
    # Build detailed column lineage
    for col_info in result['columns']:
        col_name = col_info['name']
        if col_name in result['column_mappings']:
            lineage_report['column_lineage'][col_name] = {
                'type': 'source',
                'source_tables': result['column_mappings'][col_name],
                'original_table': col_info['table']
            }
    
    for derived_col in result['derived_columns']:
        lineage_report['column_lineage'][derived_col['name']] = {
            'type': 'derived',
            'expression': derived_col['expression'],
            'expression_type': derived_col['type']
        }
    
    return lineage_report

def extract_view_name(sql_text):
    """Extract view name from CREATE VIEW statement"""
    try:
        parsed = sqlglot.parse_one(sql_text)
        if isinstance(parsed, exp.Create) and parsed.kind == "VIEW":
            return str(parsed.this)
    except:
        pass
    return "UNKNOWN_VIEW"

def print_lineage_report(report):
    """Pretty print the lineage report"""
    if 'error' in report:
        print(f" Error: {report['error']}")
        return
    
    print(f"VIEW LINEAGE ANALYSIS: {report['view_name']}")
    print("=" * 60)
    
    print(f"\nSUMMARY:")
    print(f"  • Total Columns: {report['total_columns']}")
    print(f"  • Source Columns: {report['source_columns']}")
    print(f"  • Derived Columns: {report['derived_columns']}")
    print(f"  • Source Tables: {len(report['source_tables'])}")
    print(f"  • CTEs Used: {len(report['ctes_used'])}")
    
    print(f"\n SOURCE TABLES:")
    for i, table in enumerate(report['source_tables'], 1):
        print(f"  {i}. {table}")
    
    if report['ctes_used']:
        print(f"\n CTEs:")
        for cte in report['ctes_used']:
            print(f"  • {cte}")
    
    print(f"\nCOLUMN LINEAGE:")
    for col_name, lineage in report['column_lineage'].items():
        if lineage['type'] == 'source':
            sources = ', '.join(lineage['source_tables'])
            print(f"   {col_name} {sources}")
        else:
            print(f"   {col_name}  DERIVED ({lineage['expression_type']})")
            if len(lineage['expression']) < 100:
                print(f"      Expression: {lineage['expression']}")

# Example usage with your view
if __name__ == "__main__":
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
    
    # Analyze the view
    print("Analyzing SQL View with SQLGlot...")
    report = analyze_view_lineage(sql_view, dialect="snowflake")
    
    # Print the results
    print_lineage_report(report)
    
    # Also save as JSON for programmatic use
    with open('view_lineage_report.json', 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    print(f"\n Detailed report saved to: view_lineage_report.json")