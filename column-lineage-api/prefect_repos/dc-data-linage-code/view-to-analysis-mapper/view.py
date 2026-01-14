#!/usr/bin/env python3
"""
Script to analyze views in CPS_DSCI_API and CPS_DSCI_BR schemas.
"""

import pandas as pd
from sqlalchemy import create_engine, text
from common import sec
from datetime import datetime

def get_correct_schema(env: str) -> str:
    """Get the correct schema based on environment."""
    if env == 'prod':
        return 'CPS_DSCI_API'
    else:
        return 'CPS_DSCI_BR'

def check_env(env: str) -> str:
    """Check and return the correct connection name for environment."""
    if env == "dev":
        cn = "dev_cps_dsci_etl_svc"
    elif env == "stage":
        cn = "stg_cps_dsci_etl_svc"
    elif env == "prod":
        cn = "prd_cps_dsci_etl_svc"
    else:
        cn = env
    return cn

def create_sf_connection_engine(sf_env: str):
    """Create Snowflake connection engine using existing strategy."""
    try:
        cn = check_env(sf_env)
        correct_schema = get_correct_schema(sf_env)
        engine = create_engine(
            sec.get_sf_pw(cn, 'CPS_DSCI_ETL_EXT1_WH', correct_schema)
        )
        return engine
    except Exception as e:
        print(f"Failed to create Snowflake connection: {e}")
        raise

def get_all_views(sf_env: str = 'prod'):
    """Get all views from CPS_DSCI schemas."""
    
    # Query to get all views from both schemas
    sql_query = """
    SELECT *
    FROM INFORMATION_SCHEMA.VIEWS
    WHERE TABLE_SCHEMA IN ('CPS_DSCI_API', 'CPS_DSCI_BR', 'CPS_DSCI_STG', 'CPS_DSCI_WI')
    ORDER BY TABLE_SCHEMA, TABLE_NAME;
    """
    
    print("Getting views from CPS_DSCI schemas...")
    try:
        local_engine = create_sf_connection_engine(sf_env)
        with local_engine.connect() as connection:
            df = pd.read_sql(text(sql_query), connection)
        return df
    except Exception as e:
        print(f"Error getting views: {e}")
        raise

def analyze_views(df):
    """Analyze the views and show detailed information."""
    
    if df.empty:
        print("No views found in CPS_DSCI schemas!")
        return
    
    print(f"\nFound {len(df)} views")
    print(f"\nAvailable columns: {list(df.columns)}")
    
    # Find key columns dynamically
    schema_col = None
    name_col = None
    definition_col = None
    created_col = None
    
    for col in df.columns:
        col_lower = col.lower()
        if 'schema' in col_lower and 'table' in col_lower:
            schema_col = col
        elif 'name' in col_lower and 'table' in col_lower:
            name_col = col
        elif 'definition' in col_lower or 'text' in col_lower:
            definition_col = col
        elif 'created' in col_lower:
            created_col = col
    
    print(f"\nDetected columns:")
    print(f"  Schema column: {schema_col}")
    print(f"  Name column: {name_col}")
    print(f"  Definition column: {definition_col}")
    print(f"  Created column: {created_col}")
    
    if not schema_col or not name_col:
        print("\nCould not find required columns. Showing first few rows:")
        print(df.head())
        return
    
    # Analyze by schema
    print(f"\n" + "="*70)
    print("VIEWS BY SCHEMA")
    print("="*70)
    
    schema_counts = df[schema_col].value_counts()
    for schema, count in schema_counts.items():
        print(f"\n{schema}: {count} views")
        
        schema_views = df[df[schema_col] == schema]
        
        # Show view names and details
        print(f"  Views:")
        for _, view in schema_views.iterrows():
            name = view[name_col]
            created = view[created_col] if created_col else 'Unknown'
            
            # Get definition length if available
            definition_length = 0
            if definition_col and view[definition_col]:
                definition_length = len(str(view[definition_col]))
            
            print(f"    - {name} (Created: {created}, Definition: {definition_length:,} chars)")
    
    return schema_col, name_col, definition_col

def compare_views_between_schemas(df, schema_col, name_col, definition_col):
    """Compare views between API and BR schemas."""
    
    if not schema_col or not name_col:
        return
    
    print(f"\n" + "="*70)
    print("VIEW COMPARISON BETWEEN SCHEMAS")
    print("="*70)
    
    # Split by schema
    api_views = df[df[schema_col] == 'CPS_DSCI_API']
    br_views = df[df[schema_col] == 'CPS_DSCI_BR']
    
    # Get view names
    api_names = set(api_views[name_col].tolist()) if not api_views.empty else set()
    br_names = set(br_views[name_col].tolist()) if not br_views.empty else set()
    
    # Find differences
    only_in_api = api_names - br_names
    only_in_br = br_names - api_names
    common_views = api_names & br_names
    
    print(f"CPS_DSCI_API views: {len(api_names)}")
    print(f"CPS_DSCI_BR views: {len(br_names)}")
    print(f"Common views: {len(common_views)}")
    print(f"Only in API: {len(only_in_api)}")
    print(f"Only in BR: {len(only_in_br)}")
    
    if only_in_api:
        print(f"\nVIEWS ONLY IN CPS_DSCI_API:")
        for view in sorted(only_in_api):
            print(f"  - {view}")
    
    if only_in_br:
        print(f"\nVIEWS ONLY IN CPS_DSCI_BR:")
        for view in sorted(only_in_br):
            print(f"  - {view}")
    
    if common_views:
        print(f"\nCOMMON VIEWS ({len(common_views)}):")
        for view in sorted(list(common_views)):
            print(f"  - {view}")
            
        # If we have definition column, compare definitions
        if definition_col:
            print(f"\nComparing view definitions...")
            different_definitions = 0
            identical_definitions = 0
            
            for view_name in common_views:
                api_view = api_views[api_views[name_col] == view_name]
                br_view = br_views[br_views[name_col] == view_name]
                
                if not api_view.empty and not br_view.empty:
                    api_def = str(api_view.iloc[0][definition_col]) if definition_col else ''
                    br_def = str(br_view.iloc[0][definition_col]) if definition_col else ''
                    
                    if api_def != br_def:
                        print(f"    DIFFERENT: {view_name}")
                        different_definitions += 1
                    else:
                        identical_definitions += 1
            
            print(f"\nDefinition comparison:")
            print(f"  Identical definitions: {identical_definitions}")
            print(f"  Different definitions: {different_definitions}")

def save_view_results(df):
    """Save view results to files."""
    
    if df.empty:
        print("No views to save.")
        return
    
    print(f"\n" + "="*70)
    print("SAVING VIEW RESULTS")
    print("="*70)
    
    # Save all views
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    df.to_csv(f'cps_dsci_views_{timestamp}.csv', index=False)
    df.to_csv('cps_dsci_views.csv', index=False)
    
    print(f"Views saved to:")
    print(f"  cps_dsci_views_{timestamp}.csv")
    print(f"  cps_dsci_views.csv")
    
    # Find schema column
    schema_col = None
    for col in df.columns:
        if 'schema' in col.lower() and 'table' in col.lower():
            schema_col = col
            break
    
    if schema_col:
        # Save by schema
        for schema in df[schema_col].unique():
            schema_views = df[df[schema_col] == schema]
            if not schema_views.empty:
                filename = f'{schema.lower()}_views.csv'
                schema_views.to_csv(filename, index=False)
                print(f"  {filename}")

def main():
    """Main function to analyze views."""
    
    sf_env = 'prod'
    
    try:
        print("ANALYZING VIEWS IN CPS_DSCI SCHEMAS")
        
        # Get views
        views_df = get_all_views(sf_env)
        
        if views_df.empty:
            print("No views found in CPS_DSCI schemas.")
            print("\nThis could mean:")
            print("  1. No views exist in these schemas")
            print("  2. Views are in different schemas")
            print("  3. Insufficient permissions to view them")
            return
        
        # Analyze views
        schema_col, name_col, definition_col = analyze_views(views_df)
        
        # Compare between schemas
        if schema_col and name_col:
            compare_views_between_schemas(views_df, schema_col, name_col, definition_col)
        
        # Save results
        save_view_results(views_df)
        
        print("VIEW ANALYSIS COMPLETE")
        print("Check the CSV files for detailed view information")
        
    except Exception as e:
        print(f"View analysis failed: {e}")

if __name__ == "__main__":
    main()