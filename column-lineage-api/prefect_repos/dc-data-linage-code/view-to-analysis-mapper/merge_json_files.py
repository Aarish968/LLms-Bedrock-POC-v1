import json
import re

def extract_table_name(full_table_name):
    """
    Extract table name from schema.table format
    Examples:
    - "CPS_DSCI_API.DC_BOOKINGS_CONTRACTS" -> "DC_BOOKINGS_CONTRACTS"
    - "dc_bookings_contracts" -> "dc_bookings_contracts"
    """
    if '.' in full_table_name:
        return full_table_name.split('.')[-1]
    return full_table_name

def normalize_table_name(table_name):
    """Normalize table name for case-insensitive comparison"""
    return extract_table_name(table_name).lower().strip()

def view_matches_route(view_tables, route_tables):
    """
    Check if all view tables are present in route tables (case-insensitive, schema-agnostic)
    """
    if not view_tables:  # Empty view tables list
        return False
    
    # Normalize route tables (extract table names and convert to lowercase)
    normalized_route_tables = [normalize_table_name(t) for t in route_tables]
    
    # Normalize view tables (extract table names and convert to lowercase)
    normalized_view_tables = [normalize_table_name(t) for t in view_tables]
    
    # Check if all view tables are present in route tables
    return all(view_table in normalized_route_tables for view_table in normalized_view_tables)

def find_matching_views(route_tables, view_to_tables):
    """Find all views that match the given route tables"""
    matching_views = []
    
    for view_name, view_data in view_to_tables.items():
        if view_data and len(view_data) > 0:
            view_tables = view_data[0].get("tables", [])
            if view_matches_route(view_tables, route_tables):
                matching_views.append(view_name)
    
    return matching_views

def merge_json_files(view_to_tables_file, fixed_route_analysis_file, output_file):
    """
    Merge view_to_tables.json and merged_route_analysis_with_table_data.json
    """
    
    # Read view_to_tables.json
    with open(view_to_tables_file, 'r', encoding='utf-8') as f:
        view_to_tables = json.load(f)
    
    # Read fixed_route_analysis.json
    with open(fixed_route_analysis_file, 'r', encoding='utf-8') as f:
        route_analysis = json.load(f)
    
    # Process each route file
    merged_data = {}
    
    for route_file, route_entries in route_analysis.items():
        merged_data[route_file] = []
        
        for route_entry in route_entries:
            # Create a copy of the route entry
            merged_entry = route_entry.copy()
            
            # Get tables from the route entry
            route_tables = route_entry.get("tables", [])
            
            if route_tables:  # Only process if route has tables
                # Find matching views
                matching_views = find_matching_views(route_tables, view_to_tables)
                
                # Add view field if matches found
                if matching_views:
                    merged_entry["view"] = matching_views
            
            merged_data[route_file].append(merged_entry)
    
    # Write merged data to output file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(merged_data, f, indent=2, ensure_ascii=False)
    
    print(f"Merged data written to {output_file}")
    
    # Print summary
    total_routes = sum(len(entries) for entries in merged_data.values())
    routes_with_views = sum(1 for entries in merged_data.values() 
                           for entry in entries if "view" in entry)
    
    print(f"Summary:")
    print(f"- Total routes processed: {total_routes}")
    print(f"- Routes with matching views: {routes_with_views}")
    
    return merged_data

if __name__ == "__main__":
    # File paths
    view_to_tables_file = "view_to_tables.json"
    fixed_route_analysis_file = "merged_route_analysis_with_table_data.json"
    output_file = "merged_analysis.json"
    
    try:
        merged_data = merge_json_files(view_to_tables_file, fixed_route_analysis_file, output_file)
        
        # Show some examples of matches found
        print("\nExamples of routes with matching views:")
        count = 0
        for route_file, entries in merged_data.items():
            for entry in entries:
                if "view" in entry and count < 5:
                    print(f"- {route_file}: {entry['function']} -> Views: {entry['view']}")
                    count += 1
                if count >= 5:
                    break
            if count >= 5:
                break
                
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please ensure both input JSON files exist in the current directory.")
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")