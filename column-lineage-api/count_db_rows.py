#!/usr/bin/env python3
"""
Script to count rows in the database table.
"""

import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from api.dependencies.database import DatabaseManager

def count_database_rows():
    """Count rows in the ACTION_TO_ENDPOINTS_TABLES_MAPPING table."""
    
    print("🔢 Counting rows in database table...")
    print("=" * 50)
    
    try:
        db_manager = DatabaseManager()
        
        if db_manager.mock_mode:
            print("⚠️ Running in mock mode - no actual database connection")
            return
        
        # Count total rows
        count_query = "SELECT COUNT(*) as row_count FROM CPS_DB.CPS_DSCI_BR.ACTION_TO_ENDPOINTS_TABLES_MAPPING"
        result = db_manager.execute_query(count_query)
        
        if result:
            row_count = result[0][0]
            print(f"📊 Total rows in database: {row_count}")
        else:
            print("❌ Failed to get row count")
        
        # Get some sample data to verify
        sample_query = """
        SELECT FRONTEND_FILE, FRONTEND_FUNCTION, HTTP_METHOD, ANALYSIS_TIMESTAMP 
        FROM CPS_DB.CPS_DSCI_BR.ACTION_TO_ENDPOINTS_TABLES_MAPPING 
        ORDER BY ANALYSIS_TIMESTAMP DESC 
        LIMIT 5
        """
        
        sample_result = db_manager.execute_query(sample_query)
        
        if sample_result:
            print("\n📋 Sample rows (most recent):")
            for i, row in enumerate(sample_result, 1):
                print(f"  {i}. {row[0]} - {row[1]} - {row[2]} - {row[3]}")
        
        # Check for any duplicate entries
        duplicate_query = """
        SELECT FRONTEND_FILE, FRONTEND_FUNCTION, HTTP_METHOD, COUNT(*) as count
        FROM CPS_DB.CPS_DSCI_BR.ACTION_TO_ENDPOINTS_TABLES_MAPPING 
        GROUP BY FRONTEND_FILE, FRONTEND_FUNCTION, HTTP_METHOD
        HAVING COUNT(*) > 1
        ORDER BY count DESC
        LIMIT 5
        """
        
        duplicate_result = db_manager.execute_query(duplicate_query)
        
        if duplicate_result:
            print(f"\n🔄 Found {len(duplicate_result)} duplicate entries:")
            for row in duplicate_result:
                print(f"  {row[0]} - {row[1]} - {row[2]} (count: {row[3]})")
        else:
            print("\n✅ No duplicate entries found")
        
    except Exception as e:
        print(f"❌ Error counting database rows: {e}")

if __name__ == "__main__":
    count_database_rows()