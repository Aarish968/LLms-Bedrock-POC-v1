#!/usr/bin/env python3
"""
Script to verify timestamp format for database insertion.
"""

from datetime import datetime

def show_timestamp_format():
    """Show the timestamp format that will be used in database insertion."""
    
    print("🕐 Timestamp Format Verification")
    print("=" * 50)
    
    # Generate timestamp the same way as in the service
    current_timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    
    print(f"Current UTC Timestamp: {current_timestamp}")
    print(f"Format: YYYY-MM-DD HH:MM:SS.mmm")
    print(f"Timezone: UTC (no timezone info - TIMESTAMP_NTZ)")
    print(f"Precision: Milliseconds (3 decimal places)")
    
    print("\n📝 SQL Example:")
    print(f"INSERT INTO TABLE (..., ANALYSIS_TIMESTAMP, CREATED_AT)")
    print(f"VALUES (..., '{current_timestamp}', '{current_timestamp}')")
    
    print("\n✅ This format is compatible with Snowflake TIMESTAMP_NTZ(9)")

if __name__ == "__main__":
    show_timestamp_format()