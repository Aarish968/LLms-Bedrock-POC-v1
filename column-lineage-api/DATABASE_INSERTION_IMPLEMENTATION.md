# Database Insertion Implementation - FIXED

## Overview
Added database insertion functionality to the repository analysis API that automatically inserts CSV analysis results into the `ACTION_TO_ENDPOINTS_TABLES_MAPPING` Snowflake table.

## Issue Fixed
**Problem:** The original implementation used `%(parameter)s` syntax for SQL parameters, which is not supported by Snowflake through SQLAlchemy. This caused SQL compilation errors:
```
syntax error line 7 at position 20 unexpected '%'
```

**Solution:** Changed to use direct VALUES clause construction with proper string escaping, following the same pattern used in the lineage service.

## Changes Made

### 1. Updated `RepositoryAnalysisService` 
**File:** `api/v1/services/repository_analysis_service.py`

#### New Dependencies
- Added `csv` import for CSV processing
- Added `DatabaseManager` import for database operations

#### Updated Methods

##### `_insert_csv_data_to_database(csv_file_path, job_id)` - FIXED
- **OLD:** Used parameterized queries with `%(parameter)s` syntax
- **NEW:** Uses batch insert with VALUES clause construction
- Reads the generated CSV file
- Auto-detects CSV delimiter
- Maps CSV columns to database columns (handles various column name formats)
- Calls `_batch_insert_data()` for actual insertion
- Returns `True` on success, `False` on failure

##### `_batch_insert_data(insert_data)` - NEW
- Inserts data in batches of 100 rows to avoid query size limits
- Builds VALUES clause with proper string escaping
- Escapes single quotes in data values
- Handles NULL values properly
- Returns count of successfully inserted rows

##### `_get_csv_value(row, possible_keys)` - UNCHANGED
- Helper method to handle different CSV column name variations

#### Modified Methods

##### `__init__()`
- Added `self.db_manager = DatabaseManager()` initialization

##### `run_analysis()`
- After successful CSV creation, calls `_insert_csv_data_to_database()`
- Updates job status message to indicate database insertion success/failure
- Maintains backward compatibility - analysis still succeeds even if database insertion fails

## Database Table Schema - UPDATED FOR LARGE DATA
```sql
CREATE OR REPLACE TABLE CPS_DB.CPS_DSCI_BR.ACTION_TO_ENDPOINTS_TABLES_MAPPING (
    FRONTEND_FILE VARCHAR(200),
    FRONTEND_FUNCTION VARCHAR(200),
    HTTP_METHOD VARCHAR(20),
    FRONTEND_URL VARCHAR(1000),
    BACKEND_FILE VARCHAR(200),
    BACKEND_FUNCTION VARCHAR(200),
    BACKEND_ROUTE VARCHAR(1000),
    DATABASE_TABLES VARCHAR(16777216),          -- Max VARCHAR size for large data
    STORED_PROCEDURES VARCHAR(16777216),        -- Max VARCHAR size for large data
    FLOW_CALLS VARCHAR(16777216),               -- Max VARCHAR size for large data
    RESPONSE_MODEL VARCHAR(1000),
    RESPONSE_FIELDS VARCHAR(16777216),          -- Max VARCHAR size for large data
    NESTED_FIELDS VARCHAR(16777216),            -- Max VARCHAR size for large data
    TABLE_COLUMN_DETAILS VARCHAR(16777216),     -- Max VARCHAR size for large data
    ANALYSIS_TIMESTAMP TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    CREATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
);
```

## Logic Flow

1. **Analysis Completion**: After CSV file is successfully created
2. **Table Check**: Check if `ACTION_TO_ENDPOINTS_TABLES_MAPPING` table exists
3. **Table Creation**: If table doesn't exist, create it using the DDL
4. **Data Truncation**: If table exists, truncate existing data
5. **CSV Reading**: Read the generated CSV file with auto-delimiter detection
6. **Data Processing**: Process CSV rows and prepare for batch insertion
7. **Batch Insertion**: Insert data in batches using VALUES clause with proper escaping
8. **Status Update**: Update job status with database insertion result

## Technical Details

### SQL Syntax Fix
**Before (BROKEN):**
```sql
INSERT INTO TABLE (col1, col2) VALUES (%(param1)s, %(param2)s)
```

**After (WORKING):**
```sql
INSERT INTO TABLE (col1, col2, analysis_timestamp, created_at) 
VALUES ('admin\\\\financial\\\\unverified.py', 'escaped_value2', '2026-01-07 10:30:45.123', '2026-01-07 10:30:45.123')
```

**Key Fix:** Backslashes in Windows file paths are now properly escaped as `\\\\` to prevent Unicode escape sequence errors.

### Timestamp Handling
- **ANALYSIS_TIMESTAMP**: Set to current UTC timestamp when analysis completes
- **CREATED_AT**: Set to current UTC timestamp when record is inserted
- **Format**: `YYYY-MM-DD HH:MM:SS.mmm` (with milliseconds)
- **Timezone**: UTC (TIMESTAMP_NTZ format)

### String Escaping - UPDATED
- Single quotes in data are escaped as `''`
- **Backslashes are escaped as `\\\\`** (fixes Windows file paths)
- NULL values are handled as `NULL` (not quoted)
- All string values are properly quoted
- Timestamps are formatted and quoted as strings

**Common Issues Fixed:**
- Windows file paths like `admin\\financial\\unverified.py` are properly escaped
- Unicode escape sequence errors are prevented
- SQL injection protection through proper escaping

### Large Data Handling - NEW
- **Individual INSERTs**: Changed from batch VALUES clause to individual INSERT statements
- **Reduced Batch Size**: Using batches of 10 rows instead of 100 to handle large data
- **Increased VARCHAR Sizes**: Large fields now use VARCHAR(16777216) - Snowflake's maximum
- **Progress Logging**: Shows progress every 10 inserted rows
- **Error Recovery**: Continues processing even if individual rows fail

### Batch Processing
- Processes data in batches of 10 rows (reduced from 100)
- Uses individual INSERT statements instead of large VALUES clauses
- Prevents query size limit issues with large data
- Provides detailed progress logging

## Error Handling

- **Graceful Degradation**: Analysis succeeds even if database insertion fails
- **Row-Level Error Handling**: Individual row processing errors don't stop the entire process
- **Batch-Level Error Handling**: Failed batches are logged but don't stop subsequent batches
- **Comprehensive Logging**: All operations are logged for debugging
- **Mock Mode Support**: Works in mock mode when no database connection is available

## Testing

Updated `test_database_insertion.py` to verify:
- Table creation/checking functionality
- CSV data insertion (if CSV files exist)
- Batch insertion with sample data
- Error handling and logging

## Usage

The database insertion happens automatically after each successful repository analysis. No changes needed to the API endpoints - the functionality is integrated into the existing `/analyze` workflow.

## Configuration

Ensure your `.env` file has the required Snowflake credentials:
```env
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=CPS_DB
SNOWFLAKE_SCHEMA=CPS_DSCI_BR
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_ROLE=your_role
```

## Benefits

1. **Fixed SQL Syntax**: Now uses Snowflake-compatible SQL syntax
2. **Automatic Data Persistence**: CSV results are automatically stored in Snowflake
3. **Data Consistency**: Table is truncated before each insertion to ensure fresh data
4. **Flexible Column Mapping**: Handles various CSV column name formats
5. **Robust Error Handling**: Continues processing even if individual rows fail
6. **Batch Processing**: Efficient insertion of large datasets
7. **Backward Compatible**: Existing functionality remains unchanged

## Verification

After the fix, you should see logs like:
```
Successfully inserted X rows into ACTION_TO_ENDPOINTS_TABLES_MAPPING table
Data successfully inserted into ACTION_TO_ENDPOINTS_TABLES_MAPPING table
```

Instead of the previous SQL compilation errors.