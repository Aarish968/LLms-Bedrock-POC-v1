#!/usr/bin/env python3
"""
Simple script to restore Snowflake tables from S3 using INFER_SCHEMA and USING TEMPLATE.
Uses the Snowflake approach with MATCH_BY_COLUMN_NAME for data loading.
Based on the working SQL pattern:
1. INFER_SCHEMA to detect schema from parquet
2. USING TEMPLATE to create table structure
3. COPY INTO with MATCH_BY_COLUMN_NAME to load data
"""

from typing import List, Optional

from common import sec
from sqlalchemy import create_engine, text


def get_correct_schema(env: str) -> str:
    """Get the correct schema based on environment."""
    if env == "prod":
        return "CPS_DSCI_API"
    elif env == "stage":
        return "CPS_DSCI_STG"
    else:
        return "CPS_DSCI_BR"


def check_env(env: str) -> str:
    """Check and return the correct connection name for environment."""
    if env == "dev":
        cn = "dev_cps_dsci_etl_svc"
    elif env in ["stage", "prod"]:
        cn = "prd_cps_dsci_etl_svc"
    else:
        cn = env
    return cn


def create_sf_connection_engine(sf_env: str):
    """Create Snowflake connection engine."""
    try:
        cn = check_env(sf_env)
        correct_schema = get_correct_schema(sf_env)
        print(f" Connecting to environment: {sf_env}")
        print(f" Using schema: {correct_schema}")

        engine = create_engine(
            sec.get_sf_pw(cn, "CPS_DSCI_ETL_EXT4_WH", correct_schema)
        )
        return engine
    except Exception as e:
        print(f" Failed to create Snowflake connection: {e}")
        raise


def get_parquet_files_from_stage(stage_name: str, s3_path: str, engine) -> List[str]:
    """Dynamically get list of parquet files from S3 stage."""
    try:
        with engine.connect() as connection:
            list_sql = f"LIST @{stage_name}/{s3_path}"
            print(f" Listing files: {list_sql}")

            result = connection.execute(text(list_sql))
            files = result.fetchall()

            # Filter for parquet files
            parquet_files = []
            for file_info in files:
                file_path = file_info[0]
                file_name = file_path.split("/")[-1]
                if file_name.endswith(".parquet") or file_name.endswith(
                    ".snappy.parquet"
                ):
                    parquet_files.append(file_name)

            print(f" Found {len(parquet_files)} parquet files:")
            for file in parquet_files:
                print(f" {file}")

            return parquet_files

    except Exception as e:
        print(f" Error listing parquet files: {e}")
        return []


def test_schema_inference(
    stage_name: str, s3_path: str, parquet_file: str, file_format, engine
) -> Optional[List]:
    """Test schema inference on a parquet file using INFER_SCHEMA."""

    full_path = f"{s3_path}{parquet_file}"

    try:
        with engine.connect() as connection:
            # Test INFER_SCHEMA - matches working SQL pattern
            infer_sql = f"""
            SELECT *
            FROM TABLE(INFER_SCHEMA(
                LOCATION => '@{stage_name}/{full_path}',
                FILE_FORMAT => '{file_format}'
            ))
            """

            print(f" Testing schema inference on: {parquet_file}")
            print(f" SQL: {infer_sql}")

            result = connection.execute(text(infer_sql))
            schema_info = result.fetchall()

            print(f" Schema inference successful! Found {len(schema_info)} columns:")
            for col_info in schema_info:
                print(f" {col_info}")

            return schema_info

    except Exception as e:
        print(f"Schema inference test failed: {e}")
        return None


def create_table_using_template(
    target_table: str,
    stage_name: str,
    s3_path: str,
    parquet_file: str,
    file_format,
    engine,
) -> bool:
    """Create table using USING TEMPLATE with INFER_SCHEMA - matches working SQL pattern."""

    full_path = f"{s3_path}{parquet_file}"

    try:
        with engine.connect() as connection:
            # Create table using USING TEMPLATE approach - exact match to working SQL
            create_sql = f"""
            CREATE OR REPLACE TABLE {target_table}
            USING TEMPLATE (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                FROM TABLE(
                    INFER_SCHEMA(
                        LOCATION => '@{stage_name}/{full_path}',
                        FILE_FORMAT => '{file_format}'
                    )
                )
            )
            """

            print("Creating table using USING TEMPLATE:")
            print(create_sql)

            connection.execute(text(create_sql))
            print(f" Table {target_table} created successfully using USING TEMPLATE")

            return True

    except Exception as e:
        print(f"Error creating table {target_table}: {e}")
        return False


def load_data_from_parquet(
    target_table: str, stage_name: str, s3_path: str, parquet_files: List[str], engine
) -> bool:
    """Load ALL parquet files using SINGLE COPY command with pattern matching
    Uses Snowflake's native pattern matching to load ALL files in one command.
    """

    try:
        # Use explicit transaction with commit
        with engine.begin() as connection:
            # Use pattern matching to load ALL files in ONE command
            copy_sql = f"""
            COPY INTO {target_table}
            FROM '@{stage_name}/{s3_path}'
            PATTERN = '.*\.parquet'
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            """

            print(
                f" Executing single COPY command for all {len(parquet_files)} files..."
            )
            print(f" SQL: {copy_sql}")

            import time

            start_time = time.time()

            result = connection.execute(text(copy_sql))
            copy_results = result.fetchall()

            print(" COPY command executed, committing transaction...")
            # Transaction will be automatically committed when exiting the 'with' block

            elapsed = time.time() - start_time

            # Parse results
            successful_files = 0
            failed_files = []
            total_rows = 0

            print(f"\n COPY command completed in {elapsed:.1f} seconds!")
            print(f" Processing {len(copy_results)} file results...")

            for copy_result in copy_results:
                if len(copy_result) >= 4:
                    file_name = copy_result[0].split("/")[-1]
                    status = copy_result[1]
                    rows_parsed = copy_result[2] if copy_result[2] is not None else 0
                    rows_loaded = copy_result[3] if copy_result[3] is not None else 0

                    if status == "LOADED" and rows_loaded > 0:
                        successful_files += 1
                        total_rows += rows_loaded
                    else:
                        failed_files.append(
                            (file_name, f"Status: {status}, Rows: {rows_loaded}")
                        )

            # Immediate verification - check actual table row count
            verify_sql = f"SELECT COUNT(*) FROM {target_table}"
            verify_result = connection.execute(text(verify_sql))
            actual_count = verify_result.fetchone()[0]

            print("\n ULTRA-FAST Loading Summary:")
            print(f"   Execution time: {elapsed:.1f} seconds")
            print(f"   Total files processed: {len(copy_results)}")
            print(f"   Successful files: {successful_files}")
            print(f"   Failed files: {len(failed_files)}")
            print(
                f"   Success rate: {(successful_files / len(copy_results) * 100):.1f}%"
            )
            print(f"   Total rows loaded (COPY result): {total_rows:,}")
            print(f"   Actual table row count: {actual_count:,}")

            if actual_count != total_rows:
                print(
                    f"Mismatch between COPY result ({total_rows:,}) and actual count ({actual_count:,})"
                )

            if failed_files and len(failed_files) <= 10:
                print("\n Failed files:")
                for file_name, error in failed_files[:10]:
                    print(f"   - {file_name}: {error}")
            elif failed_files:
                print(f"\n {len(failed_files)} files failed (showing first 10 only)")

            return successful_files > 0

    except Exception as e:
        print(f"Error in ultra-fast loading: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_data_access(target_table: str, engine, limit: int = 10):
    """Test data access by selecting sample rows with detailed debugging."""

    try:
        with engine.connect() as connection:
            # First check row count
            count_sql = f"SELECT COUNT(*) FROM {target_table}"
            print(f" Checking row count: {count_sql}")

            count_result = connection.execute(text(count_sql))
            row_count = count_result.fetchone()[0]
            print(f" Table row count: {row_count:,}")

            if row_count == 0:
                print(f" Table {target_table} has 0 rows despite successful COPY!")

                # Check if table exists and has correct structure
                desc_sql = f"DESCRIBE TABLE {target_table}"
                print(f" Checking table structure: {desc_sql}")
                desc_result = connection.execute(text(desc_sql))
                columns = desc_result.fetchall()
                print(f" Table has {len(columns)} columns:")
                for col in columns[:5]:  # Show first 5 columns
                    print(f"   - {col[0]}: {col[1]}")

                return False

            # Test data access
            test_sql = f"SELECT * FROM {target_table} LIMIT {limit}"
            print(f" Testing data access: {test_sql}")

            result = connection.execute(text(test_sql))
            sample_data = result.fetchall()

            print(f" Successfully accessed {len(sample_data)} sample rows")
            if sample_data:
                print("Sample data (first few columns):")
                for i, row in enumerate(sample_data[:3]):
                    print(f"   Row {i + 1}: {str(row)[:100]}...")

            return len(sample_data) > 0

    except Exception as e:
        print(f" Error testing data access: {e}")
        import traceback

        traceback.print_exc()
        return False


def restore_table_modern(
    stage_name: str,
    s3_path: str,
    target_table: str,
    sf_env: str,
    file_format,
    execute: bool = False,
):
    """Restore a table from snapshot using modern Snowflake approach:
    1. Use INFER_SCHEMA to detect schema from parquet files
    2. Use USING TEMPLATE to create table with proper structure
    3. Use COPY INTO with MATCH_BY_COLUMN_NAME to load data from all parquet files

    This matches the working SQL pattern provided.
    """
    # Create engine
    engine = create_sf_connection_engine(sf_env)

    # Step 1: Get parquet files dynamically
    parquet_files = get_parquet_files_from_stage(stage_name, s3_path, engine)

    if not parquet_files:
        print(" No parquet files found. Cannot proceed with restore.")
        return False

    # Use first parquet file for schema inference
    primary_file = parquet_files[0]
    print(f" Using primary file for schema: {primary_file}")

    # Step 2: Test schema inference
    schema_info = test_schema_inference(
        stage_name, s3_path, primary_file, file_format, engine
    )

    if not schema_info:
        print(" Schema inference failed. Cannot proceed with restore.")
        return False

    if not execute:
        print("\n Preview Mode - SQL Commands that would be executed:")
        print("=" * 80)

        # Show CREATE TABLE command
        full_path = f"{s3_path}{primary_file}"
        create_sql = f"""CREATE OR REPLACE TABLE {target_table}
                    USING TEMPLATE (
                        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                        FROM TABLE(
                            INFER_SCHEMA(
                                LOCATION => '@{stage_name}/{full_path}',
                                FILE_FORMAT => '{file_format}'
                            )
                        )
                    );"""
        print("-- Step 1: Create table using USING TEMPLATE")
        print(create_sql)

        # Show COPY commands
        print("\n-- Step 2: Load data from parquet files")
        for parquet_file in parquet_files:
            full_path = f"{s3_path}{parquet_file}"
            copy_sql = f"""COPY INTO {target_table}
            FROM '@{stage_name}/{full_path}'
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;"""
            print(f"-- Loading from {parquet_file}")
            print(copy_sql)

        print("\n Add execute=True to run the SQL commands")
        return True

    # Step 3: Create table using USING TEMPLATE
    print(f"\n Step 1: Creating table {target_table}")
    if not create_table_using_template(
        target_table, stage_name, s3_path, primary_file, file_format, engine
    ):
        return False

    # Step 4: Load data from all parquet files
    print(f"\n Step 2: Loading data from {len(parquet_files)} parquet files")
    if not load_data_from_parquet(
        target_table, stage_name, s3_path, parquet_files, engine
    ):
        return False

    # Step 5: Test data access
    print("\n Step 3: Testing data access")
    test_data_access(target_table, engine)

    print("\n Restore completed successfully!")
    print(
        f" Table {target_table} has been restored from {len(parquet_files)} parquet files"
    )

    return True


def list_snapshots(stage_name: str, sf_env: str):
    """List available snapshots in the stage."""

    engine = create_sf_connection_engine(sf_env)

    with engine.connect() as connection:
        result = connection.execute(text(f"LIST @{stage_name}/"))
        files = result.fetchall()

        snapshots = set()
        for file_info in files:
            file_path = file_info[0]
            # Extract folder names
            if "/" in file_path:
                parts = file_path.split("/")
                # Check for the pattern "snapshot_name/filename"
                if len(parts) >= 2 and parts[-1]:  # parts[-1] is the filename
                    folder_name = parts[-2]
                    if folder_name and not folder_name.endswith(".parquet"):
                        snapshots.add(folder_name)

        print(" Available snapshots:")
        for snapshot in sorted(snapshots):
            print(f"  {snapshot}")

    return snapshots


if __name__ == "__main__":
    # Dynamic configuration - matches the working SQL example
    # TABLE_NAME = "DAILY_COVERAGE_CONTRACT_2024_11_22"
    STAGE_NAME = "SNAPSHOTS_STG"
    S3_PATH = "ALL_TAGS_TBL_2023_11_29/"
    TARGET_TABLE = "CPS_DSCI_STG.ALL_TAG_TEST"
    SF_ENV = "prod"
    EXECUTE = True  # Set to True to actually run the restore
    FILE_FORMAT = "PARQUET_FORMAT"

    #  single COPY command handles everything

    try:
        # List available snapshots first
        print("\n Available snapshots:")
        list_snapshots(STAGE_NAME, SF_ENV)

        # Restore the table using modern approach
        print("\n Starting restoration:")
        success = restore_table_modern(
            stage_name=STAGE_NAME,
            s3_path=S3_PATH,
            target_table=TARGET_TABLE,
            file_format=FILE_FORMAT,
            sf_env=SF_ENV,
            execute=EXECUTE,
        )

        if success:
            print(" Restore process completed successfully!")
        else:
            print(" Restore process failed!")

    except Exception as e:
        print(f" Restore failed: {e}")
        import traceback

        traceback.print_exc()
