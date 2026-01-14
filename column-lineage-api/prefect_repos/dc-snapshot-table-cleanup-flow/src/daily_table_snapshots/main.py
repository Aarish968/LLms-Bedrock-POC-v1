from typing import Any, Dict, List

from common_prefect_next.blocks.database import TWarehouse, Warehouse
from common_prefect_next.blocks.environment import Env
from common_prefect_next.handlers import handle_failure
from prefect import flow, get_run_logger

from daily_table_snapshots.common.actions.schema_migration import move_tables_to_api_schema
from daily_table_snapshots.common.actions.table_cleanup import cleanup_tables_by_retention_policy
from daily_table_snapshots.common.logging_utils import setup_extra_loggers
from daily_table_snapshots.common.models.settings import Settings
from daily_table_snapshots.common.queries.retention_policies import TABLE_PATTERNS


@flow(
    name="table-snapshots-cleanup-flow",
    on_failure=[handle_failure],
    on_crashed=[handle_failure],
)
@setup_extra_loggers
def table_snapshots_cleanup_flow(
    warehouse: TWarehouse | Warehouse = "cps_dsci_etl_ext5_wh",
) -> List[Dict[str, Any]]:
    """
    Flow to cleanup old table snapshots that exceed retention policy.

    """
    logger = get_run_logger()
    logger.info("Starting table snapshots cleanup flow")

    settings = Settings(env=Env.prod, warehouse=warehouse)
    engine = settings.get_engine()

    with engine.begin() as conn:
        logger.info("Executing table migration and cleanup")
        
        all_results = []
        target_schema = "CPS_DSCI_API"
        
        logger.info("Moving tables to API schema")
        move_results = move_tables_to_api_schema(conn, TABLE_PATTERNS)
        all_results.extend(move_results)

        logger.info("Cleaning up tables that exceed retention policy")
        cleanup_results = cleanup_tables_by_retention_policy(conn, TABLE_PATTERNS, target_schema)
        all_results.extend(cleanup_results)
        
        # Summary logging
        moved_count = sum(1 for r in all_results if r.operation_type == "MOVE_TABLE" and r.error is None)
        cleaned_count = sum(1 for r in all_results if r.operation_type == "MOVE_CLEANUP" and r.error is None)
        move_errors = sum(1 for r in all_results if r.operation_type.startswith("MOVE") and r.error is not None)
        dropped_count = sum(1 for r in all_results if r.operation_type == "CLEANUP" and r.error is None)
        cleanup_errors = sum(1 for r in all_results if r.operation_type == "CLEANUP" and r.error is not None)
        
        logger.info(
            "Total operations completed: %d tables moved to API schema, %d duplicate tables cleaned, %d tables dropped (retention), %d move errors, %d cleanup errors",
            moved_count,
            cleaned_count,
            dropped_count,
            move_errors,
            cleanup_errors,
        )

    if all_results:
        successful_cleanups = sum(
            1 for r in all_results if r.operation_type == "CLEANUP" and r.error is None
        )
        failed_cleanups = sum(
            1 for r in all_results if r.operation_type == "CLEANUP" and r.error is not None
        )

        logger.info(
            "Total cleanup completed: %d tables dropped, %d failed",
            successful_cleanups,
            failed_cleanups,
        )
        logger.info("=== FLOW FINISHED SUCCESSFULLY ===")
    else:
        logger.info("=== FLOW FINISHED ===")

    return [result.dict() for result in all_results]


if __name__ == "__main__":
    table_snapshots_cleanup_flow(warehouse="cps_dsci_etl_ext5_wh")
