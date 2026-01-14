import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, TypedDict

import pandas as pd
from sqlalchemy import text

from common_serial_resolution.queries.get_audit import (
    get_all_resolved_serials,
    get_duplicated_resolved_serials,
    get_last_resolved_serials,
    get_not_found_serials,
)
from common_serial_resolution.utils.excel import write_to_excel_workbook

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from sqlalchemy import Connection

    from common_serial_resolution.models import (
        AuditResolvedCurrentSerialRow,
        AuditResolvedSerialRow,
        TableName,
        TableNames,
    )
    from common_serial_resolution.models.sql_models import (
        SerialResolutionProcedureSuccessResponse,
    )


class AuditData(TypedDict):
    """Data structure to hold all audit dataframes."""

    resolved: list["AuditResolvedCurrentSerialRow"]
    tagged: list["AuditResolvedSerialRow"]
    multi: list["AuditResolvedCurrentSerialRow"]
    not_found: set[str]
    summary_: dict


def gather_audit_data_(
    conn: "Connection",
    ranked_table: "TableName",
    resolved_table: "TableName",
    tags_table: "TableName",
) -> AuditData:
    """
    Gather audit data by running SQL queries and converting results to DataFrames.

    Args:
        conn: SQLAlchemy connection
        ranked_table: Name of the ranked table
        tags_table: Name of the tags table
        loader_table: Name of the loader table

    Returns:
        Dictionary containing DataFrames for each audit category
    """
    # Get data from all the audit queries
    resolved_data = get_last_resolved_serials(ranked_table, conn)
    tagged_data = get_all_resolved_serials(ranked_table, tags_table, conn)
    multi_data = get_duplicated_resolved_serials(ranked_table, conn)
    not_found_serials = get_not_found_serials(resolved_table=resolved_table, conn=conn)

    summary_ = {
        "Resolved Serials": len(resolved_data),
        "Tagged Serials": len(tagged_data),
        "Multiple Instance Audit": len(multi_data),
        "Serials Not Found": len(not_found_serials),
    }

    return AuditData(
        resolved=resolved_data,
        tagged=tagged_data,
        multi=multi_data,
        not_found=not_found_serials,
        summary_=summary_,
    )


def export_audit_data_(
    audit_data: AuditData,
    output_path: str | Path,
) -> Path:
    """
    Export audit data to Excel file with multiple sheets.

    Args:
        audit_data: Dictionary containing DataFrames for each audit category
        output_path: Path where the Excel file will be saved
    """
    # Ensure the output directory exists
    output_path = Path(output_path)
    os.makedirs(output_path.parent, exist_ok=True)

    resolved_df = pd.DataFrame(
        [row.model_dump(mode="json") for row in audit_data["resolved"]]
    )
    tagged_df = pd.DataFrame(
        [row.model_dump(mode="json") for row in audit_data["tagged"]]
    )
    multi_df = pd.DataFrame(
        [row.model_dump(mode="json") for row in audit_data["multi"]]
    )
    not_found_df = pd.DataFrame(audit_data["not_found"], columns=["serial_number"])

    exports = [
        ("Resolved", resolved_df),
        ("Tagged", tagged_df),
        ("Multi", multi_df),
        ("Not Found", not_found_df),
    ]

    return write_to_excel_workbook(dfs=exports, output=output_path)


def cleanup_audit_data_(
    proc_result: "SerialResolutionProcedureSuccessResponse", conn: "Connection"
) -> None:
    """
    We're responsible for cleaning up the transient tables created during the serial resolution process.
    """

    stmt_resolved = text(
        """
        DROP TABLE IF EXISTS IDENTIFIER(:resolved_table);
        """
    ).bindparams(resolved_table=str(proc_result.resolved_temp_tbl).lower())

    stmt_ranked = text(
        """
        DROP TABLE IF EXISTS IDENTIFIER(:ranked_table);
        """
    ).bindparams(ranked_table=str(proc_result.resolved_ranked_temp_tbl).lower())

    conn.execute(stmt_resolved)
    conn.execute(stmt_ranked)


def run_serial_resolution_audit(
    conn: "Connection",
    table_names: "TableNames",
    output_path: str | Path,
    proc_result: "SerialResolutionProcedureSuccessResponse",
) -> Path:
    """
    Run the complete serial resolution audit process.

    This function orchestrates the entire audit process by:
    1. Gathering audit data
    2. Exporting audit data to Excel
    3. Cleaning up temporary tables

    Args:
        conn: SQLAlchemy connection
        table_names: Object containing all required table names
        output_path: Path where the Excel file will be saved
        proc_result: Serial resolution procedure result object
    """
    logger.info("Starting serial resolution audit")

    # Step 1: Gather audit data
    logger.info(
        "Gathering audit data from tables: %s, %s, %s",
        table_names.ranked_table,
        table_names.resolved_table,
        table_names.engagement_tags_table,
    )
    audit_data = gather_audit_data_(
        conn=conn,
        ranked_table=table_names.ranked_table,
        resolved_table=table_names.resolved_table,
        tags_table=table_names.engagement_tags_table,
    )

    # Log summary information
    logger.info(
        "Audit data gathered: %s resolved, %s tagged, %s multi, %s not found",
        audit_data["summary_"]["Resolved Serials"],
        audit_data["summary_"]["Tagged Serials"],
        audit_data["summary_"]["Multiple Instance Audit"],
        audit_data["summary_"]["Serials Not Found"],
    )

    # Step 2: Export audit data
    logger.info("Exporting audit data to %s", output_path)
    output_path = export_audit_data_(
        audit_data=audit_data,
        output_path=output_path,
    )

    # Step 3: Clean up temporary tables
    logger.info(
        "Cleaning up temporary tables: %s, %s",
        proc_result.resolved_temp_tbl,
        proc_result.resolved_ranked_temp_tbl,
    )
    cleanup_audit_data_(
        proc_result=proc_result,
        conn=conn,
    )

    logger.info("Serial resolution audit completed successfully")

    return output_path
