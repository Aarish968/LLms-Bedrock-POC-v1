import logging
from typing import TYPE_CHECKING, Callable, Iterable

from common_serial_resolution import CommonResolutionSettings
from common_serial_resolution.models import (
    SerialResolutionProcedureParams,
    TableNames,
    preprocess_serial_numbers,
)
from common_serial_resolution.models.settings import Environment, TEnvironment
from common_serial_resolution.queries.stored_procedure import (
    call_serial_tagging_procedure,
)
from common_serial_resolution.steps.audit import (
    AuditData,
    cleanup_audit_data_,
    export_audit_data_,
    gather_audit_data_,
)
from common_serial_resolution.utils.stage import stage_serial_numbers

if TYPE_CHECKING:
    from pathlib import Path

    from mypy_boto3_s3 import S3Client
    from sqlalchemy import Engine

    from common_serial_resolution.models.sql_models import TSerialResolutionResponse


logger = logging.getLogger(__name__)


def run_serial_resolution_stored_procedure_(
    get_engine: Callable[[], "Engine"],
    serial_numbers: Iterable[str],
    table_names: TableNames,
    s3_client: "S3Client",
    request_id: int,
    dc_engagement_id: int,
    comment: str,
    requestor: str,
    settings: CommonResolutionSettings,
) -> "TSerialResolutionResponse":
    """
    Prepare, stage and call the serial resolution stored procedure.

    This does not gather audit data or export it to Excel.
    """

    sn_proc = preprocess_serial_numbers(serial_numbers=serial_numbers)
    logger.info(
        "After preprocessing serial numbers, %d serial numbers remain", len(sn_proc)
    )
    logger.debug("Table names: %s", table_names)
    logger.info("Staging serial numbers for resolution")

    staged_uri = stage_serial_numbers(
        serial_numbers=sn_proc, settings=settings, s3_client=s3_client
    )
    logger.info(
        "Serial numbers staged to S3: %s, Stage File: %s",
        staged_uri.s3_uri,
        staged_uri.snowflake_uri,
    )

    db_engine = get_engine()

    procedure_params = SerialResolutionProcedureParams(
        request_id=request_id,
        dc_engagement_id=dc_engagement_id,
        cisco_cco_id=requestor,
        comment=comment or "",
        snowflake_uri=staged_uri.snowflake_uri,
    )

    with db_engine.begin() as conn:
        logger.info(
            "Calling the stored procedure with params: %s",
            procedure_params.model_dump(mode="json", by_alias=True),
        )

        # Call the stored procedure to resolve serial numbers
        proc_result = call_serial_tagging_procedure(params=procedure_params, conn=conn)

    if not proc_result.success:
        logger.error(
            "Stored procedure failed with message: %s, code: %d logs: %s",
            proc_result.message,
            proc_result.code,
            proc_result.logs if proc_result.logs else "No logs available",
        )
        msg = (
            f"Stored procedure failed with message: {proc_result.message}, "
            f"code: {proc_result.code}"
        )
        raise ValueError(msg)

    return proc_result


def run_serial_resolution(
    get_engine: Callable[[], "Engine"],
    serial_numbers: Iterable[str],
    request_id: int,
    dc_engagement_id: int,
    comment: str | None,
    requestor: str,
    excel_file_path: "str | Path",
    s3_client: "S3Client",
    env: Environment | TEnvironment,
    settings: CommonResolutionSettings | None = None,
) -> AuditData:
    """
    Main entrypoint for the serial resolution process.

    Args:
        get_engine: Function to get the database engine
        serial_numbers: Iterable of serial numbers to process
        request_id: Request ID for the operation
        dc_engagement_id: Data Center engagement ID
        comment: Optional comment for the request
        requestor: Requestor's Cisco CCO ID
        excel_file_path: Path to save the audit data Excel file
        s3_client: Boto3 S3 client for staging serial numbers
        env: Environment in which the operation is being performed. This will override the
            environment in the settings if provided.
        settings: Optional, CommonResolutionSettings instance with configuration parameters


    Returns:
        AuditData: Dictionary containing DataFrames for each audit category
    """

    settings = settings or CommonResolutionSettings(env=Environment(env))

    table_names = TableNames.from_params(
        request_id=request_id, dc_engagement_id=dc_engagement_id
    )

    proc_result = run_serial_resolution_stored_procedure_(
        get_engine=get_engine,
        serial_numbers=serial_numbers,
        table_names=table_names,
        s3_client=s3_client,
        request_id=request_id,
        dc_engagement_id=dc_engagement_id,
        comment=comment or "",
        requestor=requestor,
        settings=settings,
    )

    db_engine = get_engine()
    with db_engine.begin() as conn:
        logger.info("Gathering audit data")

        # Gather audit data
        audit_data = gather_audit_data_(
            conn=conn,
            ranked_table=table_names.ranked_table,
            resolved_table=table_names.resolved_table,
            tags_table=table_names.engagement_tags_table,
        )

    logger.info("Audit data gathered successfully")

    # Export audit data to Excel file
    logger.info("Exporting audit data to %s", excel_file_path)
    export_audit_data_(audit_data=audit_data, output_path=excel_file_path)

    logger.info(
        "Cleaning up transient tables %s, %s",
        table_names.ranked_table,
        table_names.resolved_table,
    )

    with db_engine.begin() as conn:
        cleanup_audit_data_(proc_result=proc_result, conn=conn)

    return audit_data
