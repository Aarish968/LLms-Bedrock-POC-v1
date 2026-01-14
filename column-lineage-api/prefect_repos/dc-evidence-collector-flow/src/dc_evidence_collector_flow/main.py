import json
import pandas as pd
from io import BytesIO
from typing import TYPE_CHECKING
from sqlalchemy import insert

from common_prefect_next.blocks.aws import get_aws_credentials
from common_prefect_next.blocks.database import TWarehouse, Warehouse
from common_prefect_next.blocks.environment import TEnv, Env
from common_prefect_next.logging.models.messages import MessageStatus
from common_prefect_next.blocks.data_canvas import get_notification_block
from common_prefect_next.utils import parse_s3_uri
from prefect import flow, get_run_logger
from dc_evidence_collector_flow.common.models import (
    InputParameters,
    FileMetadata,
    FileLoader,
    ResolvedSerials,
    SerialResolutionResult,
    StoredProcException,
)
from dc_evidence_collector_flow.common.sqls import (
    create_loader_transient_table,
    create_serial_transient_table,
    insert_generator,
    fetch_resolved_serials,
    create_prepped_transient_table,
    create_resolved_transient_table,
    fetch_unscoped_serials,
    fetch_unknown_serials,
    get_resolved_serials,
    insert_statement,
    call_store_proc
)
from dc_evidence_collector_flow.common.settings import Settings, detail_table
from dc_evidence_collector_flow.common.actions import (
    insert_data_generator,
    payload_generator,
    get_instances_from_df
)
from dc_evidence_collector_flow.common.wb import format_workbook


if TYPE_CHECKING:
    from sqlalchemy import Engine


@flow(name="create_evidence_collector")
def create_evidence_collector(
    payload: InputParameters,
    env: TEnv | Env,
    warehouse: TWarehouse | Warehouse = Warehouse.medium,
) -> str:
    """
    Runs scheduled tasks
    :param payload: Input Parameters
    :param env: Env
    :param warehouse: Snowflake db warehouse
    :return: None
    """
    logger = get_run_logger()
    logger.info("Starting the collector file upload flow")

    settings = Settings(
        env=env,
        warehouse=warehouse,
        request_id=payload.request_id,
        requested_by=payload.requested_by,
        engagement_id=payload.dc_engagement_id,
    )

    # snowflake connection object
    snowflake_engine = settings.get_engine()

    # S3 session and client
    aws_credentials = get_aws_credentials()
    aws_session = aws_credentials.get_boto3_session()
    s3_client = aws_session.client("s3")

    notify_block = get_notification_block(env=env)
    notify_block.notification_id = payload.notification_id

    metadata = parse_input_json(
        request_json_loc=payload.request_json_loc,
        s3_client=s3_client,
        notify_block=notify_block
    )
    resolution = run_serial_resolution(
          settings=settings,
          file_metadata=metadata,
          engine=snowflake_engine,
          notify_block=notify_block
    )
    store_collector_data(
        file_metadata=metadata,
        resolution=resolution,
        settings=settings,
        engine=snowflake_engine,
        notify_block=notify_block
    )

    apply_tag_via_stored_proc(
        resolution=resolution,
        settings=settings,
        engine=snowflake_engine,
        notify_block=notify_block
    )
    df_bytes = package_workbook(
        resolution=resolution,
        notify_block=notify_block
    )

    s3_uri = settings.get_s3_uri()
    logger.info(f"Uploading report {s3_uri.file_name} to S3: {s3_uri.bucket}/{s3_uri.key}")

    s3_client.put_object(
        Bucket=s3_uri.bucket,
        Key=s3_uri.key,
        Body=df_bytes,
    )
    logger.info(f"Report uploaded to S3: {s3_uri.bucket}/{s3_uri.key}")
    file_path = f"s3://{s3_uri.bucket}/{s3_uri.key}"

    notify_block.send_download_link(
        url=file_path, status=MessageStatus.result, label="Download Report"
    )

    return file_path


def parse_input_json(request_json_loc: str, s3_client, notify_block) -> FileMetadata:
    """
    Downloads and parses input JSON from S3 into a FileMetadata object.
    """
    logger = get_run_logger()
    logger.info(f"Fetching input JSON from: {request_json_loc}")

    try:
        parsed = parse_s3_uri(request_json_loc)
        response = s3_client.get_object(Bucket=parsed.bucket, Key=parsed.key)
        json_data = json.loads(response["Body"].read().decode("utf-8"))

        metadata = FileMetadata.from_payload(json_data)
        notify_block.send_text("Resolving Serial Numbers to Instance Ids")
        return metadata

    except Exception as e:
        notify_block.send_exception("Could not download JSON from S3", e)
        logger.exception("S3 download failed")
        raise


def run_serial_resolution(
        settings: Settings,
        file_metadata: FileMetadata,
        engine: "Engine",
        notify_block
) -> SerialResolutionResult:
    """Run Serial Resolution"""

    logger = get_run_logger()
    file_loader = FileLoader.from_metadata(
        metadata=file_metadata,
        request_id=settings.request_id,
        requested_by=settings.requested_by,
    )
    table_names = settings.get_table_names
    serial_numbers = {
        row.serial_number for row in file_loader.rows if row.serial_number is not None
    }

    with engine.begin() as conn:
        make_loader_table = create_loader_transient_table(table_names)
        conn.execute(make_loader_table)

        for stmt in insert_generator(data_src=list(serial_numbers), table_names=table_names):
            conn.execute(stmt)

        make_serial_table = create_serial_transient_table(table_names)
        conn.execute(make_serial_table)

        resolved_serials_stmt = fetch_resolved_serials(table_names)
        result = conn.execute(resolved_serials_stmt).all()
        resolved_serials = [
            ResolvedSerials(instance_id=row.instance_id, serial_number=row.serial_number)
            for row in result
        ]
        notify_block.send_text(
            message=f"Found {len(resolved_serials)} previously resolved serial numbers. Starting resolution for unresolved serial numbers",
            status=MessageStatus.result
        )
        make_prepped_table = create_prepped_transient_table(table_names=table_names, dc_engagement_id=settings.dc_engagement_id)
        conn.execute(make_prepped_table)

        make_resolved_table = create_resolved_transient_table(table_names)
        conn.execute(make_resolved_table)

        unscoped_serials_stmt = fetch_unscoped_serials(table_names)
        result: list[str] = conn.execute(unscoped_serials_stmt).scalars().all()
        unscoped_serials = set(result)

        if unscoped_serials:
            notify_block.send_text(
                message=f"Found {len(unscoped_serials)} serial numbers that are out of scope for the current engagement",
                status=MessageStatus.result
            )

        unknown_serials_stmt = fetch_unknown_serials(table_names)
        result: list[str] = conn.execute(unknown_serials_stmt).scalars().all()
        unknown_serials = set(result)

        if unknown_serials:
            notify_block.send_text(
                message=f"Could not resolve {len(unknown_serials)} serial numbers",
                status=MessageStatus.result
            )

        get_resolved_serials_stmt = get_resolved_serials(table_names)
        resolved_serials_df = pd.read_sql(get_resolved_serials_stmt, conn)

        resolved_serials_df["instance_id"] = pd.to_numeric(
            resolved_serials_df["instance_id"], errors="coerce"
        )
        resolved_serials_df["parent_instance_id"] = pd.to_numeric(
            resolved_serials_df["parent_instance_id"], errors="coerce"
        )
        resolved_serials_df = resolved_serials_df.astype(
            {"instance_id": "Int64", "parent_instance_id": "Int64"}
        )
        resolved_serials_df = resolved_serials_df.dropna(subset=["instance_id"])
        df_result = resolved_serials_df.convert_dtypes()

        result_rows = {
            row.serial_number: row.instance_id
            for row in df_result.loc[df_result["score_rank"] == 1][
                ["serial_number", "instance_id"]
            ].itertuples()
        }

        prior_rows = {row.serial_number: row.instance_id for row in resolved_serials}
        serial_numbers_mapping = {**prior_rows, **result_rows}

        notify_block.send_text(
            message=f"Serial Resolution Complete! Engagement #{settings.dc_engagement_id} has {len(serial_numbers_mapping)} resolved serial numbers. Previous: {len(resolved_serials)}",
            status=MessageStatus.result
        )
        logger.info("Serial Resolution Complete")

        for name, table in table_names.items():
            if name != "engagement_tag_tables":
                conn.execute(stmt)

    return SerialResolutionResult(
        sn_mapping=serial_numbers_mapping,
        unscoped_serials=unscoped_serials,
        unknown_serials=unknown_serials,
        prior_resolved_serials=resolved_serials,
        df_result=df_result,
    )


def store_collector_data(
    file_metadata: FileMetadata,
    resolution: SerialResolutionResult,
    settings: Settings,
    engine: "Engine",
    notify_block
):
    logger = get_run_logger()

    logger.info("Storing your Collector Data")
    insert_stmt = insert_statement(file_metadata=file_metadata, settings=settings)

    # Remove rows that could not match
    unmatched_rows = [
        row for row in file_metadata.rows
        if row.serial_number not in resolution.sn_mapping
    ]
    if unmatched_rows:
        notify_block.send_text(
            f"Could not match {len(unmatched_rows)} serial numbers. These will not be stored."
        )
    matched_rows = [
        row for row in file_metadata.rows if row.serial_number in resolution.sn_mapping
    ]
    serial2instance = resolution.sn_mapping

    with engine.begin() as conn:
        conn.execute(insert_stmt)

        for rows in insert_data_generator(
            matched_rows, settings.request_id, serial2instance
        ):
            conn.execute(insert(detail_table), rows)

    notify_block.send_text("Collector Data Stored!")


def apply_tag_via_stored_proc(
        resolution: SerialResolutionResult,
        settings: Settings,
        engine: "Engine",
        notify_block
):
    """
    call the stored procedure TAG_INSTANCES_11 which will handle applying this tag
    """
    logger = get_run_logger()
    notify_block.send_text("Applying 'Resolved' tag to Serial Numbers")

    multis = resolution.get_multis()
    multis_shared_parent = resolution.get_multis_same_parent()
    instance_ids = get_instances_from_df(multis_shared_parent) | get_instances_from_df(
        multis
    )

    try:
        with engine.begin() as conn:
            for payload in payload_generator(instance_ids, settings):
                stmt = call_store_proc().bindparams(
                    payload=json.dumps(payload, separators=(",", ":"))
                )
                result_raw = conn.execute(stmt).scalar()

                try:
                    parsed = json.loads(result_raw)
                    if (
                        isinstance(parsed, dict)
                        and "Error type" in parsed
                        and "SQLERRM" in parsed
                    ):
                        raise StoredProcException(f"{parsed['Error type']} {parsed['SQLERRM']}")

                    result = parsed

                except (json.JSONDecodeError, TypeError):
                    print(f"Error parsing stored proc result: {result_raw=}")
                    result = result_raw

                logger.info(f"Result of Stored Proc: {result}")

    except Exception as e:
        notify_block.send_text(
            message=f"Error Applying 'Resolved' Tag {e}. Continuing run",
            status=MessageStatus.error
        )

    notify_block.send_text("Tag 'Resolved' Applied to Serial Numbers")


def package_workbook(
    resolution: SerialResolutionResult,
    notify_block
):
    """
    create an Excel Workbook
    """
    logger = get_run_logger()
    notify_block.send_text("Creating Excel Workbook with Serial Resolution Results")

    stats = resolution.get_statistics()
    logger.info(f"resolution stats: {stats}")

    df_result = resolution.df_result
    unknown_serials = resolution.unknown_serials
    prior_resolved = resolution.prior_resolved_serials

    cols = df_result.columns.tolist()
    cols.pop(cols.index("install_base_status"))
    cols.insert(cols.index("coverage_status") + 1, "install_base_status")

    df_single = resolution.get_singles()
    df_multi = resolution.get_multis()
    df_multi_same_parent = resolution.get_multis_same_parent()

    cols.pop(cols.index("is_multi"))
    cols.pop(cols.index("_multi_group_label"))

    df_single = df_single[cols]
    df_multi = df_multi[cols]
    df_multi_same_parent = df_multi_same_parent[cols]

    df_single = df_single.sort_values(
        by=["serial_number"],
        ignore_index=True,
        ascending=[False],
    )
    df_multi = df_multi.sort_values(
        by=["serial_number", "score_rank"],
        ignore_index=True,
        ascending=[False, True],
    )
    df_multi_same_parent.sort_values(
        by=["serial_number", "score_rank"],
        ignore_index=True,
        ascending=[False, True],
    )
    df_unknown = pd.DataFrame(unknown_serials, columns=["serial_number"])

    fp = BytesIO()
    writer = pd.ExcelWriter(fp, engine="xlsxwriter")

    if len(df_unknown) > 0:
        df_unknown.to_excel(writer, sheet_name="Missing", index=False)

    df_single.to_excel(writer, sheet_name="Single", index=False)
    df_multi.to_excel(writer, sheet_name="Multi", index=False)
    df_multi_same_parent.to_excel(writer, sheet_name="Multi_with_same_Parent", index=False)
    df_resolved = pd.DataFrame(
        [(row.serial_number, row.instance_id) for row in prior_resolved],
        columns=["serial_number", "instance_id"],
    )
    df_resolved.to_excel(writer, sheet_name="Previously Resolved", index=False)

    format_workbook(writer, df_multi, df_single, df_multi_same_parent)
    writer.close()

    fp.seek(0)

    return fp.getvalue()

