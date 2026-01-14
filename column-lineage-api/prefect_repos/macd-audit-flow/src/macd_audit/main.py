import gzip
import io
import json
from io import BytesIO
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING

from common_prefect_next.blocks.aws import S3StageFileUri, get_aws_credentials
from common_prefect_next.blocks.data_canvas import (
    DataCanvasNotificationBlock,
    MessageStatus,
    get_notification_block,
)
from common_prefect_next.blocks.database import TWarehouse
from common_prefect_next.handlers.failure import handle_failure
from common_prefect_next.tasks.excel import write_to_excel
from common_prefect_next.utils import setup_extra_loggers
from common_prefect_next.utils.aws_utils import S3UriComponents, parse_s3_uri
from prefect import flow, get_run_logger, task

from macd_audit.common.models import (
    MacdAuditPayload,
    MacdAuditPayloadInstanceId,
    MacdAuditPayloadSerialNumber,
    MacdAuditSchemaType,
    Settings,
    TEnv,
)
from macd_audit.common.queries.checks import get_static_tags_table_exists
from macd_audit.common.queries.macd_audit import get_macd_audit

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


@task()
def resolve_serials(
    serial_numbers: list[str],
    settings: Settings,
) -> set[int]:
    """
    Resolve serials but inform requestor of outcome and provide traceability
    """
    from common_serial_resolution.steps.resolve import run_serial_resolution

    notify_block = get_notification_block(env=settings.env)
    notify_block.notification_id = settings.notification_id

    aws_credentials = get_aws_credentials()
    s3_client = aws_credentials.get_s3_client()

    with NamedTemporaryFile(delete_on_close=False, delete=False) as temp_file_obj:
        tmp_file = Path(temp_file_obj.name)

    notify_block.send_text("Starting Serial Resolution")

    audit_data = run_serial_resolution(
        serial_numbers=serial_numbers,
        request_id=settings.request_id,
        dc_engagement_id=settings.dc_engagement_id,
        comment="Triggered from MACD Audit",
        requestor=settings.requested_by,
        excel_file_path=tmp_file,
        s3_client=s3_client,
        get_engine=settings.get_engine,
        env=settings.env,
    )

    notify_block.send_text("Finished Serial Resolution")
    notify_block.send_table(audit_data["summary_"])

    # Our tmp file is a local Excel file, we need to upload it to S3
    bucket, key = settings.get_serial_resolution_upload_loc()
    s3_client.upload_file(Filename=str(tmp_file), Bucket=bucket, Key=key)

    try:
        tmp_file.unlink()
    except Exception as e:
        logger = get_run_logger()
        logger.warning("Failed to delete temp file %s: %s", tmp_file, e)

    notify_block.send_download_link(
        f"s3://{bucket}/{key}", label="Download Serial Resolution"
    )
    serial2instance = {
        item.serial_number: item.instance_id for item in audit_data["resolved"]
    }

    found: set[int] = {
        val
        for serial in serial_numbers
        if (val := serial2instance.get(serial)) is not None
    }
    not_found = {
        serial for serial in serial_numbers if serial2instance.get(serial) is None
    }

    if not_found:
        msg = f"Some serial numbers were not found: {', '.join(not_found)} and will not be included in the report"
        notify_block.send_text(msg)
    else:
        msg = f"{len(serial_numbers)} Serial Numbers were resolved to {len(found)} Instance IDs"
        notify_block.send_text(msg)

    return found


def download_data(s3_uri: S3UriComponents, client: "S3Client") -> list[dict]:
    buffer = io.BytesIO()
    client.download_fileobj(s3_uri.bucket, s3_uri.key, buffer)
    buffer.seek(0)
    with gzip.GzipFile(fileobj=buffer) as gz:
        data = gz.read()
    return json.loads(data)


def stage_instance_ids(
    instance_ids: set[int], s3_client: "S3Client", settings: Settings
) -> S3StageFileUri:
    # Get the stage block so we can reference a json.gz file in later queries
    stage_block = settings.get_stage_block()
    file_name = f"{settings.cisco_cco_id}_{settings.request_id}.json.gz"

    data = json.dumps(
        [{"instance_id": instance_id} for instance_id in instance_ids]
    ).encode("utf-8")

    buffer = BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
        # noinspection PyTypeChecker,PydanticTypeChecker
        gz.write(data)

    buffer.seek(0)

    staged_file_uri = stage_block.make_staged_s3_uri(
        workflow="macd_audit/flat",
        file_name=file_name,
    )

    parsed = parse_s3_uri(staged_file_uri.s3_uri)

    s3_client.upload_fileobj(
        Fileobj=buffer,
        Bucket=parsed.bucket,
        Key=parsed.key,
    )

    return staged_file_uri


def stage_data(
    payload: MacdAuditPayloadSerialNumber | MacdAuditPayloadInstanceId,
    notify_block: "DataCanvasNotificationBlock",
    settings: Settings,
    s3_client: "S3Client",
) -> S3StageFileUri:
    logger = get_run_logger()
    s3_file = parse_s3_uri(payload.file_uri)
    data = download_data(s3_uri=s3_file, client=s3_client)

    if not isinstance(data, list):
        logger.error("Data is not a list")
        msg = "Data is not a list"
        notify_block.send_text(msg, status=MessageStatus.error)
        raise TypeError(msg)

    logger.info("Data size: %s", len(data))

    if payload.schema_type == MacdAuditSchemaType.serial_number:
        logger.info("Schema Type: Serial Number, resolving to Instance IDs")
        notify_block.send_text(f"Resolving {len(data)} Serial Numbers to Instance IDs")
        serial_numbers = [str(item["serial_number"]) for item in data]
        instance_ids = resolve_serials(serial_numbers=serial_numbers, settings=settings)
    elif payload.schema_type == MacdAuditSchemaType.instance_id:
        logger.info("Schema Type: Instance ID")
        instance_ids = {int(item["instance_id"]) for item in data}
    else:
        msg = f"Unsupported schema type: {payload.schema_type}"
        notify_block.send_text(msg, status=MessageStatus.error)
        raise ValueError(msg)

    if not instance_ids:
        logger.error("No Instance IDs available in the data")
        msg = "No Instance IDs available in the data"
        notify_block.send_text(msg, status=MessageStatus.error)
        raise ValueError(msg)

    staged_file_uri = stage_instance_ids(
        instance_ids=instance_ids, s3_client=s3_client, settings=settings
    )
    logger.info(
        "Staged file URI: %s, Snowflake URI: %s",
        staged_file_uri.s3_uri,
        staged_file_uri.snowflake_uri,
    )
    return staged_file_uri


@flow(on_failure=[handle_failure], on_crashed=[handle_failure])
@setup_extra_loggers
def macd_audit_flow(
    payload: MacdAuditPayload,
    env: TEnv,
    warehouse: TWarehouse,
) -> None:
    logger = get_run_logger()
    settings = Settings(
        env=env,
        warehouse=warehouse,
        request_id=payload.request_id,
        dc_engagement_id=payload.dc_engagement_id,
        requested_by=payload.requested_by,
        notification_id=payload.notification_id,
    )
    get_engine = settings.get_engine

    notify_block = get_notification_block(env=env)
    notify_block.notification_id = payload.notification_id
    notify_block.send_text("Starting MACD Audit")

    engine = get_engine()
    with engine.begin() as conn:
        static_tags_exists = get_static_tags_table_exists(
            dc_engagement_id=settings.dc_engagement_id,
            db_schema=settings.db_schema,
            conn=conn,
        )

    if not static_tags_exists:
        logger.error(
            "Static tags table does not exist for engagement ID %s in schema %s. Stopping flow.",
            settings.dc_engagement_id,
            settings.db_schema,
        )
        msg = "In order to run the MACD Audit, you must first have tagged at least one instance_id in the engagement. Please tag at least one instance_id and try again."

        notify_block.send_text(msg, status=MessageStatus.error)
        return

    aws_credentials = get_aws_credentials()
    s3_client = aws_credentials.get_s3_client()

    if payload.schema_type in (
        MacdAuditSchemaType.instance_id,
        MacdAuditSchemaType.serial_number,
    ):
        staged_file_uri = stage_data(
            payload=payload,
            notify_block=notify_block,
            settings=settings,
            s3_client=s3_client,
        )
        snowflake_uri = staged_file_uri.snowflake_uri
    else:
        snowflake_uri = None
        logger.info("Skipping stage data for schema type: %s", payload.schema_type)

    with engine.begin() as conn:
        df = get_macd_audit(
            dc_engagement_id=settings.dc_engagement_id,
            snowflake_uri=snowflake_uri,
            conn=conn,
            db_schema=settings.db_schema,
            period_start_date=payload.period_start_date,
            period_end_date=payload.period_end_date,
        )

    if df.empty:
        msg = "No data available in the MACD Audit"
        notify_block.send_text(msg, status=MessageStatus.error)
        return

    with NamedTemporaryFile(delete_on_close=False, delete=False) as temp_file_obj:
        out = Path(temp_file_obj.name)

    write_to_excel(df=df, output=out, sheet_name="MACD Audit")

    bucket, key = settings.get_upload_loc(workflow_name="macd_audit", extension=".xlsx")

    s3_client.upload_file(Filename=str(out), Bucket=bucket, Key=key)

    notify_block.send_text("Finished MACD Audit")
    notify_block.send_download_link(
        url=f"s3://{bucket}/{key}",
        label="Download MACD Audit",
        status=MessageStatus.result,
    )
