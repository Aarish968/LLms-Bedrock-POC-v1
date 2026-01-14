from typing import TYPE_CHECKING

from common_prefect_next.blocks.aws import get_aws_credentials
from common_prefect_next.blocks.data_canvas import get_notification_block
from common_prefect_next.blocks.database import TWarehouse, Warehouse
from common_prefect_next.blocks.environment import TEnv
from common_prefect_next.handlers import handle_failure
from prefect import flow, get_run_logger

from evidence_flows.common.models import (
    InstanceIdPayload,
    SchemaType,
    SerialNumberPayload,
    TCollectorDetailRow,
    TCustDetailRow,
)
from evidence_flows.common.queries import (
    create_collector_hdr_entry,
    create_customer_hdr_entry,
    load_collector_detail,
    load_customer_detail,
)
from evidence_flows.common.resolve_serials import (
    handle_collector_serial_payload,
    handle_customer_serial_payload,
)
from evidence_flows.common.settings import Settings
from evidence_flows.common.validation import (
    handle_collector_instance_payload,
    handle_customer_instance_payload,
)

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


@flow(on_crashed=[handle_failure], on_failure=[handle_failure])  # pyright: ignore[reportCallIssue]
def create_evidence_customer_flow(
    payload: SerialNumberPayload | InstanceIdPayload,
    env: TEnv,
    warehouse: TWarehouse | Warehouse = Warehouse.medium,
) -> None:
    """
    Create evidence customer flow
    """
    logger = get_run_logger()
    logger.info("Starting the customer file upload flow")

    settings = Settings(
        env=env,
        warehouse=warehouse,
        request_id=payload.request_id,
        dc_engagement_id=payload.dc_engagement_id,
        cisco_cco_id=payload.cisco_cco_id,
        notification_id=payload.notification_id,
    )

    engine = settings.get_engine()

    # S3 session and client
    aws_credentials = get_aws_credentials()
    s3_client: "S3Client" = aws_credentials.get_boto3_session().client("s3")  # pyright: ignore[reportAssignmentType]

    notify_block = get_notification_block(env=env)
    notify_block.notification_id = payload.notification_id

    if payload.schema_type == SchemaType.serial_number:
        validated_rows: list[TCustDetailRow] = handle_customer_serial_payload(
            s3_uri=payload.snowflake_uri, settings=settings
        )

    elif payload.schema_type == SchemaType.instance_id:
        validated_rows: list[TCustDetailRow] = handle_customer_instance_payload(
            s3_uri=payload.snowflake_uri,
            settings=settings,
            s3_client=s3_client,
        )
    else:
        msg = (
            f"Unsupported schema type: {payload.schema_type}. "
            "Expected 'serial_number' or 'instance_id'."
        )
        raise ValueError(msg)

    with engine.begin() as conn:
        create_customer_hdr_entry(
            request_id=payload.request_id,
            cisco_cco_id=payload.cisco_cco_id,
            effective_date=payload.effective_date,
            file_name_id=payload.file_name_id,
            source=payload.source,
            note=payload.note or "",
            dc_engagement_id=payload.dc_engagement_id,
            conn=conn,
        )

        load_customer_detail(
            conn=conn,
            parsed_data=validated_rows,
            chunk_size=15000,
            request_id=payload.request_id,
        )

    notify_block.mark_successful(
        message="Customer evidence data successfully processed and stored."
    )


@flow(on_crashed=[handle_failure], on_failure=[handle_failure])  # pyright: ignore[reportCallIssue]
def create_evidence_collector_flow(
    payload: SerialNumberPayload | InstanceIdPayload,
    env: TEnv,
    warehouse: TWarehouse | Warehouse = Warehouse.medium,
) -> None:
    """
    Create evidence collector flow
    """
    logger = get_run_logger()
    logger.info("Starting the collector file upload flow")

    settings = Settings(
        env=env,
        warehouse=warehouse,
        request_id=payload.request_id,
        dc_engagement_id=payload.dc_engagement_id,
        cisco_cco_id=payload.cisco_cco_id,
        notification_id=payload.notification_id,
    )

    engine = settings.get_engine()

    # S3 session and client
    aws_credentials = get_aws_credentials()
    s3_client: "S3Client" = aws_credentials.get_boto3_session().client("s3")  # pyright: ignore[reportAssignmentType]

    notify_block = get_notification_block(env=env)
    notify_block.notification_id = payload.notification_id

    if payload.schema_type == SchemaType.serial_number:
        validated_rows: list[TCollectorDetailRow] = handle_collector_serial_payload(
            s3_uri=payload.snowflake_uri, settings=settings
        )

    elif payload.schema_type == SchemaType.instance_id:
        validated_rows: list[TCollectorDetailRow] = handle_collector_instance_payload(
            s3_uri=payload.snowflake_uri,
            settings=settings,
            s3_client=s3_client,
        )
    else:
        msg = (
            f"Unsupported schema type: {payload.schema_type}. "
            "Expected 'serial_number' or 'instance_id'."
        )
        raise ValueError(msg)

    with engine.begin() as conn:
        create_collector_hdr_entry(
            request_id=payload.request_id,
            cisco_cco_id=payload.cisco_cco_id,
            effective_date=payload.effective_date,
            file_name_id=payload.file_name_id,
            source=payload.source,
            note=payload.note or "",
            dc_engagement_id=payload.dc_engagement_id,
            conn=conn,
        )

        load_collector_detail(
            conn=conn,
            parsed_data=validated_rows,
            chunk_size=15000,
            request_id=payload.request_id,
        )

    notify_block.mark_successful(
        message="Collector evidence data successfully processed and stored."
    )
