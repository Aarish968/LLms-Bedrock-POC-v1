from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING

from common_prefect_next.blocks.aws import get_aws_credentials
from common_prefect_next.blocks.data_canvas import get_notification_block
from common_prefect_next.logging.models.messages import (
    MessageStatus,
    MessageType,
    TableMessage,
)
from prefect import get_run_logger, task
from prefect.cache_policies import NO_CACHE

if TYPE_CHECKING:
    from tagging_flows.common.models import Settings


@task(cache_policy=NO_CACHE)
def resolve_serials_task(
    settings: "Settings", serial_numbers: list[str]
) -> dict[str, int]:
    """
    Given a snowflake URI containing serial numbers, this task resolves those serial numbers
    to instance_ids, returning a mapping of serial numbers to instance IDs. The returned mapping does
    not include serial numbers that could not be resolved to an instance ID.
    """
    from common_serial_resolution.steps.resolve import run_serial_resolution

    logger = get_run_logger()
    get_engine = settings.get_engine

    notify_block = get_notification_block(env=settings.env)
    notify_block.notification_id = settings.notification_id
    notify_block.send_text(
        "Starting Serial Tagging - Resolving Serial Numbers to Instance IDs"
    )

    cred_block = get_aws_credentials()
    s3_client = cred_block.get_s3_client()

    logger.info("Extracted %d serial numbers from the data", len(serial_numbers))
    notify_block.send_text(
        f"Resolving {len(serial_numbers)} serial numbers to instance Ids"
    )

    with NamedTemporaryFile(delete_on_close=False, delete=False) as temp_file_obj:
        temp_file = temp_file_obj.name

    logger.info("Running serial resolution with %d serial numbers", len(serial_numbers))

    serial_audit_data = run_serial_resolution(
        get_engine=get_engine,
        serial_numbers=serial_numbers,
        request_id=settings.request_id,
        dc_engagement_id=settings.dc_engagement_id,
        comment="Triggered by Serial Tagging Task",
        requestor=settings.cisco_cco_id,
        excel_file_path=temp_file,
        s3_client=s3_client,
        env=settings.env,
    )

    logger.info(
        "Serial resolution completed with %d resolved serials",
        len(serial_audit_data["tagged"]),
    )
    summary: dict[str, int] = serial_audit_data["summary_"]
    table_message = TableMessage(
        type=MessageType.table,
        data=summary,
    )
    notify_block.send_table(message=table_message)

    excel_upload_loc = settings.get_serial_resolution_upload_loc()
    logger.info(
        "Uploading serial resolution results to S3: %s/%s",
        excel_upload_loc[0],
        excel_upload_loc[1],
    )
    s3_client.upload_file(
        Filename=temp_file, Bucket=excel_upload_loc[0], Key=excel_upload_loc[1]
    )
    notify_block.send_download_link(
        url=f"s3://{excel_upload_loc[0]}/{excel_upload_loc[1]}",
        label="Download Serial Resolution",
        status=MessageStatus.pending,
    )
    logger.info("Serial resolution results uploaded successfully")
    serial_to_instance: dict[str, int] = {
        row.requested_serial: row.instance_id for row in serial_audit_data["tagged"]
    }
    logger.info(
        "Using Serial to Instance mapping for tagging of size %d",
        len(serial_to_instance),
    )

    return serial_to_instance
