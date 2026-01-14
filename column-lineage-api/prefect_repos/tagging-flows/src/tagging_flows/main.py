import gzip
import json

from common_prefect_next.blocks.aws import get_aws_credentials
from common_prefect_next.blocks.data_canvas import get_notification_block
from common_prefect_next.blocks.database import TWarehouse
from common_prefect_next.blocks.environment import TEnv
from common_prefect_next.handlers.failure import handle_failure
from prefect import flow, get_run_logger

from tagging_flows.bulk_tagging import (
    bulk_instance_tagging,
    stage_bulk_instances_from_serial,
)
from tagging_flows.common import setup_extra_loggers
from tagging_flows.common.models import (
    BulkTaggingPayload,
    SerialNumbers,
    Settings,
    TaggingPayload,
    TagInstancesProcedureParams,
    ThoughtSpotTaggingPayload,
)
from tagging_flows.common.queries import get_tag_data
from tagging_flows.common.queries.thoughtspot_tagging import (
    get_thoughtspot_tasks_details,
)
from tagging_flows.instance_tagging import run_instance_tagging
from tagging_flows.serial_tagging import resolve_serials_task
from tagging_flows.thoughtspot_tagging import (
    run_thoughtspot_requests_for_engagement,
)


@flow(on_failure=[handle_failure], on_crashed=[handle_failure])
@setup_extra_loggers
def instance_tagging_flow(
    payload: TaggingPayload,
    env: TEnv,
    warehouse: TWarehouse,
    warehouse_larger: TWarehouse,
) -> None:
    """
    Flow to apply tags to instances based on the provided payload.
    This flow assumes that the instance ids have been staged via a S3StageFileUri
    """

    logger = get_run_logger()
    settings = Settings(
        env=env,
        warehouse=warehouse,
        warehouse_larger=warehouse_larger,
        request_id=payload.request_id,
        cisco_cco_id=payload.cisco_cco_id,
        dc_engagement_id=payload.dc_engagement_id,
        notification_id=payload.notification_id,
    )
    get_engine = settings.get_engine

    notify_block = get_notification_block(env=env)
    notify_block.notification_id = payload.notification_id
    notify_block.send_text("Starting Instance Tagging")

    engine = get_engine()
    with engine.begin() as conn:
        tag_data = get_tag_data(
            tag_ids=list({m.tag_id for m in payload.tagset_tag_ids}), conn=conn
        )
        tag_id_to_name = {tag_row.tag_id: tag_row.tag_name for tag_row in tag_data}

    for i, tag_model in enumerate(payload.tagset_tag_ids, start=1):
        tag_id = tag_model.tag_id
        tag_name = tag_id_to_name.get(tag_id, "Unknown Tag")
        logger.info(
            "[%d of %d] Processing tag: %s (ID: %d)",
            i,
            len(payload.tagset_tag_ids),
            tag_name,
            tag_id,
        )
        notify_block.send_text(
            f"Applying Tag: '{tag_name}' (ID: {tag_id}) to Instance Ids"
        )
        is_last_iter = i == len(payload.tagset_tag_ids)

        with engine.begin() as conn:
            sp_params = TagInstancesProcedureParams(
                cisco_cco_id=payload.cisco_cco_id,
                comment=payload.comment,
                dc_engagement_id=payload.dc_engagement_id,
                ddl_action=payload.ddl_action,
                defer=not is_last_iter,
                snowflake_uri=payload.snowflake_uri,
                tag_id=tag_model.tag_id,
                tagset_id=tag_model.tagset_id,
            )

            result = run_instance_tagging(
                conn=conn,
                sp_params=sp_params,
                warehouse=warehouse,
            )

        if not result.success:
            logger.error("Stored procedure failed: %s", result.message)
            notify_block.mark_error(
                messages=[f"Failed to apply tag '{tag_name}': {result.message}"]
            )
        else:
            logger.info("Tag %s applied successfully", tag_name)
            notify_block.send_text(f"Tag '{tag_name}' applied successfully")

    notify_block.mark_successful(message="Instance tagging completed successfully")


@flow(on_failure=[handle_failure], on_crashed=[handle_failure])
@setup_extra_loggers
def serial_tagging_flow(
    payload: TaggingPayload,
    env: TEnv,
    warehouse: TWarehouse,
    warehouse_larger: TWarehouse,
) -> None:
    """
    As we have serial numbers, we first need to go through common_serial_resolution to get instance_ids
    """
    logger = get_run_logger()
    settings = Settings(
        env=env,
        warehouse=warehouse,
        warehouse_larger=warehouse_larger,
        request_id=payload.request_id,
        cisco_cco_id=payload.cisco_cco_id,
        dc_engagement_id=payload.dc_engagement_id,
        notification_id=payload.notification_id,
    )

    notify_block = get_notification_block(env=env)
    notify_block.notification_id = payload.notification_id
    notify_block.send_text(
        "Starting Serial Tagging - Resolving Serial Numbers to Instance IDs"
    )

    cred_block = get_aws_credentials()
    s3_client = cred_block.get_s3_client()
    logger.info(
        "Fetching data from S3: %s/%s",
        payload.snowflake_uri.bucket,
        payload.snowflake_uri.key,
    )
    data_compressed = s3_client.get_object(
        Bucket=payload.snowflake_uri.bucket, Key=payload.snowflake_uri.key
    )["Body"].read()
    data = gzip.decompress(data_compressed).decode("utf-8")
    logger.info("Data fetched and decompressed successfully")

    serial_numbers_model = SerialNumbers.validate_json(data)
    serial_numbers = [sn.serial_number for sn in serial_numbers_model]
    logger.info("Extracted %d serial numbers from the data", len(serial_numbers))

    serial_to_instance: dict[str, int] = resolve_serials_task(
        settings=settings, serial_numbers=serial_numbers
    )

    resolved: set[int] = set()
    unresolved: set[str] = set()

    for serial_number in serial_numbers:
        instance_id: int | None = serial_to_instance.get(serial_number)
        if instance_id:
            resolved.add(instance_id)
        else:
            unresolved.add(serial_number)

    logger.info(
        "Resolved %d instance IDs from %d serial numbers",
        len(resolved),
        len(serial_numbers),
    )
    if unresolved:
        logger.warning("Unresolved serial numbers: %s", ", ".join(unresolved))
        notify_block.send_text(
            f"{len(unresolved)} Serial Numbers could not be resolved to Instance IDs: {', '.join(unresolved)} and will not be tagged"
        )

    # Now we need to stage the instance ids as the stored procedure expects a staged file with instance IDs
    staged_instance_id_data = json.dumps(
        [{"instance_id": instance_id} for instance_id in resolved]
    )
    staged_instance_id_compress = gzip.compress(staged_instance_id_data.encode("utf-8"))

    staged_instance_uri = settings.get_s3_staged_instance_id()

    logger.info(
        "Staging resolved instance IDs to S3: %s/%s",
        staged_instance_uri.bucket,
        staged_instance_uri.key,
    )
    s3_client.put_object(
        Bucket=staged_instance_uri.bucket,
        Key=staged_instance_uri.key,
        Body=staged_instance_id_compress,
        ContentType="application/json",
        ContentEncoding="gzip",
    )

    logger.info("Resolved instance IDs staged successfully")

    instance_payload = payload.model_copy(update={"snowflake_uri": staged_instance_uri})

    return instance_tagging_flow(
        payload=instance_payload,
        env=env,
        warehouse=warehouse,
        warehouse_larger=warehouse_larger,
    )


@flow(on_failure=[handle_failure], on_crashed=[handle_failure])
@setup_extra_loggers
def bulk_tagging_flow(
    payload: BulkTaggingPayload,
    env: TEnv,
    warehouse: TWarehouse,
    warehouse_larger: TWarehouse,
) -> None:
    logger = get_run_logger()
    settings = Settings(
        env=env,
        warehouse=warehouse,
        warehouse_larger=warehouse_larger,
        request_id=payload.request_id,
        cisco_cco_id=payload.cisco_cco_id,
        dc_engagement_id=payload.dc_engagement_id,
        notification_id=payload.notification_id,
    )

    notify_block = get_notification_block(env=env)
    notify_block.notification_id = payload.notification_id

    if payload.id_type == "serial_number":
        logger.info("Resolving Serial Numbers to Instance IDs for Bulk Tagging")
        notify_block.send_text(
            "Resolving Serial Numbers to Instance IDs for Bulk Tagging"
        )
        resolved_staged_uri = stage_bulk_instances_from_serial(
            snowflake_uri=payload.snowflake_uri, settings=settings
        )
    else:
        resolved_staged_uri = None
    notify_block.send_text("Starting Bulk Tagging")

    result = bulk_instance_tagging(
        payload=payload,
        snowflake_uri=resolved_staged_uri,
        settings=settings,
    )

    if not result.success:
        msg = f"Bulk tagging failed: {result.message}"
        raise RuntimeError(msg)

    notify_block.mark_successful(
        message="Bulk tagging completed successfully",
    )
    return None


@flow(on_failure=[handle_failure], on_crashed=[handle_failure])
@setup_extra_loggers
def thoughtspot_tagging_flow(
    payload: ThoughtSpotTaggingPayload,
    env: TEnv,
    warehouse: TWarehouse,
    warehouse_larger: TWarehouse,
) -> None:
    """
    Follows the same semantics as `instance_tagging_flow` but requires some wrangling of data

    - Supports processing 1+ requests sourced from `dc_thoughtspot_instance_requests` and supplied with `thoughtspot_ids`
    - Each thoughtspot_id will require at least one SP call. Each tagset_id associated with the request requires 1 call.
    - Each request should have an S3 File in CSV Format that was created by the API when the request was made
    - Order matters, tasks should go oldest -> newest
    - As we complete a task we should mark them completed so this is reflected in the UI
    - This flow also handles 'unset' requests. These do not have tag_ids associated, only tagset ids
    """
    logger = get_run_logger()
    settings = Settings(
        env=env,
        warehouse=warehouse,
        warehouse_larger=warehouse_larger,
        request_id=payload.request_id,
        cisco_cco_id=payload.cisco_cco_id,
        dc_engagement_id=payload.dc_engagement_id,
        notification_id=payload.notification_id,
    )

    notify_block = get_notification_block(env=env)
    notify_block.notification_id = payload.notification_id
    notify_block.send_text("Starting ThoughtSpot Tagging Flow")

    cred_block = get_aws_credentials()
    s3_client = cred_block.get_s3_client()
    engine = settings.get_engine()

    logger.info(
        "Processing %d ThoughtSpot tasks for user %d in engagement %d",
        len(payload.thoughtspot_ids),
        payload.dc_user_id,
        payload.dc_engagement_id,
    )

    with engine.begin() as conn:
        enriched_tasks = get_thoughtspot_tasks_details(
            thoughtspot_ids=payload.thoughtspot_ids,
            dc_user_id=payload.dc_user_id,
            conn=conn,
        )

    run_thoughtspot_requests_for_engagement(
        tasks=enriched_tasks,
        config_strategy=payload.config_strategy,
        settings=settings,
        notify_block=notify_block,
        s3_client=s3_client,
    )

    logger.info("ThoughtSpot tagging flow completed successfully")
    notify_block.mark_successful(message="ThoughtSpot tagging completed successfully")
