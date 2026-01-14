import gzip

from common_prefect_next.blocks.aws import S3StageFileUri, get_aws_credentials
from common_prefect_next.blocks.data_canvas import get_notification_block
from prefect import get_run_logger, task
from prefect.cache_policies import NO_CACHE
from pydantic import TypeAdapter

from tagging_flows.common.models import (
    BulkTaggingInstanceRow,
    BulkTaggingPayload,
    BulkTaggingSerialRow,
    Settings,
)
from tagging_flows.common.models.enums import StoredProcedureNames
from tagging_flows.common.models.procedures import (
    BulkTagInstancesProcedureParams,
    StoredProcedureResult,
)
from tagging_flows.common.queries.stored_procedure import run_stored_procedure
from tagging_flows.serial_tagging import resolve_serials_task

InstanceTaggingAdapter = TypeAdapter(list[BulkTaggingInstanceRow])
SerialTaggingAdapter = TypeAdapter(list[BulkTaggingSerialRow])


@task(cache_policy=NO_CACHE)
def bulk_instance_tagging(
    payload: BulkTaggingPayload,
    snowflake_uri: S3StageFileUri | None,
    settings: Settings,
) -> StoredProcedureResult:
    """
    Call the stored procedure for bulk tagging. If 'snowflake_uri' is provided,
    that uri will be passed to the stored procedure as the staged data.
    """

    engine = settings.get_engine()

    params = BulkTagInstancesProcedureParams(
        cisco_cco_id=settings.cisco_cco_id,
        comment=payload.comment,
        dc_engagement_id=settings.dc_engagement_id,
        ddl_action=payload.ddl_action,
        snowflake_uri=snowflake_uri or payload.snowflake_uri,
        id_type="instance_id",
    )

    extra_sp_params = {"logged_user": settings.cisco_cco_id}
    logger = get_run_logger()
    with engine.begin() as conn:
        logger.info("Running stored procedure for bulk tagging")

        result = run_stored_procedure(
            proc_name=StoredProcedureNames.bulk_tagging,
            params=params,
            conn=conn,
            warehouse=settings.pick_warehouse_from_payload(payload),
            **extra_sp_params,
        )

    return result


@task(cache_policy=NO_CACHE)
def stage_bulk_instances_from_serial(
    snowflake_uri: S3StageFileUri,
    settings: Settings,
) -> S3StageFileUri:
    """
    Internally, bulk_tagging stored procedure does not handle serial numbers.
    Instead, this function is responsible for applying serial resolution to serial numbers,
    and then staging the resolved instance_ids
    """

    logger = get_run_logger()
    cred_block = get_aws_credentials()
    s3_client = cred_block.get_s3_client()

    # Fetch the data
    logger.info(
        "Fetching data from S3: %s/%s",
        snowflake_uri.bucket,
        snowflake_uri.key,
    )
    data_compressed = s3_client.get_object(
        Bucket=snowflake_uri.bucket,
        Key=snowflake_uri.key,
    )["Body"].read()
    data = gzip.decompress(data_compressed)
    parsed_data = SerialTaggingAdapter.validate_json(data)
    serial_numbers = [row.id for row in parsed_data]

    notify_block = get_notification_block(env=settings.env)
    notify_block.notification_id = settings.notification_id

    serial_to_instance: dict[str, int] = resolve_serials_task(
        settings=settings,
        serial_numbers=serial_numbers,
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
            f"{len(unresolved)} Serial Numbers could not be resolved to Instance IDs"
        )

    # Recreate BulkTaggingInstanceRows
    # Now we use the serial_to_instance mapping to create BulkTaggingInstanceRows, which is structurally similar to BulkTaggingSerialRows
    # but has an 'id' of int vs str
    # We'll also drop any that could not resolve

    def repack_row(row: BulkTaggingSerialRow) -> BulkTaggingInstanceRow | None:
        instance_id = serial_to_instance.get(row.id)
        if instance_id is None:
            return None
        return BulkTaggingInstanceRow.model_construct(
            id=instance_id,
            tagset_id=row.tagset_id,
            tag_name=row.tag_name,
        )

    repacked_rows_with_none = (repack_row(row) for row in parsed_data)
    staged_data = [row for row in repacked_rows_with_none if row is not None]
    adapter = TypeAdapter(list[BulkTaggingInstanceRow])
    # Now we need to stage the repacked rows back to s3
    staged_data_bytes = adapter.dump_json(staged_data)
    staged_data_compressed = gzip.compress(staged_data_bytes)
    staged_uri = settings.get_s3_staged_instance_id()

    cred_block = get_aws_credentials()
    s3_client = cred_block.get_s3_client()

    logger.info(
        "Staging resolved instance IDs with bulk data to S3: %s/%s",
        staged_uri.bucket,
        staged_uri.key,
    )
    s3_client.put_object(
        Bucket=staged_uri.bucket,
        Key=staged_uri.key,
        Body=staged_data_compressed,
        ContentType="application/json",
        ContentEncoding="gzip",
    )

    logger.info("Resolved instance IDs with bulk data staged successfully")
    return staged_uri
