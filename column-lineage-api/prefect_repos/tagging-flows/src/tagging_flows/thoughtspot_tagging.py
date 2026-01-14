import csv
import io
import json
import logging
from typing import TYPE_CHECKING, Iterable

from common_prefect_next.utils import parse_s3_uri

from tagging_flows.common import get_from_s3_uri, upload_to_s3_uri
from tagging_flows.common.models import (
    ConfigStrategy,
    StoredProcedureException,
    TThoughtSpotRequestRow,
)
from tagging_flows.common.queries.stored_procedure import rebuild_static_tags_table
from tagging_flows.common.queries.thoughtspot_tagging import (
    mark_thoughtspot_task_complete,
)
from tagging_flows.instance_tagging import run_instance_tagging

if TYPE_CHECKING:
    from common_prefect_next.blocks.aws import S3StageFileUri
    from common_prefect_next.blocks.data_canvas import DataCanvasNotificationBlock
    from mypy_boto3_s3 import S3Client

    from tagging_flows.common.models.settings import Settings

logger = logging.getLogger("tagging_flows")


def parse_thoughtspot_csv_file_contents(data: bytes) -> Iterable[int]:
    fp = io.StringIO(data.decode("utf-8"))
    reader = csv.reader(fp)
    header = next(reader, None)
    if not header:
        msg = "CSV file is empty or has no header"
        raise ValueError(msg)
    try:
        instance_id_idx = header.index("INSTANCE_ID")
    except ValueError as e:
        msg = "CSV header does not contain 'INSTANCE_ID' column"
        raise ValueError(msg) from e

    for row in reader:
        try:
            instance_cell = row[instance_id_idx]
            yield int(instance_cell)
        except (ValueError, IndexError):
            logger.info(
                "Skipping row '%s' due to invalid or missing INSTANCE_ID value", row
            )


def restage_instance_id_file(
    s3_client: "S3Client",
    csv_uri: str,
    thoughtspot_id: int,
    settings: "Settings",
) -> "S3StageFileUri":
    """
    Fetch the original CSV file contents from S3, parse it to extract instance IDs, dump to JSON in format [{"instance_id: int}], compress
    and re-stage it to S3.
    """

    def package_instance_id(iid: int) -> dict[str, int]:
        return {"instance_id": iid}

    restaged_uri = settings.get_s3_staged_thoughtspot_uri(thoughtspot_id)
    parsed_csv_uri = parse_s3_uri(csv_uri)

    csv_raw = get_from_s3_uri(s3_client=s3_client, uri=parsed_csv_uri)
    instance_ids_json_bytes = json.dumps(
        [
            package_instance_id(row)
            for row in parse_thoughtspot_csv_file_contents(data=csv_raw)
        ],
        separators=(",", ":"),
    ).encode("utf-8")

    upload_to_s3_uri(
        s3_client=s3_client,
        uri=restaged_uri,
        uncompressed_data=instance_ids_json_bytes,
    )

    return restaged_uri


def restage_instance_id_files(
    s3_client: "S3Client",
    tasks: list["TThoughtSpotRequestRow"],
    settings: "Settings",
) -> dict[int, "S3StageFileUri"]:
    # Now that we have a file_location for each thoughtspot_id, we need to restage as JSON for the stored procedure

    restaged_uris: dict[int, "S3StageFileUri"] = {}
    for task in tasks:
        try:
            restaged_uri = restage_instance_id_file(
                s3_client=s3_client,
                csv_uri=task.file_location,
                thoughtspot_id=task.thoughtspot_id,
                settings=settings,
            )
            restaged_uris[task.thoughtspot_id] = restaged_uri
        except Exception:
            logger.exception(
                "Failed to restage file for ThoughtSpot task %d",
                task.thoughtspot_id,
            )
            continue

    return restaged_uris


def run_thoughtspot_requests_for_engagement(
    tasks: list[TThoughtSpotRequestRow],
    config_strategy: ConfigStrategy | None,
    settings: "Settings",
    notify_block: "DataCanvasNotificationBlock",
    s3_client: "S3Client",
) -> None:
    """
    Handle all tasks related to a single dc_engagement_id.

    This ensures that we can take the fast-path of NOT re-creating the static tags table
    until we've finished the last request.

    This call to recreate the tags table is handled in the finally block
    """

    engine = settings.get_engine()
    dc_engagement_id = settings.dc_engagement_id

    # Ensure we have restaged S3 Files
    restaged_uris = restage_instance_id_files(
        s3_client=s3_client,
        tasks=tasks,
        settings=settings,
    )

    # Remove from our queue any that we could not restage
    if missing := set(restaged_uris) - {task.thoughtspot_id for task in tasks}:
        logger.warning(
            "The following ThoughtSpot tasks could not be restaged: %s",
            ", ".join(map(str, missing)),
        )
        notify_block.send_text(
            f"Failed to restage {len(missing)} ThoughtSpot tasks. These will be skipped.",
        )

    tasks: list[TThoughtSpotRequestRow] = [
        task for task in tasks if task.thoughtspot_id in restaged_uris
    ]

    if not tasks:
        logger.info(
            "No valid ThoughtSpot tasks to process for engagement %d", dc_engagement_id
        )
        notify_block.send_text(
            f"No valid ThoughtSpot tasks to process for engagement {dc_engagement_id}.",
        )
        return None

    thoughtspot_tasks: list[TThoughtSpotRequestRow] = sorted(
        tasks, key=lambda t: t.sort_key()
    )
    try:
        for thoughtspot_task in thoughtspot_tasks:
            logger.info(
                "Processing ThoughtSpot Task %d", thoughtspot_task.thoughtspot_id
            )
            snowflake_uri = restaged_uris[thoughtspot_task.thoughtspot_id]
            for sp_task in thoughtspot_task.to_sp_params(
                cisco_cco_id=settings.cisco_cco_id,
                snowflake_uri=snowflake_uri,
                config_strategy=config_strategy,
            ):
                with engine.begin() as conn:
                    sp_result = run_instance_tagging(
                        conn=conn, sp_params=sp_task, warehouse=settings.warehouse
                    )
                if sp_result.success:
                    logger.info(
                        "Success: Action: '%s', TagId: '%s', 'TagsetId: '%s' for ThoughtSpot Task %d",
                        thoughtspot_task.action,
                        sp_task.tag_id,
                        sp_task.tagset_id,
                        thoughtspot_task.thoughtspot_id,
                    )
                    if sp_task.tag_id is not None:
                        notify_block.send_text(
                            f"Success: Action: '{thoughtspot_task.action}', TagId: '{sp_task.tag_id}', TagsetId: '{sp_task.tagset_id}' for ThoughtSpot Task {thoughtspot_task.thoughtspot_id}"
                        )
                    else:
                        notify_block.send_text(
                            f"Success: Action: '{thoughtspot_task.action}', TagsetId: '{sp_task.tagset_id}' for ThoughtSpot Task {thoughtspot_task.thoughtspot_id}"
                        )
                else:
                    logger.error(
                        "Stored procedure failed for task %d: %s",
                        thoughtspot_task.thoughtspot_id,
                        sp_result.message,
                    )
                    if sp_task.tag_id is not None:
                        notify_block.send_text(
                            f"Error: Action: '{thoughtspot_task.action}', TagId: '{sp_task.tag_id}', TagsetId: '{sp_task.tagset_id}' for ThoughtSpot Task {thoughtspot_task.thoughtspot_id} - {sp_result.message}"
                        )
                    else:
                        notify_block.send_text(
                            f"Error: Action: '{thoughtspot_task.action}', TagsetId: '{sp_task.tagset_id}' for ThoughtSpot Task {thoughtspot_task.thoughtspot_id} - {sp_result.message}"
                        )
                    msg = "Stored procedure failed for ThoughtSpot task"
                    raise StoredProcedureException(
                        message=msg,
                        code=sp_result.code,
                    )

            # Mark the ThoughtSpot task as complete after processing
            with engine.begin() as conn:
                logger.info(
                    "Marking ThoughtSpot Task %d as complete",
                    thoughtspot_task.thoughtspot_id,
                )
                mark_thoughtspot_task_complete(
                    thoughtspot_id=thoughtspot_task.thoughtspot_id,
                    cisco_cco_id=settings.cisco_cco_id,
                    conn=conn,
                )

            logger.info(
                "Marked ThoughtSpot Task %d as complete",
                thoughtspot_task.thoughtspot_id,
            )
            notify_block.send_text(
                f"Completed ThoughtSpot Task {thoughtspot_task.thoughtspot_id}"
            )

    finally:
        with engine.begin() as conn:
            rebuild_static_tags_table(dc_engagement_id=dc_engagement_id, conn=conn)
