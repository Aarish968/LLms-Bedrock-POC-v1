import json
from itertools import zip_longest
from pathlib import Path
from typing import Optional, Union

import pandas as pd
import prefect
from common_tasks.aws_tasks import get_boto3_session
from common_tasks.log_utils import log_queries
from common_tasks.notifications import (
    ApiNotificationHandler,
    inject_api_logger,
    notify_api_on_failure,
)
from common_tasks.notifications.auth import GetAccessToken
from common_tasks.settings import SettingsBase, TEnv, Warehouse
from common_tasks.utils import download_from_s3, parse_s3_uri
from prefect import Flow, Parameter, task
from prefect.engine.signals import FAIL
from prefect.executors import LocalExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import Docker
from prefect.tasks.prefect import RenameFlowRun
from sqlalchemy import (
    DateTime,
    Integer,
    String,
    column,
    create_engine,
    insert,
    table,
    text,
)

from evidence_collector.common.models import FileMetadata, RowData
from evidence_collector.common.queries import (
    StoredProcException,
    parse_stored_proc_result,
)
from evidence_collector.common.serial_prediction import (
    SerialResolutionResult,
    run_serial_resolution,
)
from evidence_collector.common.wb import package_workbook


class Settings(SettingsBase):
    def __init__(
        self,
        env: TEnv,
        warehouse: Warehouse,
        dc_engagement_id: int,
        request_id: int,
        request_json_loc: str,
        requested_by: str,
        notification_id: int,
    ):
        super().__init__(env=env, warehouse=warehouse)
        self.env = env
        self.warehouse = warehouse
        self.dc_engagement_id = dc_engagement_id
        self.request_id = request_id
        self.request_json_loc = request_json_loc
        self.requested_by = requested_by
        self.notification_id = notification_id
        self.tag_resolved_id = 1411

    def get_s3_uri(self):
        bucket_name = "dc-serial-resolution"
        return f"s3://{bucket_name}/{self.env}/{self.request_id}.xlsx"


def get_engine(
    settings: Union[Settings, SettingsBase],
    warehouse: Optional[Warehouse] = None,
    schema: Optional[str] = None,
    **session_kwargs,
):
    db_url = settings.get_db_url(warehouse, schema)
    session_parameters = {"abort_detached_query": True, **session_kwargs}
    return create_engine(
        db_url,
        connect_args={
            "log_max_query_length": 10_000,
            "session_parameters": session_parameters,
            "insecure_mode": True,
            "disable_ocsp_checks": True,
        },
    )


@task()
def get_settings(
    env: TEnv,
    dc_engagement_id: int,
    request_id: int,
    request_json_loc: str,
    requested_by: str,
    notification_id: int,
) -> Settings:
    settings = Settings(
        env=env,
        warehouse=Warehouse.small,
        dc_engagement_id=dc_engagement_id,
        request_id=request_id,
        request_json_loc=request_json_loc,
        requested_by=requested_by,
        notification_id=notification_id,
    )

    RenameFlowRun().run(
        flow_run_name=f"dc-evidence-collector-{settings.request_id}-{settings.requested_by.split('@')[0]}"
    )

    return settings


token_task = GetAccessToken()


@task(log_stdout=True)
def get_json_from_s3(url: str) -> FileMetadata:
    logger = prefect.context.get("logger")
    api_logger: "ApiNotificationHandler" = prefect.context.get("api_logger")
    bucket, key = parse_s3_uri(url)
    session = get_boto3_session()
    client = session.client("s3")
    try:
        json_data = json.loads(
            download_from_s3(bucket=bucket, key=key, client=client).decode("utf-8")
        )
    except Exception as e:
        api_logger.send_exception("Could not download JSON from S3", exception=e)
        logger.exception("Could not download JSON from S3")
        raise FAIL("Could not download JSON from S3") from e
    parsed = FileMetadata.from_payload(json_data)
    api_logger.send_text("Resolving Serial Numbers to Instance Ids")
    return parsed


@task(log_stdout=True)
@log_queries
def apply_tag_via_stored_proc(resolution: SerialResolutionResult, settings: Settings):
    """
    We're calling the stored procedure TAG_INSTANCES_11 which will handle applying this tag
    """
    logger = prefect.context.get("logger")
    api_logger = prefect.context.get("api_logger")
    api_logger.send_text("Applying 'Resolved' tag to Serial Numbers")

    def payload_generator(data: set[int]):
        # noinspection PyArgumentList
        batched_ids = [
            list(filter(None, batch)) for batch in zip_longest(*[iter(data)] * 10000)
        ]

        for batch in batched_ids:
            yield {
                "userId": settings.requested_by,
                "ddl_action": "set",
                "engagementId": settings.dc_engagement_id,
                "comment": f"Applying 'Resolved' tag to Serial Numbers from Collector Upload Request Id #{settings.request_id}",
                "tagId": settings.tag_resolved_id,
                "instance": batch,
            }

    multis = resolution.get_multis()
    multis_shared_parent = resolution.get_multis_same_parent()

    def get_instances_from_df(df: pd.DataFrame) -> set[int]:
        return set(
            pd.to_numeric(df["instance_id"], errors="coerce")
            .dropna()
            .astype("Int64")
            .dropna()
            .astype(int)
            .tolist()
        )

    instance_ids = get_instances_from_df(multis_shared_parent) | get_instances_from_df(
        multis
    )

    stmt = text("CALL TAG_INSTANCES_11(:payload)")

    engine = get_engine(settings)
    with engine.begin() as conn:
        for payload in payload_generator(instance_ids):
            sp_stmt = stmt.bindparams(
                payload=json.dumps(payload, separators=(",", ":"))
            )
            result_raw = conn.execute(sp_stmt).scalar()
            try:
                result = parse_stored_proc_result(result_raw)
                logger.info(f"Result of Stored Proc {result}")
            except StoredProcException as e:
                api_logger.send_text(
                    f"Error Applying 'Resolved' Tag {e.message}. Continuing run"
                )
            except Exception as e:
                api_logger.send_text(
                    f"Error Applying 'Resolved' Tag {e}. Continuing run"
                )

    api_logger.send_text("Tag 'Resolved' Applied to Serial Numbers")


@task(log_stdout=True)
@log_queries
def store_collector_data(
    settings: Settings,
    resolution: SerialResolutionResult,
    file_metadata: FileMetadata,
):
    engine = get_engine(settings=settings)
    api_logger: "ApiNotificationHandler" = prefect.context.get("api_logger")
    api_logger.send_text(
        "Storing your Collector Data",
    )

    hdr_stmt = text(
        """
        insert into DC_EVIDENCE_COLLECTOR_HDR(request_id,
                                              create_dtm,
                                              created_by,
                                              file_name_id,
                                              effective_date,
                                              source,
                                              note,
                                              dc_engagement_id)
                                    values (:request_id,
                                        CURRENT_TIMESTAMP,
                                        :created_by,
                                        :file_name_id,
                                        :effective_date,
                                        :source,
                                        :note,
                                        :dc_engagement_id
                                        )
        """
    ).bindparams(
        request_id=settings.request_id,
        created_by=settings.requested_by,
        file_name_id=file_metadata.file_name_id,
        effective_date=file_metadata.effective_date,
        source=file_metadata.source,
        note=file_metadata.note,
        dc_engagement_id=settings.dc_engagement_id,
    )

    detail_table = table(
        "dc_evidence_collector_details",
        column("request_id", Integer),
        column("instance_id", Integer),
        column("host_name", String(500)),
        column("ip_address", String(500)),
        column("serial_number", String(500)),
        column("product_id", String(500)),
        column("product_family", String(5000)),
        column("product_subtype", String(500)),
        column("product_type", String(500)),
        column("equipment_type", String(500)),
        column("snmp_sys_location", String(500)),
        column("sw_type", String(500)),
        column("sw_version", String(500)),
        column("inventory", String(500)),
        column("segment", String(500)),
        column("collection_date", DateTime),
        column("inventory_source", String(500)),
        column("relationship", String(500)),
        column("notes_1", String(500)),
        column("notes_2", String(500)),
    )

    def insert_generator(
        rows_inner: list[RowData], request_id: int, mapping: dict[str, int]
    ):
        batched_rows: list[list[RowData]] = [
            list(filter(None, batch))
            for batch in zip_longest(*[iter(rows_inner)] * 5000)
        ]
        for batch in batched_rows:
            row_inserts = [
                {
                    "request_id": request_id,
                    "instance_id": mapping.get(row.serial_number),
                    "host_name": row.host_name,
                    "ip_address": row.ip_address,
                    "serial_number": row.serial_number,
                    "product_id": row.product_id,
                    "product_family": row.product_family,
                    "product_subtype": row.product_subtype,
                    "product_type": row.product_type,
                    "equipment_type": row.equipment_type,
                    "snmp_sys_location": row.snmp_sys_location,
                    "sw_type": row.sw_type,
                    "sw_version": row.sw_version,
                    "inventory": row.inventory,
                    "segment": row.segment,
                    "collection_date": row.collection_date,
                    "inventory_source": row.inventory_source,
                    "relationship": row.relationship,
                    "notes_1": row.notes_1,
                    "notes_2": row.notes_2,
                }
                for row in batch
            ]
            yield row_inserts

    # Remove rows that could not match
    unmatched_rows = [
        row
        for row in file_metadata.rows
        if row.serial_number not in resolution.sn_mapping
    ]
    if unmatched_rows:
        api_logger.send_text(
            f"Could not match {len(unmatched_rows)} serial numbers. These will not be stored."
        )
    matched_rows = [
        row for row in file_metadata.rows if row.serial_number in resolution.sn_mapping
    ]

    serial2instance = resolution.sn_mapping
    with engine.begin() as conn:
        conn.execute(hdr_stmt)

        for rows in insert_generator(
            matched_rows, settings.request_id, serial2instance
        ):
            conn.execute(insert(detail_table), rows)

    api_logger.send_text("Collector Data Stored!")


@task(log_stdout=True)
@log_queries
def make_serial_resolution_objects(
    settings: Settings, resolution: SerialResolutionResult
):
    """
    We will create an Excel Workbook, Create a snippet about statistics for notificaition, upload the workbook to S3 and the notification
    """
    logger = prefect.context.get("logger")
    api_logger: "ApiNotificationHandler" = prefect.context.get("api_logger")
    api_logger.send_text("Creating Excel Workbook with Serial Resolution Results")
    # Compute the statistics
    stats = resolution.get_statistics()
    api_logger.send_table(stats)

    # Package the workbook (memory intensive)
    wb_path = package_workbook(resolution_result=resolution)
    ws_uri = settings.get_s3_uri()
    bucket, key = parse_s3_uri(ws_uri)
    session = get_boto3_session()
    client = session.client("s3")

    with open(wb_path, "rb") as fp:
        client.upload_fileobj(fp, bucket, key)

    api_logger.send_download_link(url=ws_uri, label="Serial Resolution Results")
    logger.info(f"Workbook Uploaded to {ws_uri}")


def get_src():
    return Path(__file__).parent.parent


storage_obj = Docker(
    base_image="837578041534.dkr.ecr.us-east-1.amazonaws.com/bases/common-prefect:latest",
    registry_url="837578041534.dkr.ecr.us-east-1.amazonaws.com/dc/p1",
    dockerignore=str(get_src() / ".dockerignore"),
    extra_dockerfile_commands=[
        """
        RUN python -m pip install --no-cache-dir -r /tmp/flow_requirements.txt \
        && pip install /wheels/*.whl
        """
    ],
    files={
        str(get_src() / "flow_requirements.txt"): "/tmp/flow_requirements.txt",
        str(get_src() / ".dockerignore"): "/tmp/.dockerignore",
        str(get_src() / "evidence_collector" / "."): "/opt/evidence_collector/",
    },
    path="/opt/evidence_collector/main.py",
    env_vars={
        "PYTHONPATH": "${PYTHONPATH}:/opt/",
        "AWS_DEFAULT_REGION": "us-east-1",
    },
    stored_as_script=True,
    secrets=["AWS_CREDENTIALS"],
)

with Flow(
    "dc-evidence-collector",
    state_handlers=[inject_api_logger, notify_api_on_failure],
    storage=storage_obj,
    executor=LocalExecutor(),
    run_config=KubernetesRun(
        labels=["dev"],
        memory_request="4G",
        memory_limit="8G",
        cpu_request="1000m",
        cpu_limit="2000m",
        job_template={
            "apiVersion": "batch/v1",
            "kind": "Job",
            "spec": {
                "template": {
                    "spec": {
                        "ttlSecondsAfterFinished": 300,
                        "containers": [{"name": "flow"}],
                        "nodeSelector": {
                            "cam-backend-access": "true",
                    }}
                }
            },
        },
    ),
) as flow:
    env_p = Parameter("env", required=True)
    dc_engagement_id_p = Parameter("dc_engagement_id", required=True)
    request_id_p = Parameter("request_id", required=True)
    request_json_loc_p = Parameter("request_json_loc", required=True)
    requested_by_p = Parameter("requested_by", required=True)
    notification_id_p = Parameter("notification_id", required=True)
    settings_result = get_settings(
        env=env_p,
        dc_engagement_id=dc_engagement_id_p,
        request_id=request_id_p,
        request_json_loc=request_json_loc_p,
        requested_by=requested_by_p,
        notification_id=notification_id_p,
    )
    token_result = token_task(settings=settings_result)

    file_metadata_result = token_result | get_json_from_s3(url=request_json_loc_p)

    resolution_result = run_serial_resolution(
        settings=settings_result,
        file_metadata=file_metadata_result,
        get_engine=get_engine,
    )

    # Now we use the serial2instance mapping and store collector details
    stored_collector_result = store_collector_data(
        settings=settings_result,
        resolution=resolution_result,
        file_metadata=file_metadata_result,
    )

    applied_tag_result = stored_collector_result | apply_tag_via_stored_proc(
        resolution=resolution_result, settings=settings_result
    )

    make_serial_resolution_objects(
        settings=settings_result, resolution=resolution_result
    )

    # To provide traceability for the user we provide an Excel Workbook with Info

#
# if __name__ == "__main__":
#     flow.run(
#         parameters={
#             "dc_engagement_id": 94,
#             "env": "dev",
#             "notification_id": 0,
#             "request_id": 213168,
#             "request_json_loc": "s3://dc-json-requests/dev/collector_file/275327.json",
#             "requested_by": "estasney@cisco.com",
#         },
#         context={"cleanup_tables": False},
#     )
