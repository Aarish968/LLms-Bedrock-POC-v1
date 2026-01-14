from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Optional, Union

from prefect import Flow, Parameter, task
from prefect.engine.signals import FAIL
from prefect.storage import Docker
from prefect.triggers import any_failed
from sqlalchemy import create_engine, text, bindparam, Integer, column, String

from common import sec
from common.config import FlowSettings, FlowEnv, DbEnv, RunSettings, FlowType
from common.models import RequestMetadata, LocalFileUri, TagInstanceResult, S3FileUri
from common.utils import log_queries
from tasks.export_tags import tag_export
from tasks.import_tags import tag_import

flow_settings = FlowSettings()


@task(name="Gather Parameters", log_stdout=True, tags=["snowflake_xsmall"])
def gather_parameters(
    request_id: int,
    bucket_name: Union[None, str, list[str]],
    file_input_location: Optional[str],
    file_output_location: Optional[str],
    env: DbEnv,
    flow_env: FlowEnv,
    engagement_id: Optional[int],
    flow_type: Optional[FlowType],
    requestor: Optional[str],
) -> RunSettings:
    """
    We're running this task from a parent flow that dynamically picks which flow to run based on passed parameters.

    Parameters
    ----------
    request_id : int
        The request_id to use for tracking
    bucket_name : Union[None, str, list[str]]
        The S3 Bucket Name to use for storing output
    file_input_location : Optional[str]
        The file location to use for reading input. This will be None in the case of a download
    file_output_location : Optional[str]
        The file location to use for writing output. This will be None in the case of an upload
    env : DbEnv
        The environment to use for the backend database
    flow_env : FlowEnv
        The environment to use for the flow. In Dev, we don't perform request status logging
    engagement_id : Optional[int]
        The engagement_id this request is associated with. If this is None, and it is an upload, we retrieve it from
        the upload file,
    flow_type : Optional[FlowType]
        The type of flow to run. If this is None, we will determine it based on the presence of a file_location
    requestor : Optional[str]
        The requestor of this request. If this is None, we will retrieve it from the database
    """
    if flow_type is None:
        flow_type = FlowType.upload if file_input_location else FlowType.download
    run_settings = RunSettings(db_env=env, flow_env=flow_env, flow_type=flow_type)

    # First determine if this is an upload or download - if a file_location is passed, it's an upload

    def get_request_metadata_from_db():
        engine = create_engine(
            sec.get_sf_pw(
                sec.check_env(run_settings.db_env),
                run_settings.get_warehouse(),  # Warehouse
                run_settings.schema_gu,  # Schema
            ),
            connect_args={"log_max_query_length": 10_000},
        )
        get_metadata_query = (
            text(
                """
            SELECT DC_ENGAGEMENT_ID, CREATED_BY, FILE_LOCATION
            FROM IDENTIFIER(:dc_gu_upload)
            WHERE REQUEST_ID = :request_id
            AND IS_DELETED = 'F'
            LIMIT 1
            """
            )
            .bindparams(
                bindparam("dc_gu_upload", run_settings.dc_gu_upload),
                bindparam("request_id", int(request_id)),
            )
            .columns(
                column("dc_engagement_id", Integer),
                column("created_by", String),
                column("file_location", String),
            )
        )

        with engine.connect() as conn:
            metadata = conn.execute(get_metadata_query).fetchone()

        if not metadata:
            raise FAIL(f"Request ID {request_id} not found in {run_settings.dc_gu_upload}")
        return RequestMetadata(
            request_id=request_id,
            dc_engagement_id=metadata["dc_engagement_id"],
            requestor=metadata["created_by"],
            file_input_location=metadata["file_location"],
            file_output_location=file_output_location,
        )

    def get_request_metadata_from_params():
        return RequestMetadata(
            request_id=request_id,
            dc_engagement_id=engagement_id,
            requestor=requestor,
            file_input_location=file_input_location,
            file_output_location=file_output_location,
        )

    if requestor is None or engagement_id is None:
        request_metadata = get_request_metadata_from_db()
    else:
        request_metadata = get_request_metadata_from_params()

    run_settings.request_metadata = request_metadata

    return run_settings


@task(log_stdout=True, tags=["snowflake_xsmall"])
@log_queries
def task_runner(run_settings: RunSettings) -> Union[LocalFileUri, S3FileUri, BytesIO, list[TagInstanceResult]]:
    """
    This is a task that dynamically runs a flow based on the flow_type
    Parameters
    ----------
    run_settings

    Returns
    -------
    """
    if run_settings.flow_type is None:
        raise FAIL("Flow Type is None")

    if run_settings.flow_type == FlowType.download:
        result = tag_export(
            dc_engagement_id=run_settings.request_metadata.dc_engagement_id,
            run_settings=run_settings,
            export_path=run_settings.request_metadata.file_output_location,
        )
        return result
    elif run_settings.flow_type == FlowType.upload:
        result = tag_import(
            dc_engagement_id=run_settings.request_metadata.dc_engagement_id,
            run_settings=run_settings,
        )
    else:
        raise FAIL(f"Flow Type {run_settings.flow_type} is not supported")

    return result


@task(log_stdout=True, tags=["snowflake_xsmall"])
@log_queries
def update_generic_upload_log_table(run_settings: RunSettings):
    """Update the generic upload log table with the request_id and status as success"""
    if run_settings.flow_env != FlowEnv.prod:
        print("Not in prod, skipping update_generic_upload_log_table")
        return True
    file_uri = run_settings.request_metadata.file_input_location
    if file_uri is not None and file_uri.file_type == "s3":
        file_location = file_uri.uri
    else:
        file_location = None

    stmt = text(
        """
        UPDATE identifier(:dc_gu_upload)
        set STATUS = 'Success',
        FILE_LOCATION = :file_location
        where REQUEST_ID = :request_id;
        """
    ).bindparams(
        bindparam("dc_gu_upload", run_settings.dc_gu_upload),
        bindparam("request_id", run_settings.request_metadata.request_id),
        bindparam("file_location", file_location),
    )

    engine = create_engine(
        sec.get_sf_pw(
            sec.check_env(run_settings.db_env),
            run_settings.get_warehouse(),  # Warehouse
            run_settings.schema_api,  # Schema
        ),
        connect_args={"log_max_query_length": 10_000},
    )
    with engine.connect() as conn:
        conn.execute(stmt)
    return True


@task(log_stdout=True, trigger=any_failed, tags=["snowflake_xsmall"])
@log_queries
def update_generic_upload_log_table_failed(flow_env: FlowEnv, db_env: DbEnv, request_id: Optional[int]):
    """This will parse its own run_settings in case the first task of gathering parameters fails"""
    run_settings = RunSettings(db_env=db_env, flow_env=flow_env, flow_type=FlowType.upload)
    if run_settings.flow_env != FlowEnv.prod:
        print("Not in prod, skipping update_generic_upload_log_table_failed")
        return True
    if not request_id:
        print("No request_id passed, skipping update_generic_upload_log_table_failed")
        return True
    stmt = text(
        """
        UPDATE identifier(:dc_gu_upload)
        set STATUS = 'Failed'
        where REQUEST_ID = :request_id;
        """
    ).bindparams(
        bindparam("dc_gu_upload", run_settings.dc_gu_upload),
        bindparam("request_id", request_id, type_=Integer),
    )
    engine = create_engine(
        sec.get_sf_pw(sec.check_env("prod"), run_settings.get_warehouse(), run_settings.schema_api),
        connect_args={"log_max_query_length": 10_000},
    )
    with engine.connect() as conn:
        try:
            conn.execute(stmt)
        except Exception as e:
            print(e)
            print(f"Failed to update generic request_id: {request_id} to failed")
    return True


storage_obj = Docker(
    base_image=flow_settings.base_image,
    python_dependencies=[
        "'awswrangler==2.20.1'",
        "'numpy==1.23.4'",
        "'openpyxl>=3'",
        "'pandas<2'",
        "'SQLAlchemy>=1.4,<2'",
        "'pydantic<2'",
        "'snowflake-sqlalchemy'",
        "'xlsxwriter>=3.1.0'",
    ],
    registry_url=flow_settings.registry_url,
    image_name=flow_settings.image_name,
    files={
        str(Path.cwd() / "common"): "/root/.prefect/flows/common",
        str(Path.cwd() / "tasks"): "/root/.prefect/flows/tasks",
    },
    env_vars={"PYTHONPATH": "${PYTHONPATH}:/root/.prefect/flows/"},
)

with Flow(
    flow_settings.flow_name,
    storage=storage_obj,
    run_config=flow_settings.run_config(**flow_settings.run_config_params),
    executor=flow_settings.executor(**flow_settings.executor_params),
    result=flow_settings.result(**flow_settings.result_params),
) as entry_flow:
    request_id_p = Parameter("request_id", required=False, default=None)
    bucket_name_p = Parameter("bucket_name", required=False, default=None)
    file_input_location_p = Parameter("file_location", required=False, default=None)
    env_p = Parameter("env", required=False, default=DbEnv.prod)
    flow_env_p = Parameter("flow_env", required=False, default=FlowEnv.prod)

    # Below are available for testing overrides
    flow_type_p = Parameter("flow_type", required=False, default=None)
    engagement_id_p = Parameter("engagement_id", required=False, default=None)
    requestor_p = Parameter("requestor", required=False, default=None)
    file_output_location_p = Parameter("file_output_location", required=False, default=None)

    failed_flow = update_generic_upload_log_table_failed(flow_env=flow_env_p, db_env=env_p, request_id=request_id_p)

    run_params_result = gather_parameters(
        request_id=request_id_p,
        bucket_name=bucket_name_p,
        file_input_location=file_input_location_p,
        file_output_location=file_output_location_p,
        env=env_p,
        flow_env=flow_env_p,
        engagement_id=engagement_id_p,
        flow_type=flow_type_p,
        requestor=requestor_p,
    )

    task_run_result = task_runner(run_settings=run_params_result)

    run_success = update_generic_upload_log_table(run_settings=run_params_result, upstream_tasks=[task_run_result])
    entry_flow.set_reference_tasks([run_success])

if __name__ == "__main__":
    entry_flow.run(
        parameters={
            "request_id": 123,
            "file_output_location": str(Path("~").expanduser() / "Downloads" / "test.xlsx"),
            "env": DbEnv.prod,
            "flow_env": FlowEnv.dev,
            "engagement_id": 84,
            "requestor": "testuser",
        }
    )
