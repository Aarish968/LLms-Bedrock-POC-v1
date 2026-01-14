from __future__ import annotations

from pathlib import Path

from common_tasks.dataframes import DataFrameUploadTask
from prefect import Flow, Parameter, case
from prefect.storage import Docker
from prefect.tasks.control_flow import merge
from prefect.tasks.core.function import FunctionTask

from common.config import (
    FlowEnv,
    get_flow_settings,
    Env,
)
from common.utils import add_wheels
from tasks.db_ddl import (
    create_mce_transient_table,
    drop_mce_transient_table,
    update_generic_upload_log_table,
    update_generic_upload_log_table_as_failed,
)
from tasks.params import read_template_params, validate_flow_params
from tasks.queries import query_mce_data
from tasks.settings import get_run_settings

flow_settings = get_flow_settings()

save_result = DataFrameUploadTask()
# Needed for merging back in conditional branch
skip_save_result = FunctionTask(lambda x: x, name="skip_save_result")
# With Prefect v1 we can't access attributes of a returned object in a flow context
attr_getter = FunctionTask(lambda x, y: getattr(x, y), name="attr_getter")
storage_obj = Docker(
    base_image=flow_settings.base_image,
    registry_url=flow_settings.registry_url,
    python_dependencies=[
        "'awswrangler>=2.2,<3.0'",
        "'boto3'",
        "'numpy'",
        "'pandas>=1.4,<2.0'",
        "'pydantic>=1.10,<2.0'",
        "'snowflake-sqlalchemy'",
        "'sqlalchemy>=1.4,<2.0'",
        "'XlsxWriter>=3.1.0'",
    ],
    files={
        str(Path.cwd() / "common"): "/root/.prefect/flows/common",
        str(Path.cwd() / "tasks"): "/root/.prefect/flows/tasks",
        str(Path.cwd() / "common" / "data"): "/flow_data",
    },
    env_vars={
        "PYTHONPATH": "${PYTHONPATH}:/root/.prefect/flows/",
        "DATA_DIR": "/flow_data",
    },
)


with Flow(
    flow_settings.flow_name,
    storage=add_wheels(storage_obj),
    run_config=flow_settings.run_config(**flow_settings.run_config_params),
    executor=flow_settings.executor(**flow_settings.executor_params),
    result=flow_settings.result(**flow_settings.result_params),
) as flow:
    """
    Prefect flow to generate an Excel File mimicking the MCE Macro Excel File
    
    Parameters
    ----------
    request_id : Optional[str]
        Request ID for the flow
    bucket_name : Optional[str]
        Bucket name for the flow
    file_location : Optional[str]
        Location of the file that was uploaded, containing parameters for the flow
    user_invoked : Optional[bool]
        Whether the flow was invoked by a User via Data Canvas. If True, the flow will log success/failure to the
        generic_upload table.
    query_params : Optional[dict]
        Dictionary of parameters for the flow. See ``FlowParams`` for expected keys.
    flow_env : Optional[str]
        Environment to run the flow in. See ``FlowEnv`` for expected values.
    env : Optional[str]
        Database environment to use.


    """
    request_id_p = Parameter("request_id", required=False, default=None)
    bucket_name_p = Parameter("bucket_name", required=False, default=None)
    file_location_p = Parameter("file_location", required=False, default=None)
    user_invoked_p = Parameter("user_invoked", required=False, default=True)
    query_params_p = Parameter("query_params", required=False, default=None)
    flow_env_p = Parameter("flow_env", required=False, default=FlowEnv.PROD.value)
    env_p = Parameter("env", required=False, default=Env.PROD.value)

    settings = get_run_settings(env=env_p, flow_env=flow_env_p)
    egrid_path = attr_getter(settings, "egrid_path")

    with case(user_invoked_p, True):
        user_flow_params = read_template_params(
            file_location=file_location_p,
            bucket_name=bucket_name_p,
            request_id=request_id_p,
            settings=settings,
        )

    with case(user_invoked_p, False):
        prog_flow_params = validate_flow_params(query_params=query_params_p)

    flow_params_p = merge(user_flow_params, prog_flow_params)
    transient_table_name_p = create_mce_transient_table(
        params=flow_params_p, settings=settings
    )

    df_mce_p = query_mce_data(
        params=flow_params_p,
        settings=settings,
        e_grid_path=egrid_path,
        upstream_tasks=[transient_table_name_p],
    )
    transient_table_name_dropped = drop_mce_transient_table(
        transient_table_name=transient_table_name_p,
        settings=settings,
        upstream_tasks=[df_mce_p],
    )

    output_uri_p = attr_getter(flow_params_p, "output_uri")
    settings_flow_env = attr_getter(settings, "flow_env")

    with case(settings_flow_env, FlowEnv.PROD):
        df_macro_saved_prod = save_result(df=df_mce_p, output_uri=output_uri_p)
    with case(settings_flow_env, FlowEnv.DEV):
        df_macro_saved_dev = save_result(df=df_mce_p, file_path=output_uri_p)
    df_macro_saved = merge(df_macro_saved_prod, df_macro_saved_dev)

    log_updated = update_generic_upload_log_table(
        params=flow_params_p,
        settings=settings,
        upstream_tasks=[df_macro_saved],
    )

    log_updated_failed = update_generic_upload_log_table_as_failed(
        params=flow_params_p,
        settings=settings,
        upstream_tasks=[df_macro_saved],
    )

    flow.set_reference_tasks([log_updated])

# if __name__ == "__main__":
#     query_params = {
#         "customer_name": "TD Bank",
#         "output_uri": str(Path("~").expanduser() / "Downloads" / "mce_macro_dev.xlsx"),
#         "transient_table_name": "IB_MCE_MACRO_DEV_TMP",
#         "mce_engagement_id": 1673275963775,
#         "run_id": "555555",
#     }
#
#     params = {
#         "user_invoked": False,
#         "query_params": query_params,
#         "env": "prod",
#         "flow_env": "dev",
#     }
#     flow.run(parameters=params)
