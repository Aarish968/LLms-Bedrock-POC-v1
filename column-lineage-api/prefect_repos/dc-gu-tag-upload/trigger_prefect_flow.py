
from datetime import datetime

from prefect import task
import prefect
from prefect.run_configs.docker import DockerRun
from prefect.triggers import all_successful
from sqlalchemy import create_engine
import pandas as pd
from common import sec

from log_to_dc_job_messages import log_to_dc_job_messages


create_flow_run_mutation = """
mutation($input: CreateFlowRunInput!) {
    CreateFlowRun(input: $input) {
        id
    }
}
"""





PREFECT_AUTH_TOKEN = 'TKj2Eq9X0FJmV8LN2k3hPA'

prefect_client = prefect.Client(api_key=PREFECT_AUTH_TOKEN)

@task(log_stdout=True, skip_on_upstream_skip=True, trigger=all_successful)
def trigger_cloud_flow_run(
        canvasId: str,
        requestedBy: str,
        env,
        schema,
        dc_engagement_id
) -> dict:
    run_date = datetime.now()

    engine = create_engine(
        sec.get_sf_pw('prd_cps_dsci_etl_svc', 'CPS_DSCI_ETL_EXT1_WH', schema))
    request_id_qry = "select  CPS_DSCI_API.SEQ_DC_REQUEST.nextval"
    request_id = pd.read_sql(request_id_qry, engine)


    log_df = pd.DataFrame(index=[0], columns=['DC_ENGAGEMENT_ID',
                                              'REQUESTED_BY',
                                              'REQUEST_ID',
                                              'ACTION',
                                              'CREATED_BY',
                                              'CREATE_DTM',
                                              'CANVAS_ID'
                                              ])

    log_df['DC_ENGAGEMENT_ID'] = dc_engagement_id
    log_df['REQUESTED_BY'] = requestedBy
    log_df['REQUEST_ID'] = int(request_id['nextval'][0])
    log_df['ACTION']  = 'Create'
    log_df['CREATED_BY'] = requestedBy
    log_df['CREATE_DTM'] = run_date
    log_df['CANVAS_ID'] = canvasId

    print("log_df['REQUEST_ID']",log_df['REQUEST_ID'])

    log_df.to_sql('DC_TS_REPORTING'.lower(), engine, index=False, if_exists='append', chunksize=10)

    ts_request_id = int(request_id['nextval'][0])
    triggered = prefect_client.create_flow_run(
        version_group_id="39ab6eaa-46e6-43ae-bcf4-fa304105625e",
        labels=["ds-server-docker", "thought-spot"],
        parameters=dict(
            canvas_name=f"CANVAS-{canvasId}",
            request_id=ts_request_id,
            requestedBy=f"{requestedBy.split('@')[0]}",
            action='create',
            cn_name="",
            db_name="CPS_DB",
            schema="CPS_DSCI_ARCHIVE",
            connection_guid="",
            ts_env="prod",
            sf_env=env
        ),
        run_config=DockerRun()
    )
    log_to_dc_job_messages(env, canvasId,
                           f"SUCCESS: Step 11/12  triggered ThoughtSpot run for CANVAS-{canvasId} with request_id of {ts_request_id}.")
    return triggered




