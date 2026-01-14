from log_to_dc_job_messages import log_to_dc_job_messages
from enum import Enum
from prefect.client import Client
from prefect.engine.results import S3Result
from prefect import Flow, Parameter, task, case
from prefect.run_configs import KubernetesRun
from prefect.storage import Docker
from common import aws_sec, sec
import os
from datetime import datetime, timedelta, timezone
import prefect
import dateutil.parser as parser
from prefect.tasks.prefect import RenameFlowRun
import string
import random

class Environment(str, Enum):
    DEV = "development"
    STG = "stage"
    PROD = "production"


def get_correct_schema(env):
    if env == "prod":
        return "CPS_DSCI_API"
    else:
        return "CPS_DSCI_BR"


def check_env(env):
    if env == "dev":
        cn = "dev_cps_dsci_etl_svc"
    elif env == "stage":
        cn = "stg_cps_dsci_etl_svc"
    elif env == "prod":
        cn = "prd_cps_dsci_etl_svc"
    else:
        cn = env

    return cn


def get_sec_dir(pth):
    return os.path.join(os.getcwd(), pth)


@task(log_stdout=True)
def get_all_running_flows(flow_group_id):
    client = Client()

    # client = prefect.Client()
    query = """
            query TableFlowRuns($name: String, $limit: Int, $offset: Int, $orderBy: [flow_run_order_by!], $flow_group_id: uuid, $flow_id: uuid, $state: [String!]) {
      flow_run(
        where: {flow: {flow_group_id: {_eq: $flow_group_id}, id: {_eq: $flow_id}}, name: {_ilike: $name}, state: {_in: $state}, }
        order_by: $orderBy
        limit: $limit
        offset: $offset
      ) {
        name
        start_time
        state
        parameters
        id
        __typename
      }
    }

    """
    variables = {
        "limit": 40,
        "name": None,
        "offset": 0,
        "state": ["Running"],
        "orderBy": {"scheduled_start_time": "desc"},
        "flow_group_id": flow_group_id,
    }
    flow_run_query = client.graphql(query=query, variables=variables)

    return flow_run_query


def cancel_flow(flowRunId):
    client = Client()
    query = """
    mutation SetFlowRunStates($flowRunId: UUID!, $version: Int!, $state: JSON!) {
      set_flow_run_states(
        input: {
          states: [{ flow_run_id: $flowRunId, state: $state, version: $version }]
        }
      ) {
        states {
          id
          status
          message
        }
      }
    }
    """
    res = client.graphql(
        query,
        variables={
            "flowRunId": flowRunId,
            "version": 4,
            "state": {"type": "Cancelled", "message": "Erics API test"},
        },
    )
    print(res)
    return True


def trigger_canvas_flow_run(params: dict, version_group_id) -> dict:
    create_flow_run_mutation = """
    mutation($input: CreateFlowRunInput!) {
        CreateFlowRun(input: $input) {
            id
        }
    }
    """

    PREFECT_AUTH_TOKEN = "TKj2Eq9X0FJmV8LN2k3hPA"

    prefect_client = prefect.Client(api_key=PREFECT_AUTH_TOKEN)

    reponse = prefect_client.create_flow_run(
        version_group_id=version_group_id, parameters=params
    )

    return reponse


@task(log_stdout=True, tags=['snowflake_xsmall'])
def main(flow_run_query, version_group_id):
    two_hour_duration = timedelta(minutes=120)
    for i in flow_run_query["data"]["flow_run"]:
        if i["start_time"]:
            diff = datetime.now(timezone.utc) - datetime.fromisoformat(
                parser.parse(i["start_time"]).isoformat()
            )
            print(diff)
            payload = i["parameters"]
            print("PAYLOAD", payload)
            if diff > two_hour_duration:
                canceled = cancel_flow(i["id"])
                payload = i["parameters"]
                print("PAYLOAD", payload)
                prefect_response_cloud = trigger_canvas_flow_run(
                    payload, version_group_id
                )
                logged = log_to_dc_job_messages(
                    payload["env"],
                    payload["canvas_id"],
                    f"INFO: Flow run for CANVAS-{payload['canvas_id']} ran for longer than 90 min, cancelling run and restarting.",
                )
                letters = string.ascii_letters
                random_string = '{}'.format(''.join(random.choice(letters) for i in range(10)))
                RenameFlowRun().run(flow_run_name=f"""CANVAS-{payload['canvas_id']}-Restarted-{random_string}""")
            else:
                print(
                    f"SUCCESS: CANVAS-{payload['canvas_id']} has not been running for longer than 90 min."
                )
        else:
            print("INFO: This flow has no start time.")

    return True


storage_obj = Docker(
    base_image="837578041534.dkr.ecr.us-east-1.amazonaws.com/bases/prefect:0.15.13-python3.9",
    python_dependencies=[
        "pandas==1.3.3",
        "awswrangler==2.12.1",
        "numpy==1.25.1",
        "boto3==1.18.16",
        "aiohttp==3.8.4",
        "hvac==0.11.2",
        "snowflake-sqlalchemy==1.2.4",
        "s3fs==0.4",
        "SQLAlchemy==1.4.41",
        "fastparquet==0.7.2",
        "XlsxWriter==3.1.2",
        "oyaml==1.0",
        "cloudpickle==2.0.0",
    ],
    files={
        get_sec_dir(
            "common/new_bulkload.py"
        ): "/root/.prefect/flows/common/new_bulkload.py",
        get_sec_dir("common/sec.py"): "/root/.prefect/flows/common/sec.py",
        get_sec_dir("common/aws_sec.py"): "/root/.prefect/flows/common/aws_sec.py",
        get_sec_dir("flow_variables.py"): "/root/.prefect/flows/flow_variables.py",
        get_sec_dir("log_to_dc_job_messages.py"): "/root/.prefect/flows/log_to_dc_job_messages.py",
        get_sec_dir("common/sql_pool.py"): "/root/.prefect/flows/common/sql_pool.py",
    },
    registry_url="837578041534.dkr.ecr.us-east-1.amazonaws.com/dc/p1",
    env_vars={
        "PYTHONPATH": "${PYTHONPATH}:/root/.prefect/flows/",
    },
)

with Flow(
    "dc-long-running-flow-check",
    storage=storage_obj,
    # run_config=DockerRun(labels=["thought-spot", "ds-server-docker"]),
    run_config=KubernetesRun(),
    # executor=LocalExecutor(),
    result=S3Result(
        bucket="cam-prefect-results",
        boto3_kwargs={
            "credentials": {
                "ACCESS_KEY": aws_sec.ACCESS_KEY,
                "SECRET_ACCESS_KEY": aws_sec.SECRET_KEY,
            }
        },
    ),
) as flow:

    current_flow_run_query = get_all_running_flows(
        "c03585fd-e0b0-4442-8cc6-d9ff457d4a73"
    )
    complete_current = main(
        current_flow_run_query,
        "7f348bf0-f9aa-4fbc-8ff0-2f2b64056eb7",
        upstream_tasks=[current_flow_run_query],
    )

    file_based_flow_run_query = get_all_running_flows(
        "429a29a6-f749-46ea-ae95-fdce224cc7be"
    )
    complete_file_based = main(
        file_based_flow_run_query,
        "196e53dd-07d3-4659-a3f0-dfa0c2c37dbf",
        upstream_tasks=[file_based_flow_run_query],
    )


if __name__ == "__main__":
    flow.run()


