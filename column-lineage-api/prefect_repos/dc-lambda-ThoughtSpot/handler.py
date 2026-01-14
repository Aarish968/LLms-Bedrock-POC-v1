try:
    import unzip_requirements
except ImportError:
    pass
import json
import logging
import os


import boto3
from jsonschema import validate
import prefect
from prefect.run_configs.docker import DockerRun

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)
canvas_lock_bucket = 'canvas-lock'
s3_client = boto3.client("s3")
ssm_client = boto3.client("ssm", "us-east-1")

create_flow_run_mutation = """
mutation($input: CreateFlowRunInput!) {
    CreateFlowRun(input: $input) {
        id
    }
}
"""


def decrypt_parameter(parameter_name: str):
    parameter = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
    return parameter["Parameter"]["Value"]


PREFECT_AUTH_TOKEN = decrypt_parameter(f"/cam/{os.getenv('STAGE')}/prefect/token")

prefect_client = prefect.Client(api_key=PREFECT_AUTH_TOKEN)


def get_payload_in_file(bucket_name: str, object_key: str):
    json_object = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    payload = json_object["Body"].read()
    return json.loads(payload)


def trigger_cloud_flow_run(
        canvasId: str,
        requestId: str,
        action: str,
        requestedBy: str,
        memory_request: int,
        run_env : str,
) -> dict:
    return prefect_client.create_flow_run(
        version_group_id="39ab6eaa-46e6-43ae-bcf4-fa304105625e",
        labels=["ds-server-docker", "thought-spot"],
        parameters=dict(
            canvas_name=canvasId,
            request_id=requestId,
            requestedBy=requestedBy,
            action=action.lower(),
            cn_name="",
            db_name="CPS_DB",
            schema="CPS_DSCI_ARCHIVE",
            connection_guid="",
            ts_env=run_env,
            sf_env=run_env
        ),
        run_config=DockerRun()
    )


def trigger_cloud_flow_save_run(
        canvasId: str,
        requestId: str,
        action: str,
        requestedBy: str,
        memory_request: int,
        run_env: str,
) -> dict:
    return prefect_client.create_flow_run(
        version_group_id="ac738405-7885-48b6-aa4b-4bdbecbe43a8",
        labels=["ds-server-docker", "thought-spot"],
        parameters=dict(
            canvas_name=canvasId,
            request_id=requestId,
            requestedBy=requestedBy,
            action=action.lower(),
            cn_name="",
            db_name="CPS_DB",
            schema="CPS_DSCI_ARCHIVE",
            connection_guid="",
            ts_env=run_env,
            sf_env=run_env
        ),
        run_config=DockerRun()
    )


def verify_payload(payload: dict):
    schema = {
        "type": "object",
        "properties": {
            "canvasId": {"type": "string"},
            "requestId": {"type": "string"},
            "action": {"type": "string"},
            "requestedBy": {"type": "string"},
            "runEnv": {"type": "string"},
        },
        "required": ["canvasId", "requestId", "action", "requestedBy","runEnv"],
    }
    try:
        validate(instance=payload, schema=schema)
    except Exception as ex:
        logger.exception(ex)

        raise ex


def run(event, context):
    # canvas_is_locked = False
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    object_key = event["Records"][0]["s3"]["object"]["key"]
    logger.info(f"Received message via object {object_key} in bucket {bucket_name}")

    payload = get_payload_in_file(bucket_name, object_key)
    logger.info(f"Payload in file: {json.dumps(payload, indent=2)}")

    verify_payload(payload)
    memory_required = 60000000000

    # print('success')
    # lock_key = f'{payload["canvasId"]}.json'
    # print(f'''Checking if {lock_key} is locked''')

    # try:
    #     json_object = s3_client.get_object(Bucket=canvas_lock_bucket, Key=lock_key)
    #     canvas_is_locked = True
    #     print("canvas is locked")
    # except:
    #     print("canvas is not locked")
    #     response = s3_client.put_object(
    #                                     Body=lock_key,
    #                                     Bucket=canvas_lock_bucket,
    #                                     Key=lock_key,
    #                                 )

    if payload["action"] == "Save TML":
        prefect_response_cloud = trigger_cloud_flow_save_run(
            canvasId=payload["canvasId"],
            requestId=payload["requestId"],
            action=payload["action"],
            requestedBy=payload["requestedBy"],
            memory_request=memory_required,
            run_env=payload["runEnv"]
        )

    else:
        prefect_response_cloud = trigger_cloud_flow_run(
            canvasId=payload["canvasId"],
            requestId=payload["requestId"],
            action=payload["action"],
            requestedBy=payload["requestedBy"],
            memory_request=memory_required,
            run_env=payload["runEnv"]
        )

    logger.info(
        f"Flow Run ID: {prefect_response_cloud}"
    )
    # else:
    #     print(f'exiting flow start up without kicking off new flow for {payload["canvasId"]} due to it currently running already')

    return "Success"
