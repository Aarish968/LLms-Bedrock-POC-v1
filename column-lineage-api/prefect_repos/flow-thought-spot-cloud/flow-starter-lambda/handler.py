import json
import logging
import os
import typing
import urllib
from datetime import datetime

import boto3
from jsonschema import validate
import prefect
from prefect.run_configs.docker import DockerRun

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

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


def trigger_flow_run(
        canvasId: str,
        requestId: str,
        action: str,
        requestedBy: str,
        memory_request: int
) -> dict:
    return prefect_client.create_flow_run(
        version_group_id="835d7e04-253b-4c25-b2cf-76286a08f855",
        labels=["ds-server-docker"],
        parameters=dict(
            canvas_name=canvasId,
            request_id=requestId,
            requestedBy=requestedBy,
            action=action.lower(),
            cn_name="gen_dev_con",
            db_name="CPS_DB",
            schema="CPS_DSCI_ARCHIVE",
            connection_guid="2c88ab1f-68ca-47ae-9850-ad445ac88b8e",
            ts_env="dev",
            sf_env="prod"
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
        },
        "required": ["canvasId", "requestId", "action", "requestedBy"],
    }
    try:
        validate(instance=payload, schema=schema)
    except Exception as ex:
        logger.exception(ex)
        # post_message_to_s3(
        #     canvas_id=payload["canvas_name"],
        #     status="ERROR",
        #     message="Message posted in S3 did not match expected format",
        #     details=repr(ex),
        # )
        raise ex


# def post_message_to_s3(canvas_id: str, status: str, message: str, details: str):
#     message_object = s3_client.Object(
#         f"messaging.{os.getenv('STAGE')}.cisco.com",
#         f"canvas-processing-status/{canvas_id}-{datetime.now().isoformat()}.json",
#     )
#     message_object.put(
#         Body=(
#             bytes(
#                 json.dumps(
#                     {
#                         canvas_id: canvas_id,
#                         status: status,
#                         message: message,
#                         details: details,
#                     }
#                 ).encode("UTF-8")
#             )
#         )
#     )


def run(event, context):
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    object_key = event["Records"][0]["s3"]["object"]["key"]
    logger.info(f"Received message via object {object_key} in bucket {bucket_name}")

    payload = get_payload_in_file(bucket_name, object_key)
    logger.info(f"Payload in file: {json.dumps(payload, indent=2)}")

    verify_payload(payload)
    memory_required = 60000000000
    # memory_required = get_memory_required(file_refs=payload["files"])
    # print(memory_required)

    print('success')

    prefect_response = trigger_flow_run(
        canvasId=payload["canvasId"],
        requestId=payload["requestId"],
        action=payload["action"],
        requestedBy=payload["requestedBy"],
        memory_request=memory_required,
    )

    logger.info(
        f"Flow Run ID: {prefect_response}"
    )

    return "Success"
