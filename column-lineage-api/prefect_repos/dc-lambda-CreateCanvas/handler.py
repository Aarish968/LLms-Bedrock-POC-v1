import json
import logging
import os
import typing
import urllib
from datetime import datetime

import boto3
from jsonschema import validate
import prefect
from prefect.run_configs.kubernetes import KubernetesRun

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


def trigger_current_flow_run(
        canvas_id: str,
        file_refs: list,
        date: str,
        engagement_id: str,
        source_data_date_filter: str,
        run_env : str,
) -> dict:
    return prefect_client.create_flow_run(
        version_group_id="7f348bf0-f9aa-4fbc-8ff0-2f2b64056eb7",
        parameters=dict(
            canvas_id=canvas_id,
            files=file_refs,
            destination_table="DATA_CANVAS_DETAILS",
            date=date,
            engagement_id=engagement_id,
            source_data_date_filter=source_data_date_filter,
            env=run_env,
            json_env = 'prod',

        ),
        run_config=KubernetesRun(memory_request=128000000000)
    )


def trigger_flow_run(
        canvas_id: str,
        file_refs: list,
        date: str,
        engagement_id: str,
        run_env : str,
) -> dict:
    return prefect_client.create_flow_run(
        version_group_id="196e53dd-07d3-4659-a3f0-dfa0c2c37dbf",
        parameters=dict(
            canvas_id=canvas_id,
            files=file_refs,
            destination_table="DATA_CANVAS_DETAILS",
            date=date,
            engagement_id=engagement_id,
            env= run_env,
            json_env='prod',
        ),
        run_config=KubernetesRun(memory_request=128000000000)
    )


def verify_payload(payload: dict):
    schema = {
        "type": "object",
        "properties": {
            "canvasId": {"type": "integer"},
            "engagementId": {"type": "integer"},
            "date": {"type": "string"},
            "runEnv": {"type": "string"},
            "files": {
                "type": "array",
                "items": {
                    "type": "object",
                    "minItems": 1,
                    "properties": {
                        "name": {"type": "string"},
                        "loc": {"type": "string"},
                        "date": {"type": "string"},
                    },
                },
            },
        },
        "required": ["canvasId", "date", "files", "engagementId", "runEnv"],
    }
    try:
        validate(instance=payload, schema=schema)
    except Exception as ex:
        logger.exception(ex)
        post_message_to_s3(
            canvas_id=payload["canvasId"],
            status="ERROR",
            message="Message posted in S3 did not match expected format",
            details=repr(ex),
        )
        raise ex


def post_message_to_s3(canvas_id: str, status: str, message: str, details: str):
    message_object = s3_client.Object(
        f"messaging.{os.getenv('STAGE')}.cisco.com",
        f"canvas-processing-status/{canvas_id}-{datetime.now().isoformat()}.json",
    )
    message_object.put(
        Body=(
            bytes(
                json.dumps(
                    {
                        canvas_id: canvas_id,
                        status: status,
                        message: message,
                        details: details,
                    }
                ).encode("UTF-8")
            )
        )
    )


def get_memory_required(file_refs: typing.List[typing.Dict]):
    default_memory_required = 60000000000
    one_mill = 2040109465
    three_mill = 6120328396
    six_mill = 12025908428
    try:
        memory_required = 0
        for file in file_refs:
            bucket = file["loc"].replace("s3://", "").split("/")[0]
            key = file["loc"].replace(f"s3://{bucket}/", "")
            print(bucket, key)
            for key in s3_client.list_objects(Bucket=bucket, Prefix=key)["Contents"]:
                memory_required = memory_required + key["Size"]
        print(f"""Size of file(s) to be ran : {memory_required}, estimated RAM needed : {memory_required * 500}""")
        ram_size = memory_required * 500
        if ram_size < 30000000000:
            ram_size = 30000000000
        else:
            ram_size = 60000000000
        return ram_size
    except Exception as ex:
        print(f"""Could not infer size of files, reverting to default memory required of : {default_memory_required}""")
        print(ex)
        return default_memory_required


def run(event, context):
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    object_key = event["Records"][0]["s3"]["object"]["key"]
    logger.info(f"Received message via object {object_key} in bucket {bucket_name}")

    payload = get_payload_in_file(bucket_name, object_key)
    logger.info(f"Payload in file: {json.dumps(payload, indent=2)}")

    verify_payload(payload)

    memory_required = get_memory_required(file_refs=payload["files"])
    print(memory_required)

    current_view = False
    if payload['canvasType'] == 'current view canvas':
        current_view = True

    if current_view:
        prefect_response = trigger_current_flow_run(
            canvas_id=payload["canvasId"],
            file_refs=payload["files"],
            date=payload["date"],
            engagement_id=payload["engagementId"],
            source_data_date_filter=payload["source_data_date_filter"],
            run_env = payload["runEnv"]
        )
    else:
        prefect_response = trigger_flow_run(
            canvas_id=payload["canvasId"],
            file_refs=payload["files"],
            date=payload["date"],
            engagement_id=payload["engagementId"],
            run_env=payload["runEnv"]
        )

    logger.info(
        f"Flow Run ID: {prefect_response}"
    )

    return "Success"
