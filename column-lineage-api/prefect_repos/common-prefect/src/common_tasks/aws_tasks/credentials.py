import json
from typing import TypedDict

from prefect.client import Secret


class PrefectAwsCredentials(TypedDict):
    ACCESS_KEY: str
    SECRET_ACCESS_KEY: str
    
def _get_boto3_session(region_name: str = "us-east-1", **kwargs):
    import boto3
    session = boto3.session.Session(region_name=region_name, **kwargs)
    return session


def _get_secret(secret_name, credentials: PrefectAwsCredentials):
    session = _get_boto3_session(
        aws_access_key_id=credentials["ACCESS_KEY"],
        aws_secret_access_key=credentials["SECRET_ACCESS_KEY"]
        )
    client = session.client("secretsmanager")
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    secret = get_secret_value_response["SecretString"]
    return json.loads(secret)

def get_boto3_session():
    """
    This pulls AWS_CREDENTIALS from the prefect.context.secrets.
    These are only available if the storage object is configured to have access to the secrets.
    """
    import prefect
    secrets = prefect.context.get("secrets", {}).get("AWS_CREDENTIALS")
    if not secrets:
        try:
            secrets = Secret("AWS_CREDENTIALS").get()
        except Exception as e:
            raise ValueError("Could not get AWS_CREDENTIALS from context or secrets") from e
    session = _get_boto3_session(
        aws_access_key_id=secrets["ACCESS_KEY"],
        aws_secret_access_key=secrets["SECRET_ACCESS_KEY"]
    )
    return session

__all__ = ["get_boto3_session"]