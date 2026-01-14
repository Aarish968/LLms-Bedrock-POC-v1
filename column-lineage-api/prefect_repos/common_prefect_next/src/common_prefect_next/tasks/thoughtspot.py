import json
from enum import Enum
from typing import TypedDict

from prefect_aws import AwsSecret

from common_prefect_next.blocks.environment import Env, TEnv
from common_prefect_next.blocks.thoughtspot import AwsThoughtSpotBlockNames


class ThoughtspotCredentials(TypedDict):
    username: str
    password: str


class ThoughtspotServerUrl(str, Enum):
    dev = "https://cisco-dev.thoughtspot.cloud"
    prod = "https://cisco.thoughtspot.cloud"

    def __str__(self) -> str:
        return str.__str__(self)


class ThoughtspotServerCredentials(TypedDict):
    server_url: str
    credentials: ThoughtspotCredentials


def _get_thoughtspot_credentials(env: TEnv | Env) -> ThoughtspotCredentials:
    block: AwsSecret = AwsSecret.load(AwsThoughtSpotBlockNames.generic_user)
    secret = block.read_secret()
    parsed = json.loads(secret)
    return ThoughtspotCredentials(**parsed[str(env)])


def _get_thoughtspot_server(env: TEnv | Env) -> str:
    return str(ThoughtspotServerUrl[env])


def get_thoughtspot_credentials(env: TEnv | Env) -> ThoughtspotServerCredentials:
    credentials = _get_thoughtspot_credentials(env)
    server_url = _get_thoughtspot_server(env)
    return ThoughtspotServerCredentials(server_url=server_url, credentials=credentials)
