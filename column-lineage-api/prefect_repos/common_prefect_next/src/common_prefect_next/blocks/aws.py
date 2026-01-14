import json
from enum import Enum
from typing import Any, Optional, TypedDict

from prefect._internal.compatibility.async_dispatch import async_dispatch
from prefect.utilities.asyncutils import run_sync_in_worker_thread
from prefect_aws import AwsCredentials, AwsSecret, S3Bucket
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_serializer

from common_prefect_next.blocks.environment import Env


class AwsSecretNames(str, Enum):
    local = "prd_cps_dsci_etl_svc_local_conn_str"
    cloud = "prd_cps_dsci_etl_svc_cloud_conn_str"
    dev_api = "dev/Cognito"
    prod_api = "prod/Cognito"
    prod_cx_oracle = "prod/CxOracle"

    def __str__(self) -> str:
        return str.__str__(self)


class AwsBlockNames(str, Enum):
    aws_credentials = "aws-credentials"
    local = "sf-local-connection"
    cloud = "sf-cloud-connection"
    flow_storage = "flow-storage"
    dev_api = "cognito-dev"
    prod_api = "cognito-prod"
    dev_sf_file_stage = "dev-sf-file-stage"
    prod_sf_file_stage = "prod-sf-file-stage"
    dc_data_store = "dc-data-store"
    prod_cx_oracle = "prod-cx-oracle"

    def __str__(self) -> str:
        return str.__str__(self)


class CognitoSecretValue(TypedDict):
    USERNAME: str
    PASSWORD: str
    UserPoolId: str
    ClientId: str
    AuthFlow: str


class AwsCognitoCredentials(AwsSecret):
    """
    Block for performing AWS Cognito AdminInitiateAuth using
    AWS Credentials to retrieve An AWS Secret for the Cognito process
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)
    _block_type_name = "AWS Cognito Credentials"

    aws_credentials: AwsCredentials
    secret_name: str = Field(default=..., description="The name of the secret.")

    async def aread_secret(
        self,
        version_id: Optional[str] = None,
        version_stage: Optional[str] = None,
        **read_kwargs: Any,
    ) -> CognitoSecretValue:
        result = await super().aread_secret(version_id, version_stage, **read_kwargs)
        return CognitoSecretValue(**json.loads(result))

    @async_dispatch(aread_secret)
    def read_secret(
        self,
        version_id: Optional[str] = None,
        version_stage: Optional[str] = None,
        **read_kwargs: Any,
    ) -> CognitoSecretValue:
        result = super().read_secret(version_id, version_stage, **read_kwargs)
        return CognitoSecretValue(**json.loads(result))

    async def aget_access_token(self) -> str:
        client = self.aws_credentials.get_client("cognito-idp")
        credentials = await self.aread_secret()
        response = await run_sync_in_worker_thread(
            client.admin_initiate_auth,
            UserPoolId=credentials["UserPoolId"],
            ClientId=credentials["ClientId"],
            AuthFlow=credentials["AuthFlow"],
            AuthParameters={
                "USERNAME": credentials["USERNAME"],
                "PASSWORD": credentials["PASSWORD"],
            },
        )
        self.logger.info("Cognito AdminInitiateAuth response completed")
        return response["AuthenticationResult"]["AccessToken"]

    @async_dispatch(aget_access_token)
    def get_access_token(self) -> str:
        client = self.aws_credentials.get_client("cognito-idp")
        credentials = self.read_secret()
        response = client.admin_initiate_auth(
            UserPoolId=credentials["UserPoolId"],
            ClientId=credentials["ClientId"],
            AuthFlow=credentials["AuthFlow"],
            AuthParameters={
                "USERNAME": credentials["USERNAME"],
                "PASSWORD": credentials["PASSWORD"],
            },
        )
        self.logger.info("Cognito AdminInitiateAuth response completed")
        return response["AuthenticationResult"]["AccessToken"]


class S3StageFileUri(BaseModel):
    bucket: str
    key: str
    s3_uri: str
    snowflake_uri: str


class AwsS3SnowflakeStage(S3Bucket):
    """
    Block for coordinating AWS S3 Buckets with Snowflake Stages
    """

    _block_type_name = "AWS S3 Snowflake Stage"
    env: Env = Field(description="The environment to use for the stage.")
    stage_name: str = Field(description="The name of the Snowflake stage.")

    def make_staged_s3_uri(self, workflow: str, file_name: str) -> S3StageFileUri:
        """
        Create a staged S3 URI for a given workflow and file name.

        Note: Files, must be in json format and compressed with gzip.
        """
        sf_key = f"json/{self.env!s}/{workflow}/{file_name}"
        snowflake_uri = f"@{self.stage_name}/{sf_key}"
        s3_key = f"{self.bucket_folder}/{sf_key}"
        s3_uri = f"s3://{self.bucket_name}/{s3_key}"

        return S3StageFileUri(
            bucket=self.bucket_name,
            key=s3_key,
            s3_uri=s3_uri,
            snowflake_uri=snowflake_uri,
        )

    def make_staged_thoughtspot_s3_uri(self, thoughtspot_id: int) -> S3StageFileUri:
        """
        Create a staged S3 URI using the `thoughtspot_id` from `dc_thoughtspot_instance_requests`.

        Note: These files, are uploaded in CSV Format (uncompressed)
        """
        sf_key = f"{self.env!s}/{thoughtspot_id}.csv"
        snowflake_uri = f"@{self.stage_name}/{sf_key}"
        s3_key = f"{self.bucket_folder}/{sf_key}"
        s3_uri = f"s3://{self.bucket_name}/{s3_key}"

        return S3StageFileUri(
            bucket=self.bucket_name,
            key=s3_key,
            s3_uri=s3_uri,
            snowflake_uri=snowflake_uri,
        )


def get_aws_credentials() -> AwsCredentials:
    return AwsCredentials.load(AwsBlockNames.aws_credentials)


def get_local_connection() -> str:
    block: AwsSecret = AwsSecret.load(AwsBlockNames.local)
    parsed = json.loads(block.read_secret())
    return parsed[str(AwsSecretNames.local)]


def get_cloud_connection() -> str:
    block: AwsSecret = AwsSecret.load(AwsBlockNames.cloud)
    parsed = json.loads(block.read_secret())
    return parsed[str(AwsSecretNames.cloud)]


def get_dev_api_access_token() -> str:
    block: AwsCognitoCredentials = AwsCognitoCredentials.load(AwsBlockNames.dev_api)
    token = block.get_access_token()
    return token


def get_prod_api_access_token() -> str:
    block: AwsCognitoCredentials = AwsCognitoCredentials.load(AwsBlockNames.prod_api)
    token = block.get_access_token()
    return token


def get_s3_result_bucket() -> S3Bucket:
    """
    Returns the S3 bucket used for storing results.
    """
    block = S3Bucket.load(AwsBlockNames.dc_data_store)
    return block


def get_s3_staged_file_block(env: Env) -> AwsS3SnowflakeStage:
    """
    Returns the S3 Snowflake stage block for the given environment.
    """
    if env == Env.prod:
        return AwsS3SnowflakeStage.load(AwsBlockNames.prod_sf_file_stage)
    elif env == Env.dev:
        return AwsS3SnowflakeStage.load(AwsBlockNames.dev_sf_file_stage)
    else:
        msg = f"Unsupported environment: {env}. Supported environments are: {Env.prod}, {Env.dev}."
        raise ValueError(msg)


class OracleDbParams(BaseModel):
    user: str
    password: SecretStr
    dsn: str

    @field_serializer("password")
    def dump_secret(self, v: SecretStr) -> str:
        return v.get_secret_value()


def get_cx_oracle_db_params() -> OracleDbParams:
    """
    Returns the Oracle DB connection parameters.

    If using as a mapping like conn = oracledb.conn(**params) - use model_dump()
    """
    block = AwsSecret.load(AwsBlockNames.prod_cx_oracle)
    raw = block.read_secret()
    return OracleDbParams.model_validate_json(raw)
