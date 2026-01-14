from typing import Literal
from uuid import uuid4

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

from . import BaseEnum, Model


class Environment(BaseEnum):
    dev = "dev"
    prod = "prod"


TEnvironment = Literal["dev", "prod"]


class S3StageFileUri(Model):
    bucket: str = Field(
        description="S3 bucket name", examples=["dsci.snowflake.storage"]
    )
    key: str = Field(
        description="S3 object key",
        examples=[
            "thought_spot_tag_requests/json/dev/common_serial_resolution/1234567890abcdef.json.gz"
        ],
    )
    s3_uri: str = Field(
        description="S3 URI of the file",
        examples=[
            "s3://dsci.snowflake.storage/thought_spot_tag_requests/json/dev/common_serial_resolution/1234567890abcdef.json.gz"
        ],
    )
    snowflake_uri: str = Field(
        description="Snowflake flavored URI of S3",
        examples=[
            "@CPS_DSCI_STG.MY_CSV_STAGE/dev/workflow_name/user_request_id.json.gz"
        ],
    )


class CommonResolutionSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="common_resolution_",
        case_sensitive=False,
    )
    stage_bucket: str = Field(
        default="dsci.snowflake.storage",
        description="The Name of the S3 bucket where the staged files are stored.",
    )
    key_prefix: str = Field(
        default="thought_spot_tag_requests",
        description="The Snowflake Stage is defined to have a path of s3://<bucket>/<key_prefix>",
    )
    env: Environment = Field(default=Environment.prod)
    stage_name: str = Field(
        default="CPS_DSCI_STG.MY_CSV_STAGE",
        description="The name of the Snowflake stage where the files are stored.",
    )
    workflow_name: str = Field(
        default="common_serial_resolution",
        description="The workflow name to be used in the S3 URI.",
    )

    @computed_field
    @property
    def stage_url(self) -> str:
        """
        The S3 URL of the stage where the files are stored.
        This is constructed using the stage bucket and key prefix.
        """
        return f"s3://{self.stage_bucket}/{self.key_prefix}"

    def make_staged_s3_uri(self) -> S3StageFileUri:
        """
        Create the actual S3 URI for the file where
        s3://<self.bucket>/<self.key_prefix>/json/<self.env>/<workflow>/<file_name>
        And Snowflake would reference if as.
        @<self.stage_name>/json/<self.env>/<workflow>/<file_name>
        """
        bucket = self.stage_bucket
        file_name = f"{uuid4().hex}.json.gz"

        sf_key = f"{self.env!s}/{self.workflow_name}/{file_name}"

        snowflake_uri = f"@{self.stage_name}/json/{sf_key}"
        key = f"{self.key_prefix}/json/{sf_key}"
        s3_uri = f"s3://{bucket}/{key}"

        return S3StageFileUri(
            bucket=bucket, key=key, s3_uri=s3_uri, snowflake_uri=snowflake_uri
        )
