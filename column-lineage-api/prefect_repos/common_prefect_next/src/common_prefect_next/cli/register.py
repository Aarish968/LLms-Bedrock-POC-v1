import json
from urllib.parse import urlunsplit

import click
from prefect.variables import Variable
from prefect_aws import AwsCredentials, AwsSecret, S3Bucket

from common_prefect_next.blocks.aws import (
    AwsBlockNames,
    AwsCognitoCredentials,
    AwsSecretNames,
)
from common_prefect_next.blocks.environment import Env
from common_prefect_next.tasks.thoughtspot import ThoughtspotServerUrl


def register_aws() -> None:
    """
    Register database secret urls and credentials as defined in blocks.aws
    These do not store secret values, rather they make prefect aware of the secret names
    """
    from common_prefect_next.blocks.aws import (
        AwsBlockNames,
        AwsCognitoCredentials,
        AwsS3SnowflakeStage,
    )

    try:
        credential = AwsCredentials.load(AwsBlockNames.aws_credentials)
    except Exception as e:
        click.echo(
            f"Failed to load block {AwsBlockNames.aws_credentials!s}. These should be created manually in the UI",
            err=True,
        )
        raise click.Abort() from e

    db_local_conn = AwsSecret(
        aws_credentials=credential, secret_name=AwsSecretNames.local
    )
    db_local_conn.save(AwsBlockNames.local, overwrite=True)
    click.secho(
        f"Saved {AwsSecretNames.local!s} as '{AwsBlockNames.local!s}'", fg="green"
    )

    db_cloud_conn = AwsSecret(
        aws_credentials=credential, secret_name=AwsSecretNames.cloud
    )
    db_cloud_conn.save(AwsBlockNames.cloud, overwrite=True)
    click.secho(
        f"Saved {AwsSecretNames.cloud!s} as '{AwsBlockNames.cloud!s}'", fg="green"
    )

    dev_api_secret = AwsSecret(
        aws_credentials=credential, secret_name=AwsSecretNames.dev_api
    )
    dev_api_secret.save(AwsBlockNames.dev_api.value, overwrite=True)
    click.secho(
        f"Saved {AwsSecretNames.dev_api!s} as '{AwsBlockNames.dev_api!s}'", fg="green"
    )

    prod_api_secret = AwsSecret(
        aws_credentials=credential, secret_name=AwsSecretNames.prod_api
    )
    prod_api_secret.save(AwsBlockNames.prod_api.value, overwrite=True)
    click.secho(
        f"Saved {AwsSecretNames.prod_api!s} as '{AwsBlockNames.prod_api!s}'", fg="green"
    )

    Variable.set(name="flow_storage_bucket", value="prefect2.flow.code", overwrite=True)

    flow_storage_bucket = S3Bucket(
        bucket_name="prefect2.flow.code", bucket_folder="", credentials=credential
    )
    flow_storage_bucket.save(str(AwsBlockNames.flow_storage), overwrite=True)
    click.secho(
        f"Saved {AwsBlockNames.flow_storage!s} as '{AwsBlockNames.flow_storage!s}'",
        fg="green",
    )

    AwsCognitoCredentials.register_type_and_schema()
    click.secho(
        f"Registered Custom Block '{AwsCognitoCredentials._block_type_name}'",
        fg="green",
    )

    # Register the Dev and Prod AwsCognitoCredentials blocks
    dev_api_cognito = AwsCognitoCredentials(
        aws_credentials=credential, secret_name=AwsSecretNames.dev_api
    )
    dev_api_cognito.save(AwsBlockNames.dev_api, overwrite=True)
    click.secho(
        f"Saved {AwsSecretNames.dev_api!s} as '{AwsBlockNames.dev_api!s}'", fg="green"
    )

    prod_api_cognito = AwsCognitoCredentials(
        aws_credentials=credential, secret_name=AwsSecretNames.prod_api
    )
    prod_api_cognito.save(AwsBlockNames.prod_api, overwrite=True)
    click.secho(
        f"Saved {AwsSecretNames.prod_api!s} as '{AwsBlockNames.prod_api!s}'", fg="green"
    )

    # Register the Snowflake stage block
    db_dev_stage = AwsS3SnowflakeStage(
        bucket_name="dsci.snowflake.storage",
        credentials=credential,
        bucket_folder="thought_spot_tag_requests",
        env=Env.dev,
        stage_name="CPS_DSCI_STG.MY_CSV_STAGE",
    )
    db_dev_stage.save(AwsBlockNames.dev_sf_file_stage, overwrite=True)
    click.secho(
        f"Saved {AwsBlockNames.dev_sf_file_stage!s} as '{AwsBlockNames.dev_sf_file_stage!s}'",
        fg="green",
    )
    db_prod_stage = AwsS3SnowflakeStage(
        bucket_name="dsci.snowflake.storage",
        credentials=credential,
        bucket_folder="thought_spot_tag_requests",
        env=Env.prod,
        stage_name="CPS_DSCI_STG.MY_CSV_STAGE",
    )
    db_prod_stage.save(AwsBlockNames.prod_sf_file_stage, overwrite=True)
    click.secho(
        f"Saved {AwsBlockNames.prod_sf_file_stage!s} as '{AwsBlockNames.prod_sf_file_stage!s}'",
        fg="green",
    )

    dc_data_store = S3Bucket(
        bucket_name="dc-data-store", bucket_folder="", credentials=credential
    )
    dc_data_store.save(AwsBlockNames.dc_data_store, overwrite=True)
    click.secho(
        f"Saved {AwsBlockNames.dc_data_store!s} as '{AwsBlockNames.dc_data_store!s}'",
        fg="green",
    )

    # Register the OracleCX Secret
    prod_cx_oracle_secret = AwsSecret(
        aws_credentials=credential, secret_name=AwsSecretNames.prod_cx_oracle
    )
    prod_cx_oracle_secret.save(AwsBlockNames.prod_cx_oracle.value, overwrite=True)
    click.secho(
        f"Saved {AwsSecretNames.prod_cx_oracle!s} as '{AwsBlockNames.prod_cx_oracle!s}'",
        fg="green",
    )


def register_database() -> None:
    """
    Register database variables as defined in blocks.database
    """

    from common_prefect_next.blocks.database import DbSchemas, Warehouse

    warehouses = {k: str(v) for k, v in Warehouse.__members__.items()}

    Variable.set(name="warehouses", value=warehouses, overwrite=True)
    Variable.set(name="warehouse_xsmall", value=Warehouse.x_small, overwrite=True)
    Variable.set(name="warehouse_small", value=Warehouse.small, overwrite=True)
    Variable.set(name="warehouse_medium", value=Warehouse.medium, overwrite=True)
    Variable.set(name="warehouse_large", value=Warehouse.large, overwrite=True)
    Variable.set(name="warehouse_xlarge", value=Warehouse.x_large, overwrite=True)
    Variable.set(name="warehouse_huge", value=Warehouse.huge, overwrite=True)

    click.secho("Saved warehouse variables", fg="green")

    db_schemas = {k: str(v) for k, v in DbSchemas.__members__.items()}

    Variable.set(name="db_schemas", value=db_schemas, overwrite=True)
    Variable.set(name="db_schema_dev", value=DbSchemas.dev, overwrite=True)
    Variable.set(name="db_schema_prod", value=DbSchemas.prod, overwrite=True)

    click.secho("Saved database schema variables", fg="green")


def register_thoughtspot() -> None:
    """
    Register thoughtspot blocks as defined in blocks.thoughtspot
    """

    from common_prefect_next.blocks.thoughtspot import (
        AwsThoughtSpotBlockNames,
        ThoughtSpotSecretNames,
    )

    try:
        credential = AwsCredentials.load(AwsBlockNames.aws_credentials)
    except Exception as e:
        click.echo(
            f"Failed to load block {AwsBlockNames.aws_credentials!s}. These should be created manually in the UI",
            err=True,
        )
        raise click.Abort() from e

    generic_user = AwsSecret(
        aws_credentials=credential, secret_name=ThoughtSpotSecretNames.generic_user
    )
    generic_user.save(AwsThoughtSpotBlockNames.generic_user, overwrite=True)
    click.secho(
        f"Saved {ThoughtSpotSecretNames.generic_user!s} as {AwsThoughtSpotBlockNames.generic_user!s}",
        fg="green",
    )

    servers = {k: str(v) for k, v in ThoughtspotServerUrl.__members__.items()}

    Variable.set(name="thoughtspot_servers", value=servers, overwrite=True)
    Variable.set(name="thoughtspot_dev", value=ThoughtspotServerUrl.dev, overwrite=True)
    Variable.set(
        name="thoughtspot_prod", value=ThoughtspotServerUrl.prod, overwrite=True
    )

    click.secho("Saved thoughtspot server urls", fg="green")


def register_environment() -> None:
    """
    Register environment variables as defined in blocks.environment
    """
    from common_prefect_next.blocks.environment import Env

    Variable.set(name=Env.dev, value=Env.dev, overwrite=True)
    click.secho(f"Saved {Env.dev!s} as {Env.dev!s}", fg="green")
    Variable.set(name=Env.prod, value=Env.prod, overwrite=True)
    click.secho(f"Saved {Env.prod!s} as {Env.prod!s}", fg="green")


def register_docker() -> None:
    """
    Register docker variables
    """
    base = (
        "837578041534.dkr.ecr.us-east-1.amazonaws.com/bases/common-prefect-next:latest"
    )
    base_312 = "837578041534.dkr.ecr.us-east-1.amazonaws.com/bases/common-prefect-next:python-3.12"

    Variable.set(name="docker_base", value=base, overwrite=True)
    click.secho(f"Saved 'docker_base' {base}", fg="green")
    Variable.set(name="docker_base_312", value=base_312, overwrite=True)
    click.secho(f"Saved 'docker_base_312' {base_312}", fg="green")


def register_data_canvas() -> None:
    from common_prefect_next.blocks.data_canvas import (
        DataCanvasBlockNames,
        DataCanvasDomainEndpoints,
        DataCanvasEndpoints,
        DataCanvasNotificationBlock,
    )

    dev_domain = DataCanvasDomainEndpoints.dev.value
    dev_update_notification_status = urlunsplit(
        ("https", dev_domain, DataCanvasEndpoints.update_status.value, None, None)
    )
    dev_update_notification = urlunsplit(
        ("https", dev_domain, DataCanvasEndpoints.update_notification.value, None, None)
    )
    prod_domain = DataCanvasDomainEndpoints.prod.value
    prod_update_notification_status = urlunsplit(
        ("https", prod_domain, DataCanvasEndpoints.update_status.value, None, None)
    )
    prod_update_notification = urlunsplit(
        (
            "https",
            prod_domain,
            DataCanvasEndpoints.update_notification.value,
            None,
            None,
        )
    )
    Variable.set(
        name=DataCanvasBlockNames.dev.value,
        value={
            "domain": dev_domain,
            "update_notification_status": dev_update_notification_status,
            "update_notification": dev_update_notification,
        },
        overwrite=True,
    )
    click.secho("Saved 'dev' datacanvas endpoints", fg="green")
    Variable.set(
        name=DataCanvasBlockNames.prod.value,
        value={
            "domain": prod_domain,
            "update_notification_status": prod_update_notification_status,
            "update_notification": prod_update_notification,
        },
        overwrite=True,
    )
    click.secho("Saved 'prod' datacanvas endpoints", fg="green")

    DataCanvasNotificationBlock.register_type_and_schema()
    click.secho(
        f"Registered Custom Block '{DataCanvasNotificationBlock._block_type_name}'",
        fg="green",
    )

    # Get cognito blocks needed for DataCanvasNotificationBlock
    dev_cognito_block = AwsCognitoCredentials.load(name=AwsBlockNames.dev_api)

    dev_notification_block = DataCanvasNotificationBlock(
        aws_cognito_credentials=dev_cognito_block,
        env=Env.dev,
    )

    dev_notification_block.save(
        DataCanvasBlockNames.dev_notification.value, overwrite=True
    )
    click.secho(f"Saved '{DataCanvasBlockNames.dev_notification.value}'", fg="green")

    prod_cognito_block = AwsCognitoCredentials.load(name=AwsBlockNames.prod_api)

    prod_notification_block = DataCanvasNotificationBlock(
        aws_cognito_credentials=prod_cognito_block,
        env=Env.prod,
    )

    prod_notification_block.save(
        DataCanvasBlockNames.prod_notification.value, overwrite=True
    )
    click.secho(f"Saved '{DataCanvasBlockNames.prod_notification.value}'", fg="green")


def register_work_queue_names() -> None:
    from common_prefect_next.blocks.work_queues import WorkQueueNames

    work_queues = {k: str(v) for k, v in WorkQueueNames.__members__.items()}

    for k, v in work_queues.items():
        var_name = f"work_queue_{k}"
        Variable.set(name=var_name, value=v, overwrite=True)
        click.secho(f"Saved {var_name} as {v}", fg="green")

    click.secho("Saved work queue names", fg="green")
