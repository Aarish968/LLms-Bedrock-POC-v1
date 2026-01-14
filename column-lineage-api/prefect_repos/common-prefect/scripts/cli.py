import json
import click
import boto3


@click.group("cli")
def cli():
    ...


@cli.group("gul")
def gul():
    ...


@cli.group("aws_tasks")
def cognito():
    ...


@cognito.command("add-admin-user")
@click.argument("user_pool_id", type=str)
@click.argument("client_id", type=str)
@click.argument("user_email", type=str)
def add_admin_user(user_pool_id, client_id, user_email):
    client = boto3.client("aws_tasks-idp")
    # Create a temporary password for the user
    temporary_password = click.prompt(
        hide_input=True, text="Temporary Password?", confirmation_prompt=True
    )

    response = client.admin_initiate_auth(
        UserPoolId=user_pool_id,
        ClientId=client_id,
        AuthFlow="ADMIN_NO_SRP_AUTH",
        AuthParameters={"USERNAME": user_email, "PASSWORD": temporary_password},
    )
    print(response)
    # Now we need to set the password to something else


@gul.command("get-gul")
def get_gul():
    s3 = boto3.client("s3")
    response = s3.get_object(
        Bucket="data.canvas.thought.spot.generic.upload.cisco.com",
        Key="generic-upload-types-dict/gul-types-dict.json",
    )
    print(response["Body"].read().decode("utf-8"))


@gul.command("remove-gul-entry")
def remove_gul_entry():
    import yaml

    s3 = boto3.resource("s3")
    gul_obj = s3.Object(
        "data.canvas.thought.spot.generic.upload.cisco.com",
        "generic-upload-types-dict/gul-types-dict.json",
    )
    gul_data = gul_obj.get()["Body"].read().decode("utf-8")
    gul_dict = yaml.safe_load(gul_data)
    click.echo("Removing entry from GUL")
    click.echo(f"Available keys: {list(gul_dict.keys())}")
    key = click.prompt("Key", type=str)
    click.echo(f"Key: {key}")
    click.confirm("Are you sure?", abort=True)
    if key not in gul_dict:
        click.echo(f"Key {key} does not exist in GUL")
        click.echo("Exiting...")
        return
    gul_dict.pop(key)
    gul_obj.put(Body=json.dumps(gul_dict))


@gul.command("add-gul-entry")
@click.option("--key", required=True, type=str, prompt=True)
@click.option("--version-group-id", required=True, type=str, prompt=True)
def add_gul_entry(key, version_group_id):
    import yaml

    click.echo("Adding entry to GUL")
    click.echo(f"Key: {key}")
    click.echo(f"Version Group ID: {version_group_id}")
    click.confirm("Are you sure?", abort=True)
    s3 = boto3.resource("s3")
    gul_obj = s3.Object(
        "data.canvas.thought.spot.generic.upload.cisco.com",
        "generic-upload-types-dict/gul-types-dict.json",
    )
    gul_data = gul_obj.get()["Body"].read().decode("utf-8")
    gul_dict = yaml.load(gul_data, Loader=yaml.FullLoader)
    version_group_id = version_group_id.strip()
    if key in gul_dict:
        if not click.confirm(
            f"Key {key} already exists. Do you want to overwrite it?", abort=True
        ):
            click.echo("Aborted!")
    gul_dict[key] = version_group_id
    # Upload the new dict to S3
    gul_obj.put(Body=json.dumps(gul_dict))


if __name__ == "__main__":
    cli()
