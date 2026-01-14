# Blocks

## Using As a Library

```bash
uv add 'common_prefect_next @ https://git-codecommit.us-east-1.amazonaws.com/v1/repos/common_prefect_next.git'
```
OR Install with Extras
```bash
uv add 'common_prefect_next[all] @ https://git-codecommit.us-east-1.amazonaws.com/v1/repos/common_prefect_next.git'
```

## Updating AWS Credentials

When rotating an Access Key for the IAM User [prefect-3-cloud](https://us-east-1.console.aws.amazon.com/iam/home?region=us-east-1#/users/details/prefect-3-cloud?section=permissions),
the new Access Key and Secret Access Key must be entered via Prefect Cloud in the UI. Edit the [aws-credentials block](https://app.prefect.cloud/account/421a2ff7-ff12-46b7-9d43-8f012c9bb18a/workspace/70c6c7e5-7706-4c2d-a3be-7cd81a5ce5c4/settings/blocks/block/4a5f02a0-8584-4e78-861c-8d00a4c35d04).

-----

Register Database Variables with Prefect

```bash
uv run cli register db
```

Register Thoughtspot Blocks and Variables with Prefect

```bash
uv run cli register ts
```

Register Environment Variables with Prefect

```bash
uv run cli register env
```

Register Docker Image Variables with Prefect

```bash
uv run cli register docker
```

Register Data Canvas Variables with Prefect

```bash
uv run cli register data_canvas
```

Register Work Queue Variables with Prefect

```bash
uv run cli register work_queues
```

## Docker Image

This Docker image is intended to include all the necessary dependencies for running Prefect v3 flows along with internal dependencies.

### Building the Docker Image

See scripts/build_base_image.sh

## Deployment Steps

Including private repositories in the flow

[build_code_bundle](./src/common_prefect_next/deployments/steps.py:build_code_bundle)

This will create a .tar.gz file with the flow code built as a .whl file and any additional packages that are required.