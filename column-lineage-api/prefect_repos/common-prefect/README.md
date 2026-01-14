# Common Prefect Tasks

## Setting Up

This is a brief overview of integrating with the internal team's Prefect tasks.

### CodeCommit Specifics

[AWS CLI](https://aws.amazon.com/cli/) will need to be installed and configured.

Visit [duo-sso](https://wwwin-github.cisco.com/ATS-operations/duo-sso) and follow the instructions to install the binary
application

Once you've been able to authenticate, you need to configure git to use the credential helper. This will leverage your
Duo SSO session token
to authenticate with CodeCommit.

```toml
[credential "https://git-codecommit.us-east-1.amazonaws.com*"]
helper = !aws codecommit credential-helper $@
UseHttpPath = true
```

## Creating a Flow

- Create a new project and virtual environment
- Add 'prefect==0.15.13' to your **dev** requirements. We will include this in the build requirements.
- Add `'common-tasks @ git+https://git-codecommit.us-east-1.amazonaws.com/v1/repos/common-prefect'` to your **dev**
  requirements. We will build this as a wheel in the build process.

### Docker Storage

This is not the best option, but the one in use.

```python
from prefect.storage import Docker
from pathlib import Path


storage_obj = Docker(
    base_image="837578041534.dkr.ecr.us-east-1.amazonaws.com/bases/prefect:0.15.13-python3.9",
    registry_url="837578041534.dkr.ecr.us-east-1.amazonaws.com/dc/p1",
    dockerignore=str(Path(__file__).parent / ".dockerignore"),
    extra_dockerfile_commands=[
        "RUN pip install -r /tmp/flow_requirements.txt",
    ],
    path="/opt/name_of_your_flow/main.py",
    files={
        str(Path(__file__).parent / "."): "/opt/name_of_your_flow/"  # Trailing slash is important
        # Add and files not in the CWD
    },
    env_vars={"PYTHONPATH": "$PYTHONPATH:/opt/name_of_your_flow", "AWS_DEFAULT_REGION": "us-east-1"},
    secrets=["AWS_CREDENTIALS"],
    stored_as_script=True
)
```

## Including `common-tasks` in your Flow

Assuming this is being built on AWS CodeCommit. We have to jump though a few hoops to get the package installed as its
hosted on AWS CodeCommit.

The 'runtime' image can have permission to access the git repo. However, when prefect builds the image, it does not have
the same permissions.
Before we get to this stage, we will build it as a wheel and include it in the image.

First, modify your storage object to include the following:

```python
from src.common_tasks import add_wheels


storage_obj = add_wheels(storage_obj)
```

### buildspec.yaml

Add the following to your buildspec.yaml file. git-credential-helper will allow the build process to access the git
repo.

```yaml
  env:
    variables:
      TENANT: cisco-dev
      FLOW_PATH: main.py
      PROJECT_NAME: "ElasticSearch Canvas Load"
    secrets-manager:
      API_KEY: prefect-api-key:prefect-api-key
    git-credential-helper: yes
```

Add one additional command to the `install` section of the buildspec.yaml file. This will build the wheel and store it
in the `wheels` directory.

```yaml
  install:
    runtime-versions:
      python: 3.9
    commands:
      - python3 -m pip install --upgrade pip
      - pip install -r build_requirements.txt
      - echo 'Building Wheels' && pip wheel 'common-tasks @ git+https://git-codecommit.us-east-1.amazonaws.com/v1/repos/common-prefect' --no-deps --wheel-dir ./wheels
```

And to make it available for the build. This is called immediately before prefect register

```yaml
build:
  commands:
    - pip install ./wheels/*.whl
```
