from pathlib import Path
from typing import Optional, Union

from prefect.storage import Docker

DEFAULT_BASE_IMAGE="837578041534.dkr.ecr.us-east-1.amazonaws.com/bases/common-prefect",
DEFAULT_VERSION="latest"
DEFAULT_REGISTRY_URL="837578041534.dkr.ecr.us-east-1.amazonaws.com/dc/ts/p1"


def get_src(fp: Path) -> Path:
    return fp.parent.parent

def build_docker_storage(
    flow_file_path: Union[str, Path], base_image: Optional[str] = None, registry_url: Optional[str] = None
    ):
    """
    Build a Docker storage object for a Prefect project. Using this function will require common_tasks to be available at build time.
    
    This will install your flow in the /opt/your_project_name directory of the container.
    
    Parameters
    ----------
    registry_url
    flow_file_path: The file path of the file calling this function. I.e. __file__.

    Notes
    -------
    The structure of your repository should be as follows:
    ```
    ├── src
    │   ├── your_project_name
    │   │   ├── __init__.py
    │   │   ├── main.py
    │   │   ├── other_local_modules
    │   ├── build_requirements.txt
    │   ├── flow_requirements.txt
    │   ├── .dockerignore
    │   ├── buildspec.yml
    │   ├── pyproject.toml

    """
    flow_src = get_src(flow_file_path)
    package_name = get_src(flow_file_path).parent.name
    storage_obj = Docker(
        base_image=base_image or DEFAULT_BASE_IMAGE,
        registry_url=registry_url or DEFAULT_REGISTRY_URL,
        dockerignore=str(get_src(flow_file_path) / ".dockerignore"),
        stored_as_script=True,
        path=str(Path("/opt") / flow_file_path.relative_to(flow_src)),
        extra_dockerfile_commands=[
            "RUN pip install -r /tmp/flow_requirements.txt",
        ],
        files={
            str(flow_src / "flow_requirements.txt"): "/tmp/flow_requirements.txt",
            str(flow_src / ".dockerignore"): "/tmp/.dockerignore",
            str(flow_src / package_name / "."): str(Path("/opt") / package_name),
        },
        env_vars={
            "PYTHONPATH": f"$PYTHONPATH:/opt/{package_name}",
            "AWS_DEFAULT_REGION": "us-east-1"
        },
        secrets=["AWS_CREDENTIALS"],
    
    )
    return storage_obj
