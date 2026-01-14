import imp
from operator import imod
from prefect import task, Flow, Parameter, case
import prefect
from prefect.client import Secret
from subprocess import run, PIPE, STDOUT
from prefect.tasks.control_flow import merge
from prefect.run_configs.kubernetes import KubernetesRun
from prefect.storage.codecommit import CodeCommit
from prefect.storage import Docker
logger = prefect.context.get("logger")

prefect_api_key = Secret("PREFECT_API_KEY").get()

# pip list --format=freeze > requirements.txt


@task
def run_cmd(cmd, cwd):
    command = run(
        cmd,
        cwd=cwd,
        stdout=PIPE,
        stderr=STDOUT,
    )
    logger.info(command.stdout.decode())
    return cwd


repo_path = Parameter("Repo Path", default="")
repo_user = Parameter("Github User", default="")
provider = Parameter("Repo Provider", default="codecommit")
python_dependecies = Parameter("Python dependencies", default="requirements.txt")
prefect_project = Parameter("Prefect Project", default="testing")
prefect_tenant = Parameter("Prefect Tenant", default="cisco-dev")

storage = Docker(
    base_image="837578041534.dkr.ecr.us-east-1.amazonaws.com/flows/flow-registration:latest",
    registry_url="837578041534.dkr.ecr.us-east-1.amazonaws.com/flows"
    )
    

with Flow(
    "Register Flow",
    run_config=KubernetesRun(),
    storage=storage
) as flow:
    with case(provider, "github"):
        github_repo = run_cmd(
            cmd=[
                "git",
                "clone",
                f"https://github.com/{repo_user.run()}/{repo_path.run()}.git",
            ],
            cwd="./",
        )

    with case(provider, "codecommit"):
        codecommit_repo = run_cmd(
            cmd=[
                "git",
                "clone",
                f"codecommit::us-east-1://{repo_path.run()}",
                repo_path,
            ],
            cwd="./",
        )

    dependencies = run_cmd(
        cmd=["pip", "install", "-r", python_dependecies],
        cwd=repo_path,
        upstream_tasks=[python_dependecies],
    )

    switch_to_tenant = run_cmd(
        cmd=["prefect", "auth", "login", "-k", prefect_api_key],
        cwd=repo_path,
        upstream_tasks=[dependencies],
    )

    switch_to_tenant = run_cmd(
        cmd=["prefect", "auth", "switch-tenants", "--slug", prefect_tenant],
        cwd=repo_path,
        upstream_tasks=[dependencies],
    )

    register = run_cmd(
        cmd=["prefect", "register", "-p", "flow.py", "--project", prefect_project],
        cwd=repo_path,
        upstream_tasks=[switch_to_tenant],
    )


if __name__ == "__main__":
    flow.run()
