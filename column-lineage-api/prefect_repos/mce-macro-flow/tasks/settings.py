from __future__ import annotations

from prefect import task

from common.config import Env, FlowEnv, RunSettings


@task(log_stdout=True)
def get_run_settings(env: Env, flow_env: FlowEnv) -> RunSettings:
    settings = RunSettings(env=env, flow_env=flow_env)
    print(f"Run Settings: {settings}")
    return settings
