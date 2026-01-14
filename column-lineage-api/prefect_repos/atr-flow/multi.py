from collections import defaultdict
from pathlib import Path

from prefect import task, Flow, Parameter
from prefect.engine.results import LocalResult
from prefect.engine.serializers import JSONSerializer
from prefect.executors import LocalDaskExecutor

from main import flow as atr_flow


class Config:
    RESULT_PATH = str(Path().cwd() / "flows")
    REQUEST_ID_DIR = str(Path(RESULT_PATH) / "request_ids")
    REQUEST_ID_LOCATION = "request_ids.json"
    REQUEST_ID_PATH = str(Path(REQUEST_ID_DIR) / REQUEST_ID_LOCATION)


RequestIdResult = LocalResult(
    dir=Config.REQUEST_ID_DIR,
    location=Config.REQUEST_ID_LOCATION,
    serializer=JSONSerializer(),
)


@task(log_stdout=True, result=RequestIdResult)
def generate_request_ids(input_folder):
    file2id = defaultdict()
    file2id.default_factory = lambda: f"{len(file2id) + 1}".zfill(6)
    for file in Path(input_folder).iterdir():
        if file.is_file() and not file.name.startswith("."):
            _ = file2id[file.name]
    return dict(file2id)


@task(log_stdout=True)
def get_input_files(input_folder) -> list[dict]:
    if not RequestIdResult.exists(RequestIdResult.location):
        raise ValueError("RequestIdResult does not exist")
    file2id = RequestIdResult.read(RequestIdResult.location).value
    files = []
    for file_name, request_id in file2id.items():
        file_path = Path(input_folder) / file_name
        if file_path.exists():
            files.append({"file_name": str(file_path), "request_id": request_id})

    print(files)
    return files


@task(log_stdout=True)
def get_request_id(file_name):
    if not RequestIdResult.exists(RequestIdResult.location):
        raise ValueError("RequestIdResult does not exist")
    file2id = RequestIdResult.read(RequestIdResult.location).value
    return file2id[file_name]


def get_request_id_from_data(data, **kwargs):
    flow_name = kwargs.get("flow_name")
    task_name = kwargs.get("task_name")
    request_id = data["request_id"]
    return f"{flow_name}/{task_name}/{request_id}"


@task(
    log_stdout=True,
    target=get_request_id_from_data,
    result=LocalResult(dir=Config.RESULT_PATH),
)
def run_atr_flow(data):
    file_name = data["file_name"]
    request_id = data["request_id"]
    print(f"Running ATR flow for {file_name} with request_id {request_id}")
    params = {"file_location": file_name, "request_id": request_id, "flow_env": "dev"}
    state = atr_flow.run(parameters=params)
    return request_id


with Flow("multi-atr-flow", executor=LocalDaskExecutor()) as flow:
    input_folder_p = Parameter(
        "input_folder",
        default=r"C:\Users\estasney\PycharmProjects\NewAtrByCams\flows\input",
    )
    request_ids = generate_request_ids(input_folder=input_folder_p)
    input_files = get_input_files(
        input_folder=input_folder_p, upstream_tasks=[request_ids]
    )
    result = run_atr_flow.map(data=input_files)

flow.run()
