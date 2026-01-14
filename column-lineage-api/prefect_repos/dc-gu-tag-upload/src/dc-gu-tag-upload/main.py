import io
import json
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import quote
from zoneinfo import ZoneInfo

import boto3
import numpy as np
import pandas as pd
import prefect
from common_tasks.notifications.api_logger import ApiNotificationHandler
from common_tasks.notifications.auth import GetAccessToken, get_access_token
from common_tasks.settings import SettingsBase, TEnv, Warehouse
from common_tasks.utils import log_queries, parse_s3_uri
from prefect import Flow, Parameter, task
from prefect.engine.results import S3Result
from prefect.engine.signals import FAIL, SKIP
from prefect.executors import LocalExecutor
from prefect.run_configs.kubernetes import KubernetesRun
from prefect.storage import Docker
from prefect.tasks.prefect import RenameFlowRun
from pydantic import Field
from pydantic.main import BaseModel
from sqlalchemy import Integer, create_engine, text


UTC = ZoneInfo("UTC")


class Settings(SettingsBase):
    
    api_logger: Optional[ApiNotificationHandler]
    
    def __init__(
        self, env: TEnv, warehouse: Warehouse, request_id: int, upload_uri: str,
        dc_engagement_id: int, requested_by: str, auth_token: Optional[str]
    ):
        super().__init__(env, warehouse)
        self.request_id = request_id
        self.upload_uri = upload_uri
        self.dc_engagement_id = dc_engagement_id
        self.requested_by = requested_by
        self.auth_token = auth_token
        self.notification_id = None
        self.api_logger = None
    
    @property
    def message_endpoint(self):
        origin = "datacanvaswf.cisco.com" if self.env == "prod" else "devdatacanvaswf.cisco.com"
        
        return f"https://{origin}/api/v2/workflows/notifications/message/{self.notification_id}?logged_user={quote(self.requested_by)}"


class WriteTagSPParams(BaseModel):
    user_id: str = Field(alias="userId")
    engagement_id: int = Field(alias="engagementId")
    instance_ids: list[int] = Field(alias="instance")
    comment: str = Field(default="", alias="comment")
    tag_id: int = Field(alias="tagId")
    ddl_action: str = "set"
    
    
    class Config:
        allow_population_by_field_name = True
        use_enum_values = True


@task(log_stdout=True)
def get_settings(
    env: TEnv, request_id: int, bucket: list[str], upload_uri: str, object_key: str
) -> Settings:
    bucket_name = bucket[0]
    client = boto3.client("s3")
    
    with io.BytesIO() as buffer:
        client.download_fileobj(bucket_name, object_key, buffer)
        raw = buffer.getvalue().decode("utf-8")
    data = json.loads(raw)
    dc_engagement_id = data["engagementId"]
    requested_by = data["requestedBy"]
    
    auth_token = get_access_token(env=env)
    
    settings = Settings(
        env, Warehouse.x_small, request_id=request_id, upload_uri=upload_uri,
        dc_engagement_id=dc_engagement_id, requested_by=requested_by,
        auth_token=auth_token
    )
    RenameFlowRun().run(flow_run_name=f"dc-gu-tags-upload-{request_id}")
    
    notification_id = create_job(settings)
    settings.notification_id = notification_id
    
    api_logger = ApiNotificationHandler(
        auth_token=settings.auth_token, api_url=settings.message_endpoint,
        notification_id=notification_id
        )
    
    settings.api_logger = api_logger
    
    return settings


def get_timestamp():
    return datetime.now(UTC).isoformat()


def get_engine(settings: Settings, warehouse: Optional[Warehouse] = None):
    return create_engine(
        settings.get_db_url(warehouse=warehouse), connect_args={
            "log_max_query_length": 10_000, "client_session_keep_alive": True,
            "disable_ocsp_checks": True
        }, )

def create_job(settings: Settings) -> int:
    # Insert into dc_wf_background_job
    
    xs_engine = get_engine(settings, warehouse=Warehouse.x_small)
    user_id_query = text(
        """
        SELECT USER_ID FROM DC_USERS WHERE CISCO_CCO_ID = :requested_by
        AND IS_DELETED = 'F'
        LIMIT 1
        """
    )
    
    with xs_engine.begin() as conn:
        try:
            user_id = conn.execute(
                user_id_query.bindparams(requested_by=settings.requested_by)
            ).scalar_one()
        except Exception as e:
            raise FAIL(
                f"Requested_by '{settings.requested_by}' not found in DC_USERS"
            ) from e
    
    stmt = text(
        """
        INSERT INTO DC_WF_BACKGROUND_JOB
        (REQUEST_ID, DC_ENGAGEMENT_ID, DC_USER_ID, EXTERNAL_JOB_ID, EXTERNAL_RUN_ID, CREATED_BY, CREATE_DTM, IS_DELETED)
        VALUES (:request_id, :dc_engagement_id, :dc_user_id, :external_job_id, :external_run_id, :created_by, CURRENT_TIMESTAMP, 'F')
        
        """
    ).bindparams(
        request_id=settings.request_id, dc_engagement_id=settings.dc_engagement_id,
        dc_user_id=user_id,
        external_job_id=prefect.context.get("flow_state_version"),
        external_run_id=prefect.context.get("flow_run_id"),
        created_by=settings.requested_by,
    )
    
    with xs_engine.begin() as conn:
        conn.execute(stmt)
    
    # Create the notification
    # qUERY THE TREE_D
    # Consume a dc_wf_notification_seq
    
    tree_id_stmt = text(
        """
        SELECT TREE_ID FROM DC_WF_ACTION_ITEM
        WHERE UI_ENUM = 'instance-tagging'
        """
    ).columns(tree_id=Integer)
    
    stmt = text(
        """
        INSERT INTO DC_WF_NOTIFICATION
        (NOTIFICATION_ID, TREE_ID, DC_ENGAGEMENT_ID, NOTIFICATION_CATEGORY, SUBJECT, DATA, REQUEST_ID, DC_USER_ID, CREATED_BY, CREATE_DTM, IS_DELETED)
        VALUES (:notification_id, :tree_id, :dc_engagement_id, :notification_category, :subject, :data, :request_id, :user_id, :created_by, CURRENT_TIMESTAMP, 'F')
        """
    )
    
    with xs_engine.begin() as conn:
        notification_id = conn.execute(
            "SELECT DC_WF_NOTIFICATION_SEQ.NEXTVAL"
        ).scalar_one()
        tree_id = conn.execute(tree_id_stmt).scalar_one()
        conn.execute(
            stmt.bindparams(
                notification_id=notification_id, tree_id=tree_id,
                dc_engagement_id=settings.dc_engagement_id,
                notification_category="pending",
                subject="Instance Tagging (Via Generic Upload)", data="[]",
                request_id=settings.request_id, created_by=settings.requested_by, user_id=user_id)
        )
    return notification_id


@task(log_stdout=True)
def load_excel(settings: Settings) -> pd.DataFrame:
    session = boto3.Session()
    s3 = session.client("s3")
    
    upload_bucket, upload_key = parse_s3_uri(settings.upload_uri)
    
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        s3.download_fileobj(upload_bucket, upload_key, tmp_file)
    
    df = pd.read_excel(
        tmp_file.name, sheet_name="InstanceID - Tag mapping", engine="openpyxl"
    )
    try:
        df = df[["Instance_ID", "Tag_ID"]].dropna().astype("int64", errors="raise")
    except Exception as e:
        msg = "Either the 'Instance_ID' or 'Tag_ID' columns are missing or contain non-integer values"
        settings.api_logger.send_text(msg, status='error')
        raise SKIP(
            msg
        ) from e
    
    if df.empty:
        msg = "The uploaded file is empty - nothing to do"
        settings.api_logger.send_text(msg, status='error')
        raise SKIP(msg)
    
    return df


@task(log_stdout=True)
def get_tagset_ids(id_df: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    """
    id_df: [['Instance_ID', 'Tag_ID']]

    Query and join tagset_id to the dataframe

    """
    
    # We get a resuilt ot tag_id -> tagset_id
    # Snowflake has limit of 200,000 items in a list
    logger = prefect.context.get("logger")
    batch_size = prefect.context.get("batch_size", 100_000)
    tag_ids = id_df["Tag_ID"].unique()
    logger.info(f"Querying {len(tag_ids)} tag_ids")
    
    n_batches = len(tag_ids) // batch_size + 1
    logger.info(f"Splitting into {n_batches} batches")
    batches = np.array_split(tag_ids, n_batches)
    
    stmt = text(
        """
        SELECT TAG_ID, TAGSET_ID
        FROM DC_TAGS WHERE TAG_ID IN (:tag_ids)
        AND IS_DELETED = 'F'
        """
    )
    
    engine = get_engine(settings)
    
    results = []
    with engine.begin() as conn:
        for i, batch in enumerate(batches):
            logger.info(f"Querying batch {i + 1} of {n_batches}")
            res = conn.execute(
                stmt.bindparams(tag_ids=batch.tolist()).columns(
                    tag_id=Integer, tagset_id=Integer
                )
            ).all()
            results.extend((row.tag_id, row.tagset_id) for row in res)
    if not results:
        logger.info("No tagsets found")
        msg = "The supplied tag_ids could not be joined to any tagsets - nothing to do"
        settings.api_logger.send_text(msg, status='error')
        raise SKIP(
            msg
        )
    tagsets_df = pd.DataFrame(results, columns=["Tag_ID", "Tagset_ID"], dtype="int64")
    
    joined_df = id_df.merge(tagsets_df, on="Tag_ID", how="left")
    
    # Check for rows where Tagset_ID is missing
    missing_tagset_idx = joined_df["Tagset_ID"].isna()
    if missing_tagset_idx.any():
        missing_tag_ids = joined_df.loc[missing_tagset_idx, "Tag_ID"].unique()
        msg = f"These tag_ids could not be joined to any tagsets: {missing_tag_ids}"
        logger.info(
            msg
        )
        settings.api_logger.send_text(msg)
    
    joined_df = joined_df.dropna(subset=["Tagset_ID"])
    
    tagset_counts = joined_df.groupby(["Instance_ID"])["Tagset_ID"].count()
    tagset_duplicated = tagset_counts[tagset_counts > 1].index
    
    if tagset_duplicated.any():
        msg = f"Instance_IDs tagged multiple times with the same tagset_id: {tagset_duplicated}"
        settings.api_logger.send_text(msg)
        joined_df = joined_df[joined_df["Instance_ID"].notin(tagset_duplicated)]
    
    return joined_df


@task(log_stdout=True)
@log_queries
def call_tag_instances_sp(joined_df: pd.DataFrame, settings: Settings):
    # We can call the stored procedure with
    
    batch_size = prefect.context.get("sp_batch_size", 20_000)
    logger = prefect.context.get("logger")
    tag_procedure = prefect.context.get("tag_procedure", "tag_instances_11")
    
    stmt = text("CALL IDENTIFIER(:proc_name)(:params)")
    
    # Group by tag_id
    
    def make_pipeline(df_inner: pd.DataFrame):
        tag_ids = df_inner["Tag_ID"].unique()
        for tag_id in tag_ids:
            logger.info(f"Processing {tag_id=}")
            df_slice = df_inner.loc[df_inner["Tag_ID"] == tag_id]
            n_batches = len(df_slice) // batch_size + 1
            batches = np.array_split(df_slice, n_batches)
            for i, batch in enumerate(batches):
                logger.info(f"Processing batch {i + 1}/{n_batches}")
                yield make_sp_payload(
                    tag_id=tag_id, batch=batch["Instance_ID"].tolist()
                )
    
    def make_sp_payload(tag_id, batch: list[int]):
        model = WriteTagSPParams(
            user_id=settings.requested_by, engagement_id=settings.dc_engagement_id,
            instance_ids=batch, comment="", tag_id=tag_id, )
        return model.json(by_alias=True, separators=(",", ":"))
    
    pipeline = make_pipeline(joined_df)
    xs_engine = get_engine(settings, warehouse=Warehouse.x_small)
    with xs_engine.begin() as conn:
        for payload in pipeline:
            try:
                res = conn.execute(
                    stmt.bindparams(proc_name=tag_procedure, params=payload)
                ).scalar()
                logger.info(f"Result: {res}")
            except Exception as e:
                logger.exception("Error calling stored procedure")
                settings.api_logger.send_exception(message="Error calling stored procedure", exception=e)
                raise FAIL("Error calling stored procedure") from e


def get_src():
    return Path(__file__).parent.parent


token_task = GetAccessToken()

storage_obj = Docker(
    base_image="837578041534.dkr.ecr.us-east-1.amazonaws.com/bases/common-prefect:latest",
    image_name="dc-gu-tags-upload",
    dockerignore=str(get_src() / ".dockerignore"),
    extra_dockerfile_commands=[
        """
        RUN python -m pip install --no-cache-dir -r /tmp/flow_requirements.txt \
        && pip install /wheels/*.whl
        """
                               ],
    files={
        str(get_src() / "flow_requirements.txt"): "/tmp/flow_requirements.txt",
        str(get_src() / ".dockerignore"): "/tmp/.dockerignore",
        str(get_src() / "dc-gu-tag-upload" / "."): "/opt/dc-gu-tag-upload/",
    }, registry_url="837578041534.dkr.ecr.us-east-1.amazonaws.com/dc/gu/p1",
    path="/opt/dc-gu-tag-upload/main.py",
    env_vars={"PYTHONPATH": "${PYTHONPATH}:/", "AWS_DEFAULT_REGION": "us-east-1"},
    stored_as_script=True, secrets=["AWS_CREDENTIALS"], )

storage_obj.installation_commands = [step for step in storage_obj.installation_commands if "pip show prefect" not in step]

with Flow(
    "dc-gu-tags-upload", storage=storage_obj,
    run_config=KubernetesRun(
        labels=["dev"], memory_request="2G", job_template={
            "apiVersion": "batch/v1", "kind": "Job", "spec": {
                "ttlSecondsAfterFinished": 300,
                "template": {"spec": {"containers": [{"name": "flow"}]}},
            },
        }, ), executor=LocalExecutor(),
    result=S3Result(bucket="cam-prefect-results"), ) as flow:
    env_p = Parameter("env", required=True)
    request_id_p = Parameter("request_id", required=True)
    object_key_p = Parameter("object_key", required=True)
    bucket_name_p = Parameter("bucket_name", required=True)
    file_location_p = Parameter("file_location", required=True)
    
    settings_result = get_settings(
        env=env_p, request_id=request_id_p, bucket=bucket_name_p,
        upload_uri=file_location_p, object_key=object_key_p
    )
    id_df = load_excel(settings=settings_result)
    full_df = get_tagset_ids(id_df=id_df, settings=settings_result)
    
    api_response = call_tag_instances_sp(joined_df=full_df, settings=settings_result)

if __name__ == "__main__":
    flow.run(
        
        parameters={
            "bucket_name": ["dc-generic-upload-trigger-messaging.prod"],
            "env": "prod",
            "file_location": "s3://dc-generic-upload-trigger-messaging.prod/prod/requested_files/2024_269873.xlsx",
            "object_key": "prod/requests/536-269873.json",
            "request_id": "269873"
        }
    )
