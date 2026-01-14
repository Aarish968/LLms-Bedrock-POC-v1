import datetime

from pydantic import BaseModel, ConfigDict
from sqlalchemy import Connection, Integer, String, text

from common_prefect_next.logging.models.messages import (
    CreateMessage,
    Message,
    MessageStatus,
    UiEnum,
)
from common_prefect_next.utils import isoformat_utc, json_dumps


class Model(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        use_enum_values=True,
        json_encoders={datetime.datetime: isoformat_utc},
    )


class DCUserRow(Model):
    dc_user_id: int
    cisco_cco_id: str


class DCBackgroundJobRow(Model):
    dc_user_id: int
    cisco_cco_id: str
    dc_engagement_id: int
    parameters: dict | None
    external_job_id: str | None
    external_run_id: str | None
    canvas_id: int | None
    request_id: int
    ui_enum: UiEnum
    tree_id: int | None


class DCNotificationRow(BaseModel):
    dc_user_id: int
    cisco_cco_id: str
    dc_engagement_id: int
    messages: list[Message | CreateMessage] | None
    subject: str
    notification_category: MessageStatus
    request_id: int | None
    notification_id: int
    tree_id: int


def get_notification_id(conn: "Connection") -> int:
    """Retrieve a new id for a notification"""
    stmt = text("select dc_wf_notification_seq.nextval")
    result = conn.execute(stmt).scalar_one()
    return result


def get_request_id(conn: "Connection") -> int:
    """Retrieve a new id for a request"""
    stmt = text("select seq_dc_request.nextval")
    result = conn.execute(stmt).scalar_one()
    return result


def get_dc_user_from_id(dc_user_id: int, conn: "Connection") -> DCUserRow:
    """Retrieve a user row"""
    stmt = (
        text(
            """
        SELECT USER_ID AS dc_user_id,
         CISCO_CCO_ID AS cisco_cco_id
        FROM DC_USERS
        WHERE USER_ID = :dc_user_id
        AND IS_DELETED = 'F'
        """
        )
        .bindparams(dc_user_id=dc_user_id)
        .columns(dc_user_id=Integer, cisco_cco_id=String)
    )
    result = conn.execute(stmt).mappings().one()
    return DCUserRow(
        cisco_cco_id=result["cisco_cco_id"], dc_user_id=result["dc_user_id"]
    )


def get_tree_id_from_enum(enum: UiEnum, conn: "Connection") -> int:
    """Retrieve a tree_id from a ui_enum"""
    stmt = (
        text(
            """
        SELECT TREE_ID
        FROM DC_WF_ACTION_ITEM
        WHERE UI_ENUM = :ui_enum
        AND IS_DELETED = 'F'
        """
        )
        .bindparams(ui_enum=str(enum))
        .columns(tree_id=Integer)
    )
    result = conn.execute(stmt).scalar_one()
    return result


def create_job(
    dc_user_id: int,
    dc_engagement_id: int,
    parameters: dict | None,
    external_job_id: str | None,
    external_run_id: str | None,
    canvas_id: int | None,
    ui_enum: UiEnum,
    conn: "Connection",
    request_id: int | None = None,
) -> DCBackgroundJobRow:
    """
    Creates a background_job
    """
    request_id = request_id or get_request_id(conn=conn)
    db_user = get_dc_user_from_id(dc_user_id=dc_user_id, conn=conn)
    cisco_cco_id = db_user.cisco_cco_id
    parameters = parameters or {}
    tree_id = get_tree_id_from_enum(enum=ui_enum, conn=conn)

    stmt = text(
        """
        INSERT INTO dc_wf_background_job (
            REQUEST_ID,
            DC_ENGAGEMENT_ID,
            DC_USER_ID,
            CREATED_BY,
            CREATE_DTM,
            CANVAS_ID,
            PARAMETERS,
            EXTERNAL_JOB_ID,
            EXTERNAL_RUN_ID,
            WORKFLOW_ENUM,
            IS_DELETED
        ) VALUES (
            :request_id,
            :dc_engagement_id,
            :dc_user_id,
            :cisco_cco_id,
            SYSDATE(),
            :canvas_id,
            :parameters,
            :external_job_id,
            :external_run_id,
            :workflow_enum,
            'F'
        )
        """
    ).bindparams(
        request_id=request_id,
        dc_engagement_id=dc_engagement_id,
        dc_user_id=dc_user_id,
        cisco_cco_id=cisco_cco_id,
        canvas_id=canvas_id,
        parameters=json_dumps(parameters),
        external_job_id=external_job_id,
        external_run_id=external_run_id,
        workflow_enum=str(ui_enum),
    )

    conn.execute(stmt)

    return DCBackgroundJobRow(
        dc_user_id=dc_user_id,
        cisco_cco_id=cisco_cco_id,
        dc_engagement_id=dc_engagement_id,
        parameters=parameters,
        external_job_id=external_job_id,
        external_run_id=external_run_id,
        canvas_id=canvas_id,
        request_id=request_id,
        ui_enum=ui_enum,
        tree_id=tree_id,
    )


def create_notification(
    dc_user_id: int,
    dc_engagement_id: int,
    messages: list[Message | CreateMessage] | None,
    cisco_cco_id: str,
    subject: str | None,
    request_id: int | None,
    tree_id: int,
    conn: "Connection",
    notification_id: int | None = None,
) -> DCNotificationRow:
    notification_id = notification_id or get_notification_id(conn=conn)
    subject = subject or ""

    messages = messages or []
    messages_json = json_dumps([msg.model_dump() for msg in messages])

    stmt = text(
        """
    INSERT INTO dc_wf_notification (
        NOTIFICATION_ID,
        DC_USER_ID,
        DC_ENGAGEMENT_ID,
        DATA,
        SUBJECT,
        CREATE_DTM,
        CREATED_BY,
        NOTIFICATION_CATEGORY,
        IS_DELETED,
        REQUEST_ID,
        TREE_ID
    ) VALUES (
        :notification_id,
        :dc_user_id,
        :dc_engagement_id,
        :messages_json,
        :subject,
        SYSDATE(),
        :cisco_cco_id,
        :notification_category,
        'F',
        :request_id,
        :tree_id
    )
    """
    ).bindparams(
        notification_id=notification_id,
        dc_user_id=dc_user_id,
        dc_engagement_id=dc_engagement_id,
        messages_json=messages_json,
        subject=subject,
        cisco_cco_id=cisco_cco_id,
        notification_category=str(MessageStatus.pending),
        request_id=request_id,
        tree_id=tree_id,
    )

    conn.execute(stmt)

    return DCNotificationRow(
        dc_user_id=dc_user_id,
        cisco_cco_id=cisco_cco_id,
        dc_engagement_id=dc_engagement_id,
        messages=messages,
        subject=subject,
        notification_category=MessageStatus.pending,
        request_id=request_id,
        notification_id=notification_id,
        tree_id=tree_id,
    )
