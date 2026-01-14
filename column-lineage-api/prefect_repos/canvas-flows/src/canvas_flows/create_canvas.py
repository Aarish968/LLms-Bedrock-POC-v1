from common_canvas_next.models import CanvasStatus, EmptyScopeError, TEmptyScopePolicy
from common_canvas_next.tasks.create_canvas import create_canvas
from common_canvas_next.tasks.subtasks import update_canvas_status
from common_prefect_next.blocks.data_canvas import get_notification_block
from common_prefect_next.blocks.database import TWarehouse, Warehouse
from common_prefect_next.blocks.environment import Env, TEnv
from common_prefect_next.events.canvas import (
    emit_canvas_create_failure,
    emit_canvas_create_started,
    emit_canvas_create_success,
)
from common_prefect_next.logging.models.messages import MessageStatus
from common_prefect_next.utils import log_versions, setup_extra_loggers
from prefect import flow, get_run_logger

from canvas_flows.common import CreateCanvasPayload, Settings


@flow()
@setup_extra_loggers
@log_versions({"common_prefect_next", "common_canvas_next"})
def create_canvas_flow(
    payload: CreateCanvasPayload,
    env: TEnv | Env,
    warehouse: TWarehouse | Warehouse,
    empty_scope_policy: TEmptyScopePolicy,
) -> None:
    logger = get_run_logger()
    settings = Settings(env=env, warehouse=warehouse)
    get_engine = settings.get_engine
    emit_params = {
        "env": env,
        "canvas_id": payload.canvas_id,
        "notification_id": payload.notification_id,
        "dc_user_id": payload.dc_user_id,
        "dc_engagement_id": payload.dc_engagement_id,
        "request_id": payload.request_id,
    }
    emit_canvas_create_started(**emit_params)
    notify_block = get_notification_block(env=env)
    notify_block.notification_id = payload.notification_id
    notify_block.send_text("Starting canvas creation")

    engine = get_engine()
    with engine.begin() as conn:
        update_canvas_status(
            conn=conn, canvas_id=payload.canvas_id, status=CanvasStatus.running
        )

    try:
        create_canvas(
            payload=payload,
            get_engine=get_engine,
            empty_scope_policy=empty_scope_policy,
            env=env,
            clean_up=not settings.debug,
        )
        # Emit here so thoughtspot flow can pick up the event and run
        emit_canvas_create_success(**emit_params)
        notify_block.send_text(
            "Canvas creation complete, scheduling the liveboard imports"
        )
        with engine.begin() as conn:
            update_canvas_status(
                conn=conn, canvas_id=payload.canvas_id, status=CanvasStatus.success
            )
    except EmptyScopeError:
        emit_canvas_create_failure(**emit_params)
        logger.exception("Canvas creation failed due to empty scope")
        notify_block.send_text(
            "The Canvas creation failed because the scope was empty. Please review your sources and tags.",
            status=MessageStatus.error,
        )
        with engine.begin() as conn:
            update_canvas_status(
                conn=conn, canvas_id=payload.canvas_id, status=CanvasStatus.stopped
            )
        return None
    except Exception as e:
        emit_canvas_create_failure(**emit_params)
        logger.exception("Canvas creation failed")
        notify_block.send_exception("Canvas creation failed", exception=e)
        with engine.begin() as conn:
            update_canvas_status(
                conn=conn, canvas_id=payload.canvas_id, status=CanvasStatus.stopped
            )
        raise
