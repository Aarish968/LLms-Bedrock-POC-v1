from common_canvas_next.models import CanvasStatus
from common_canvas_next.tasks.refresh_canvas_view import refresh_canvas_view
from common_canvas_next.tasks.subtasks import update_canvas_status
from common_prefect_next.blocks.data_canvas import get_notification_block
from common_prefect_next.blocks.database import TWarehouse, Warehouse
from common_prefect_next.blocks.environment import Env, TEnv
from common_prefect_next.events.canvas import (
    emit_canvas_refresh_failure,
    emit_canvas_refresh_started,
    emit_canvas_refresh_success,
)
from common_prefect_next.utils import log_versions, setup_extra_loggers
from prefect import flow, get_run_logger

from canvas_flows.common import RefreshCanvasViewPayload, Settings


@flow()
@setup_extra_loggers
@log_versions({"common_prefect_next", "common_canvas_next"})
def refresh_canvas_view_flow(
    payload: RefreshCanvasViewPayload,
    env: TEnv | Env,
    warehouse: TWarehouse | Warehouse,
) -> None:
    """
    The Canvas View can +/- columns depending on Global and Engagement tags.
    This flow skips the entire Canvas creation process and just refreshes the view.
    """
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
    emit_canvas_refresh_started(**emit_params)
    notify_block = get_notification_block(env=env)
    notify_block.notification_id = payload.notification_id
    notify_block.send_text("Starting canvas view refresh")
    engine = get_engine()
    try:
        with engine.begin() as conn:
            update_canvas_status(
                conn=conn, canvas_id=payload.canvas_id, status=CanvasStatus.running
            )
        refresh_canvas_view(payload=payload, get_engine=get_engine)
        # Emit here so thoughtspot flow can pick up the event and run
        emit_canvas_refresh_success(**emit_params)
        with engine.begin() as conn:
            update_canvas_status(
                conn=conn, canvas_id=payload.canvas_id, status=CanvasStatus.success
            )
        notify_block.send_text("Canvas view refresh complete")
    except Exception as e:
        emit_canvas_refresh_failure(**emit_params)
        logger.exception("Canvas view refresh failed")
        notify_block.send_exception("Canvas view refresh failed", exception=e)
        with engine.begin() as conn:
            update_canvas_status(
                conn=conn, canvas_id=payload.canvas_id, status=CanvasStatus.stopped
            )
        raise
