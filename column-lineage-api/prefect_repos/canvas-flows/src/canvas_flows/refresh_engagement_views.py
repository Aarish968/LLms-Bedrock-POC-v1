from common_canvas_next.models import (
    EngagementRefreshViewsPayload,
)
from common_canvas_next.tasks.refresh_canvas_view import (
    refresh_canvas_views_for_engagement,
)
from common_prefect_next.blocks.data_canvas import get_notification_block
from common_prefect_next.blocks.database import TWarehouse, Warehouse
from common_prefect_next.blocks.environment import Env, TEnv
from common_prefect_next.events.engagement import (
    emit_engagement_refresh_failure,
    emit_engagement_refresh_started,
    emit_engagement_refresh_success,
)
from common_prefect_next.utils import log_versions, setup_extra_loggers
from prefect import flow, get_run_logger

from canvas_flows.common import RefreshEngagementViewsPayload, Settings


@flow()
@setup_extra_loggers
@log_versions({"common_prefect_next", "common_canvas_next"})
def refresh_engagement_views_flow(
    env: TEnv | Env,
    payload: RefreshEngagementViewsPayload,
    warehouse: TWarehouse | Warehouse = Warehouse.small,
) -> None:
    """
    The Canvas Views can +/- columns depending on Global and Engagement tags.
    After implementing static tag tables, we need to have a process to refresh all
    Canvas Views for a given Engagement, to refresh a list of active columns.
    This flow skips the entire Canvas creation process and just refreshes Canvas Views.
    """
    logger = get_run_logger()
    settings = Settings(env=env, warehouse=warehouse)
    get_engine = settings.get_engine
    emit_params = {
        "env": env,
        "notification_id": payload.notification_id,
        "dc_user_id": payload.dc_user_id,
        "dc_engagement_id": payload.dc_engagement_id,
        "request_id": payload.request_id,
    }
    emit_engagement_refresh_started(**emit_params)
    notify_block = get_notification_block(env=env)
    notify_block.notification_id = payload.notification_id
    notify_block.send_text(
        "A change in engagement tags has been detected - all canvas views will be refreshed"
    )
    task_payload = EngagementRefreshViewsPayload(
        dc_engagement_id=payload.dc_engagement_id
    )
    try:
        refresh_canvas_views_for_engagement(
            payload=task_payload, get_engine=get_engine, db_schema=settings.db_schema
        )
        emit_engagement_refresh_success(**emit_params)
        notify_block.mark_successful(message="Engagement views refresh complete")
    except Exception as e:
        emit_engagement_refresh_failure(**emit_params)
        logger.exception("Engagement views refresh failed")
        notify_block.send_exception("Engagement views refresh failed", exception=e)
        raise
