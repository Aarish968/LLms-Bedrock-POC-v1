from common_prefect_next.blocks.database import Warehouse
from common_prefect_next.blocks.environment import Env, TEnv
from common_prefect_next.utils import (
    log_versions,
    setup_extra_loggers,
)
from dc_canvas_service.services.canvas import CanvasService
from prefect import flow, get_run_logger

from thoughtspot_flows.common import (
    Settings,
    create_notification,
    get_canvas_action_tree_id,
    get_canvas_users,
    get_notification_id,
    get_request_id,
    name_flow_run
)


@flow(flow_run_name=name_flow_run)
@log_versions({"dc_canvas_service", "common_prefect_next"})
@setup_extra_loggers
def handle_canvas_cleanup(
    env: TEnv | Env,
    canvas_id: int,
) -> None:
    """
    This flow is triggered via prefect cloud in the Automations. This is nearly identical to the handle_canvas_deleted flow, but
    since it is triggered by a background process, and not user action,
    we can bypass the need for the notification_id, dc_user_id, and dc_engagement_id

    dc-canvas-service still requires the dc_engagement_id and cisco_cco_id to clean up the TS data
    so we will attempt to find the primary_user (original creator) of the canvas
    """
    logger = get_run_logger()
    settings = Settings(env=env, warehouse=Warehouse.small)
    get_engine = settings.get_engine
    engine = get_engine()

    with engine.begin() as conn:
        canvas_users = get_canvas_users(conn=conn, canvas_id=canvas_id)

    if not canvas_users:
        msg = f"Could not find any users for canvas {canvas_id=}"
        raise ValueError(msg)

    primary_user = next((user for user in canvas_users if user.is_owner), None)
    if primary_user is None:
        primary_user = canvas_users[0]
        msg = f"Could not find the primary_user of the canvas {canvas_id=}. Using the first user {primary_user.user_id=}"
        logger.info(msg)

    with engine.begin() as conn:
        tree_id = get_canvas_action_tree_id(conn=conn)
        notification_id = get_notification_id(conn=conn)
        request_id = get_request_id(conn=conn)
        create_notification(
            conn=conn,
            tree_id=tree_id,
            dc_user_id=primary_user.user_id,
            dc_engagement_id=primary_user.dc_engagement_id,
            request_id=request_id,
            notification_id=notification_id,
            subject="Canvas Cleanup",
            message=f"We have cleaned up a canvas you created, Canvas ID: {canvas_id}",
            created_by="handle_canvas_cleanup",
            status="result",
        )

    canvas_service = CanvasService(
        canvas_id=canvas_id,
        engagement_id=primary_user.dc_engagement_id,
        user_cisco_cco_id=primary_user.cisco_cco_id,
        env=env,
        get_engine=get_engine,
    )

    canvas_service.clean_ts()
