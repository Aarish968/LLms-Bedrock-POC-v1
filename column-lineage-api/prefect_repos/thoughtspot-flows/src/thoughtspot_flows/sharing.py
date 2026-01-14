from common_prefect_next.blocks.database import Warehouse
from common_prefect_next.blocks.environment import Env, TEnv
from common_prefect_next.events.canvas import emit_canvas_share_success
from common_prefect_next.events.engagement import emit_engagement_share_started
from common_prefect_next.utils import (
    log_versions,
    setup_extra_loggers,
    track_wf_background_job,
)
from dc_canvas_service.services.action import ActionService
from prefect import flow

from thoughtspot_flows.common import (
    ErrorHandler,
    Settings,
    get_cisco_cco_id,
    name_flow_run,
    setup_notification_block,
)
from thoughtspot_flows.common.queries import get_engagement_canvases


@flow(flow_run_name=name_flow_run)
@log_versions({"dc_canvas_service", "common_prefect_next"})
@setup_extra_loggers
def handle_engagement_shared(
    env: TEnv | Env,
    dc_engagement_id: int,
    notification_id: int,
    dc_user_id: int,
    shared_with_dc_user_id: int,
    request_id: int,
) -> None:
    """
    Handle engagement sharing. When this occurs we need to share all canvas liveboards with the shared user
    """
    settings = Settings(env=env, warehouse=Warehouse.small)
    get_engine = settings.get_engine
    notify_block = setup_notification_block(env=env, notification_id=notification_id)

    emit_params = {
        "env": env,
        "notification_id": notification_id,
        "dc_user_id": dc_user_id,
        "dc_engagement_id": dc_engagement_id,
        "request_id": request_id,
        "shared_with_dc_user_id": shared_with_dc_user_id,
    }
    emit_engagement_share_started(**emit_params)
    engine = get_engine()

    with engine.begin() as conn:
        track_wf_background_job(conn=conn)

    with ErrorHandler(
        error_message="Failed to get Cisco CCO ID", notification_block=notify_block
    ):
        cisco_cco_id = get_cisco_cco_id(engine=engine, dc_user_id=dc_user_id)
    with ErrorHandler(
        error_message="Failed to get shared user Cisco CCO ID",
        notification_block=notify_block,
    ):
        shared_cisco_cco_id = get_cisco_cco_id(
            engine=engine, dc_user_id=shared_with_dc_user_id
        )
    with ErrorHandler(
        error_message=f"Failed to retrieve canvases related to {dc_engagement_id=}",
        notification_block=notify_block,
    ):
        canvas_ids = get_engagement_canvases(
            engine=engine, dc_engagement_id=dc_engagement_id
        )
    if not canvas_ids:
        notify_block.mark_successful(message="No canvases found - nothing to share")
        return
    for canvas_id in canvas_ids:
        with ErrorHandler(
            error_message=f"Failed to share canvas {canvas_id=}",
            notification_block=notify_block,
        ):
            action_service = ActionService(
                canvas_id=canvas_id,
                engagement_id=dc_engagement_id,
                user_cisco_cco_id=cisco_cco_id,
                env=env,
                get_engine=get_engine,
            )
            action_service.share_canvas(users=[shared_cisco_cco_id])
            emit_canvas_share_success(**emit_params, canvas_id=canvas_id)
            notify_block.send_text(
                f"Shared canvas {canvas_id=} with {shared_with_dc_user_id=} {shared_cisco_cco_id=}"
            )
    notify_block.mark_successful(message="Shared all canvases with the user")
