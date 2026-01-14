from common_prefect_next.blocks.database import Warehouse
from common_prefect_next.blocks.environment import Env, TEnv
from common_prefect_next.events.thoughtspot_liveboards import (
    emit_manage_liveboards_started,
)
from common_prefect_next.utils import (
    log_versions,
    setup_extra_loggers,
    track_wf_background_job,
)
from dc_canvas_service.services.action import ActionService
from prefect import flow, get_run_logger

from thoughtspot_flows.common import (
    ErrorHandler,
    Settings,
    get_cisco_cco_id,
    name_flow_run,
    setup_notification_block,
)


@flow(flow_run_name=name_flow_run)
@log_versions({"dc_canvas_service", "common_prefect_next"})
@setup_extra_loggers
def handle_liveboard_manage_request(
    env: TEnv | Env,
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int,
) -> None:
    """
    Handle Liveboard Management requests done outside of canvas creation
    """
    logger = get_run_logger()
    settings = Settings(env=env, warehouse=Warehouse.small)
    get_engine = settings.get_engine
    notify_block = setup_notification_block(env=env, notification_id=notification_id)

    emit_params = {
        "env": env,
        "canvas_id": canvas_id,
        "notification_id": notification_id,
        "dc_user_id": dc_user_id,
        "dc_engagement_id": dc_engagement_id,
        "request_id": request_id,
    }
    emit_manage_liveboards_started(**emit_params)
    engine = get_engine()

    with engine.begin() as conn:
        track_wf_background_job(conn=conn)

    with ErrorHandler(
        error_message="Failed to get Cisco CCO ID", notification_block=notify_block
    ):
        cisco_cco_id = get_cisco_cco_id(engine=engine, dc_user_id=dc_user_id)

    with ErrorHandler(
        error_message="Failed to create ActionService", notification_block=notify_block
    ):
        action_service = ActionService(
            canvas_id=canvas_id,
            engagement_id=dc_engagement_id,
            user_cisco_cco_id=cisco_cco_id,
            env=env,
            get_engine=get_engine,
        )

    with ErrorHandler(
        error_message="Failed to retrieve request", notification_block=notify_block
    ):
        request_items = action_service.get_pending_actions(request_id=request_id)
        if not request_items:
            msg = f"Request {request_id} not found"
            raise ValueError(msg)

    request = request_items[0]
    with ErrorHandler(
        error_message=f"Failed to process request {request_id=}",
        notification_block=notify_block,
    ):
        action_service.handle_request(request_id=request.request_id)

    msg = f"Finished processing request {request.request_id}"
    notify_block.mark_successful(message=msg)
    logger.info(msg)
