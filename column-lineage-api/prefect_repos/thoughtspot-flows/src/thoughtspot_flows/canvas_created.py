from common_prefect_next.blocks.database import Warehouse
from common_prefect_next.blocks.environment import Env, TEnv
from common_prefect_next.utils import log_versions, setup_extra_loggers
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
def handle_canvas_created(
    env: TEnv | Env,
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
) -> None:
    """This flow is triggered via prefect cloud in the Automations"""
    logger = get_run_logger()
    settings = Settings(env=env, warehouse=Warehouse.small)
    get_engine = settings.get_engine
    notify_block = setup_notification_block(env=env, notification_id=notification_id)
    notify_block.send_text("Looking for pending liveboard management tasks")

    engine = get_engine()
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
        error_message="Failed to get pending actions", notification_block=notify_block
    ):
        pending_results = action_service.get_pending_actions()

    if not pending_results:
        msg = "No pending actions found"
        notify_block.mark_successful(message=msg)
        logger.info(msg)
        return

    msg = f"Found {len(pending_results)} pending requests"
    notify_block.send_text(msg)
    logger.info(msg)
    for request in pending_results:
        with ErrorHandler(
            error_message=f"Failed to process pending request {request.request_id}",
            notification_block=notify_block,
        ):
            action_service.handle_request(request_id=request.request_id)
            msg = f"Finished processing request {request.request_id}"
            notify_block.send_text(msg)
            logger.info(msg)

    notify_block.mark_successful(message="Processed all pending requests")
