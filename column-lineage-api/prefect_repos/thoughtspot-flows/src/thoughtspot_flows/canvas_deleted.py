from common_prefect_next.blocks.database import Warehouse
from common_prefect_next.blocks.environment import Env, TEnv
from common_prefect_next.events.canvas import (
    emit_canvas_delete_started,
    emit_canvas_delete_success,
)
from common_prefect_next.utils import (
    log_versions,
    setup_extra_loggers,
    track_wf_background_job,
)
from dc_canvas_service.services.canvas import CanvasService
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
def handle_canvas_deleted(
    env: TEnv | Env,
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
) -> None:
    """
    This flow is triggered via prefect cloud in the Automations. Once a canvas is
    deleted we need to clean up any associated data in TS
    """
    logger = get_run_logger()
    settings = Settings(env=env, warehouse=Warehouse.small)
    get_engine = settings.get_engine
    emit_params = {
        "env": env,
        "canvas_id": canvas_id,
        "notification_id": notification_id,
        "dc_user_id": dc_user_id,
        "dc_engagement_id": dc_engagement_id,
        "request_id": None,
    }
    emit_canvas_delete_started(**emit_params)
    notify_block = setup_notification_block(env=env, notification_id=notification_id)
    notify_block.send_text("Cleaning up ThoughtSpot data for deleted canvas")

    engine = get_engine()

    with engine.begin() as conn:
        track_wf_background_job(conn=conn)

    with ErrorHandler(
        error_message="Failed to get Cisco CCO ID", notification_block=notify_block
    ):
        cisco_cco_id = get_cisco_cco_id(engine=engine, dc_user_id=dc_user_id)

    with ErrorHandler(
        error_message="Failed to create CanvasService", notification_block=notify_block
    ):
        canvas_service = CanvasService(
            canvas_id=canvas_id,
            engagement_id=dc_engagement_id,
            user_cisco_cco_id=cisco_cco_id,
            env=env,
            get_engine=get_engine,
        )

    with ErrorHandler(
        error_message="Failed to clean TS data", notification_block=notify_block
    ):
        canvas_service.clean_ts()

    msg = f"Cleaned up ThoughtSpot data for deleted {canvas_id=}"
    notify_block.mark_successful(message=msg)
    logger.info(msg)
    emit_canvas_delete_success(**emit_params)
