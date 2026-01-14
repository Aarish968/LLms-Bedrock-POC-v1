from common_prefect_next.blocks.database import Warehouse
from common_prefect_next.blocks.environment import Env, TEnv
from common_prefect_next.utils import log_versions, setup_extra_loggers
from dc_canvas_service.services.canvas import CanvasService
from prefect import flow

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
def handle_canvas_rebuild(
    env: TEnv | Env,
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
) -> None:
    """This flow is triggered via prefect cloud in the Automations"""
    settings = Settings(env=env, warehouse=Warehouse.small)
    get_engine = settings.get_engine
    notify_block = setup_notification_block(env=env, notification_id=notification_id)
    notify_block.send_text("Refreshing the ThoughtSpot data source")

    engine = get_engine()
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
        error_message="Failed to refresh ThoughtSpot data source",
        notification_block=notify_block,
    ):
        canvas_service.refresh_ts_datasource()

    notify_block.mark_successful(
        message="Finished refreshing the ThoughtSpot data source"
    )
