from common_prefect_next.blocks.database import Warehouse
from common_prefect_next.blocks.environment import Env, TEnv
from common_prefect_next.events.thoughtspot_liveboards import (
    emit_discover_liveboards_started,
)
from common_prefect_next.utils import log_versions, setup_extra_loggers
from dc_canvas_service.services.sync import SyncService
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
def handle_canvas_discover(
    env: TEnv | Env,
    canvas_id: int,
    notification_id: int,
    dc_user_id: int,
    dc_engagement_id: int,
    request_id: int,
) -> None:
    """
    This flow is akin to reading state in the ThoughtSpot liveboards and updating the state
    in the database. Users invoke this manually to update the state of the liveboards.

    """

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

    emit_discover_liveboards_started(**emit_params)
    notify_block.send_text("Discovering Liveboards")

    engine = get_engine()
    with ErrorHandler(
        error_message="Failed to get Cisco CCO ID", notification_block=notify_block
    ):
        cisco_cco_id = get_cisco_cco_id(engine=engine, dc_user_id=dc_user_id)

    with ErrorHandler(
        error_message="Failed to create SyncService", notification_block=notify_block
    ):
        sync_service = SyncService(
            env=env, requested_user=cisco_cco_id, get_engine=get_engine
        )
        sync_service.sync(canvas_id=canvas_id)

    notify_block.mark_successful(message="Liveboards discovered successfully")
