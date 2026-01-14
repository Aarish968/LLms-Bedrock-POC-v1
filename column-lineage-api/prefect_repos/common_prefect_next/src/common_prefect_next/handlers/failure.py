import logging
from operator import getitem
from typing import TYPE_CHECKING, Any

from prefect import Flow, State, get_run_logger

from common_prefect_next.blocks.environment import Env, TEnv

if TYPE_CHECKING:
    from prefect.client.schemas.objects import FlowRun


logger = logging.getLogger(__name__)


async def handle_failure(
    flow: "Flow", flow_run: "FlowRun", flow_state: "State"
) -> None:
    """
    Handle failure of the flow by sending a notification.

    This function should be passed to the `on_failure` parameter of a Prefect flow.

    Example usage:
    @flow(on_failure=[handle_failure])
    ...
    """

    env: TEnv | None = _get_value_from_parameters("env", flow_run.parameters)
    notification_id: int | None = _get_value_from_parameters(
        "notification_id", flow_run.parameters
    )

    prefect_logger = get_run_logger()

    if not all((env, notification_id)):
        logger.error(
            "Missing parameters for notification - unable to send failure message."
        )
        prefect_logger.error(
            "Missing parameters for notification - unable to send failure message."
        )
        return
    try:
        notification_id = int(notification_id)
    except:
        prefect_logger.warning(
            "Found non-integer notification_id: %s - unable to send failure message",
        )
        logger.warning(
            "Found non-integer notification_id: %s - unable to send failure message",
            notification_id,
        )
        return
    try:
        env = Env(env).value
    except ValueError:
        logger.warning("Invalid environment value: %s - falling back to 'dev'", env)
        prefect_logger.warning(
            "Invalid environment value: %s - falling back to 'dev'", env
        )
        env = Env.dev.value

    env: TEnv

    logger.info(f"Handling failure for flow {flow.name} with state: {flow_state}")
    await _notify_failure(
        flow=flow,
        flow_run=flow_run,
        flow_state=flow_state,
        notification_id=notification_id,
        env=env,
    )


def _get_field(field_name: str, obj: Any) -> Any | None:
    try:
        val_attr = getattr(obj, field_name)

    except:
        ...
    else:
        return val_attr
    try:
        val_item = getitem(obj, field_name)
    except:
        ...
    else:
        return val_item
    return None


def _get_value_from_parameters(
    field_name: str, parameters: dict[str, Any]
) -> Any | None:
    if field_name in parameters:
        return parameters[field_name]
    for v in parameters.values():
        found = _get_field(field_name, v)
        if found:
            return found
    return None


async def _notify_failure(
    flow: "Flow",
    flow_run: "FlowRun",
    flow_state: "State",
    notification_id: int,
    env: TEnv,
) -> None:
    from common_prefect_next.blocks.data_canvas import get_notification_block
    from common_prefect_next.logging.models.messages import MessageStatus

    prefect_logger = get_run_logger()

    notify_block = get_notification_block(env=env)
    notify_block.notification_id = notification_id
    user_msg = f"Flow {flow.name} Run: {flow_run.name} failed with state: {flow_state}"

    logger.info("Sending failure notification for notification ID: %s")
    prefect_logger.info(
        "Sending failure notification for notification ID: %s", notification_id
    )

    notify_block.send_text(
        user_msg,
        status=MessageStatus.error,
    )

    logger.info("Failure notification sent successfully.")
    prefect_logger.info("Failure notification sent successfully.")
