from typing import TYPE_CHECKING

import prefect
from prefect.engine.state import TriggerFailed

from .api_logger import API_LOGGER_CONTEXT_KEY, ApiNotificationHandler
from .null_logger import NullNotificationHandler
from .sp_logger import SP_LOGGER_CONTEXT_KEY, SpNotificationHandler

if TYPE_CHECKING:
    from prefect.engine.state import State


def inject_api_logger(_obj, _old_state: "State", new_state: "State"):
    """
    Add ApiNotificationHandler to communicate with API into prefect.context.
    
    This modifies the flow context to include an instance of ApiNotificationHandler. If
    the handler cannot be created, the NullNotificationHandler will be injected to prevent
    errors.
    
    See get_notification_context for the required context keys / parameters

    """
    
    
    if prefect.context.get(API_LOGGER_CONTEXT_KEY) is not None:
        # Already injected
        return new_state
    
    prefect_logger = prefect.context.get("logger")
    prefect_logger.info("Adding ApiNotificationHandler to flow context")
    try:
        auth_context = ApiNotificationHandler.get_notification_context(new_state)
    except Exception as e:
        prefect_logger.error(f"Failed to get notification context: {e!s}. Notifications will not be sent. Injecting NullNotificationHandler")
        null_logger = NullNotificationHandler()
        prefect.context.update({"api_logger": null_logger})
        return new_state
    
    api_logger = ApiNotificationHandler.from_context(auth_context)
    prefect.context.update({"api_logger": api_logger})
    return new_state

def notify_api_on_failure(_obj, _old_state: "State", new_state: "State"):
    """
    If any tasks failed, notify the API with the error message.
    """
    
    def make_error_message(exception: Exception):
        return {"type": "text", "data": f"An Error Occurred: \n{exception!s}"}
    
    
    # Look for failed tasks and generate messages
    if new_state.result is None:
        return new_state
    task_keys = list(new_state.result.keys())
    all_failed_results = [new_state.result[key] for key in task_keys if
                          new_state.result[key].is_failed()]
    # Remove trigger failed states (these just mean the task was skipped due to a failure upstream)
    
    failed_results = [result for result in all_failed_results if
                      not isinstance(result, TriggerFailed)]
    
    if not failed_results:
        return new_state
    
    

    messages = [make_error_message(result.result) for result in failed_results]
    
    api_logger = prefect.context.get(API_LOGGER_CONTEXT_KEY)
    prefect_logger = prefect.context.get("logger")
    if api_logger is None:
        prefect_logger.info("No API Logger found in context - attempting to inject")
        try:
            auth_context = ApiNotificationHandler.get_notification_context(new_state)
            api_logger = ApiNotificationHandler.from_context(auth_context)
        except Exception as e:
            prefect_logger.error(f"Failed to get notification context: {e!s} - cannot notify API")
            return new_state
    
    api_logger.mark_error(messages=messages)
    prefect_logger.info(f"Updated notification_id={api_logger.notification_id} as error state")
    return new_state
    
def inject_sp_logger(_obj, _old_state: "State", new_state: "State"):
    if prefect.context.get(SP_LOGGER_CONTEXT_KEY) is not None:
        # Already injected
        return new_state
    
    prefect_logger = prefect.context.get("logger")
    prefect_logger.info("Adding SpNotificationHandler to flow context")
    try:
        auth_context = SpNotificationHandler.get_notification_context(new_state)
    except Exception as e:
        prefect_logger.error(
            f"Failed to get notification context: {e!s}. Notifications will not be sent. Injecting NullNotificationHandler"
            )
        null_logger = NullNotificationHandler()
        prefect.context.update({"sp_logger": null_logger})
        return new_state
    
    sp_logger = SpNotificationHandler.from_context(auth_context)
    prefect.context.update({"sp_logger": sp_logger})
    return new_state


def notify_sp_on_failure(_obj, _old_state: "State", new_state: "State"):
    """
    If any tasks failed, notify the API with the error message.
    """
    
    def make_error_message(exception: Exception):
        return {"type": "text", "data": f"An Error Occurred: \n{exception!s}"}
    
    # Look for failed tasks and generate messages
    if new_state.result is None:
        return new_state
    task_keys = list(new_state.result.keys())
    all_failed_results = [new_state.result[key] for key in task_keys if
                          new_state.result[key].is_failed()]
    # Remove trigger failed states (these just mean the task was skipped due to a failure upstream)
    
    failed_results = [result for result in all_failed_results if
                      not isinstance(result, TriggerFailed)]
    
    if not failed_results:
        return new_state
    
    messages = [make_error_message(result.result) for result in failed_results]
    
    sp_logger = prefect.context.get(SP_LOGGER_CONTEXT_KEY)
    prefect_logger = prefect.context.get("logger")
    if sp_logger is None:
        prefect_logger.info("No SP Logger found in context - attempting to inject")
        try:
            auth_context = SpNotificationHandler.get_notification_context(new_state)
            sp_logger = SpNotificationHandler.from_context(auth_context)
        except Exception as e:
            prefect_logger.error(
                f"Failed to get notification context: {e!s} - cannot notify SP"
                )
            return new_state
    
    sp_logger.mark_error(messages=messages)
    prefect_logger.info(
        f"Updated notification_id={sp_logger.notification_id} as error state"
        )
    return new_state
