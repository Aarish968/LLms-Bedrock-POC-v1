import warnings
from typing import TYPE_CHECKING, Literal, Optional, TypedDict, Union
from urllib.parse import urlencode, urlparse, urlunparse

import prefect
import requests

if TYPE_CHECKING:
    from prefect.engine.state import State
    
Status = Literal['result', 'error']

AUTH_CONTEXT_KEY = "DC_AUTH_CONTEXT"
API_LOGGER_CONTEXT_KEY = "api_logger"
DEV_ORIGIN = "devdatacanvaswf.cisco.com"
PROD_ORIGIN = "datacanvaswf.cisco.com"
MESSAGE_ENDPOINT = "/api/v2/workflows/notifications/message/{notification_id:}"


class NotificationContext(TypedDict):
    env: Literal['dev', 'prod']
    notification_id: int
    logged_user: Optional[str]
    auth_token: str
    api_url: str

class ApiNotificationHandler:

    def __init__(self, auth_token: str, api_url: str, notification_id: int):
        self.auth_token = auth_token
        self.api_url = api_url
        self.notification_id = notification_id
        
    def _make_payload(self, messages: Optional[list], notification_category: Optional[str]):
        payload = {}
        if notification_category is not None:
            payload['notification_category'] = notification_category
        if messages is not None:
            payload['data'] = messages
        return payload
    
    def _send_payload(self, payload: dict):
        session = requests.Session()
        session.headers.update(
            {
                "Authorization": f"Bearer {self.auth_token}",
                "User-Agent": "NotificationHandler",
            }
        )
        
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                response = session.patch(
                    self.api_url, json=payload,
                    verify=False
                )
                response.raise_for_status()
                return response
        except Exception as e:
            print(f"Error sending log to API: {e!s}")
            
    @staticmethod
    def get_token_from_prefect_state(new_state: "State") -> Optional[str]:
        # Check if there was a task with a tag 'token' that has already run
        if new_state.result is None:
            return None
        task_keys = list(new_state.result.keys())
        
        tagged_tasks = next(
            (task for task in task_keys if 'token' in task.tags), None
        )
        if tagged_tasks is None:
            return None
        
        token_result = new_state.result[tagged_tasks]
        if token_result.is_successful():
            return token_result.result
        return None
    
    @staticmethod
    def get_notification_context(new_state: "State") -> "NotificationContext":
        """
        In order to function, ApiNotificationHandler requires the following context keys / parameters:
        
        - env: Literal['dev', 'prod']
        - notification_id: int
        
        Optional:
        - logged_user: str (added to the query string)
        - dc_uri: str (Used to override the default origin, mainly for testing)
        - auth_token: str (If already available, it will be used, otherwise it will be fetched)
        """
        
        if prefect.context.get(AUTH_CONTEXT_KEY) is not None:
            # Already initialized
            return prefect.context.get(AUTH_CONTEXT_KEY).copy()
        
        logger = prefect.context.get("logger")
        # Flow parameters are stored here
        parameters = prefect.context.get("parameters")
        # This is only used for local testing
        dc_uri = prefect.context.get("dc_uri")
        env = parameters.get("env") or prefect.context.get("env")
        if env is None:
            raise ValueError("'env' parameter or context is required.")
        if env not in {'dev', 'prod'}:
            raise ValueError(
                f"'env' parameter or context must be either 'dev' or 'prod'. Got {env}"
            )
        
        notification_id = parameters.get("notification_id") or prefect.context.get("notification_id")
        if notification_id is None:
            logger.error(
                "'notification_id' parameter or context is missing."
            )
            raise ValueError("notification_id parameter is required.")
        
        if notification_id == 0:
            logger.warning(
                "notification_id is 0, this is likely a test notification."
            )
            raise ValueError("notification_id is 0")
        
        logged_user = (
            parameters.get("logged_user") or prefect.context.get("logged_user")
            or parameters.get("requested_by") or prefect.context.get("requested_by")
        )
        
        netloc = (
            urlparse(
                dc_uri
            ).netloc if dc_uri is not None else PROD_ORIGIN if env == "prod" else DEV_ORIGIN)
        
        scheme = "http" if 'localhost' in netloc else "https"
        path = MESSAGE_ENDPOINT.format(notification_id=notification_id)
        query = urlencode({"logged_user": logged_user}) if logged_user else None
        
        # noinspection PyTypeChecker
        api_url: str = urlunparse((scheme, netloc, path, "", query, ""))
        
        from .auth import get_access_token
        token = ApiNotificationHandler.get_token_from_prefect_state(new_state)
        if not token:
            try:
                token = get_access_token(env)
            except Exception as e:
                logger.error(
                    f"Failed to get token from AWS Cognito, cannot notify on failure: {e}"
                )
                raise e
        if token is None:
            logger.error("Failed to get token from AWS Cognito, cannot notify on failure.")
            raise ValueError("Failed to get token from AWS")
        
        auth_context = {
            "env": env,
            "notification_id": notification_id,
            "logged_user": logged_user,
            "auth_token": token,
            "api_url": api_url,
        }
        prefect.context.update({AUTH_CONTEXT_KEY: auth_context})
        return auth_context
    
    @classmethod
    def from_context(cls, context: "NotificationContext"):
        return cls(
            auth_token=context["auth_token"],
            api_url=context["api_url"],
            notification_id=context["notification_id"]
        )
            
    def send_text(self, message: str, *, status: Optional[Status] = None):
        payload = self._make_payload(
            messages=[{"type": "text", "data": message}],
            notification_category=status
        )
        return self._send_payload(payload)
    
    def info(self, message: str, *, status: Optional[Status] = None):
        """Alias for send_text"""
        return self.send_text(message, status=status)
    
    def send_download_link(self, url: str, *, label: Optional[str]=None, status: Optional[Status] = "result"):
        msg= {
            "type": "download",
            "data": {"url": url, "label": label if label is not None else "Download Results"},
        }
        payload = self._make_payload(
            messages=[msg],
            notification_category=status
        )
        return self._send_payload(payload)
    
    def send_table(self, table: dict[str, Union[str, int]], *, status: Optional[Status] = None):
        msg = {
            "type": "table",
            "data": table
        }
        payload = self._make_payload(
            messages=[msg],
            notification_category=status
        )
        return self._send_payload(payload)
    
    def send_exception(self, message: Optional[str], *, exception: Optional[Exception]=None):
        e_body = str(exception) if exception is not None else ""
        text = f"{message}: {e_body}" if message is not None else f"An exception occurred : {e_body}" if e_body else "An exception occurred."
        
        payload = self._make_payload(
            messages=[{"type": "exception", "data": text}],
            notification_category="error"
        )
        return self._send_payload(payload)
    
    def mark_successful(self, *, message: Optional[str] = None):
        msg = [{"type": "text", "data": message}] if message is not None else None
        payload = self._make_payload(
            messages=msg,
            notification_category="result"
        )
        return self._send_payload(payload)
    
    def mark_error(self, *, messages: Optional[list]=None):
        payload = self._make_payload(
            messages=messages,
            notification_category="error"
        )
        return self._send_payload(payload)






