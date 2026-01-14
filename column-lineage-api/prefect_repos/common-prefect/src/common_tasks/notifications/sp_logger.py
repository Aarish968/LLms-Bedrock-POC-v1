import json
from typing import TYPE_CHECKING, Literal, Optional, TypedDict, Union

import prefect
from sqlalchemy import create_engine, text

from common_tasks.settings import TEnv, Warehouse

if TYPE_CHECKING:
    from prefect.engine.state import State
    from sqlalchemy.engine import Engine

TStatus = Literal['result', 'error', 'pending']
SP_CONTEXT_KEY = "DC_SP_CONTEXT"
SP_LOGGER_CONTEXT_KEY = "sp_logger"
SP_NAME = "add_messages_to_notification"

class SpNotificationContext(TypedDict):
    env: Literal['dev', 'prod']
    notification_id: int
    
    
class SpNotificationHandler:
    """When the API is unreachable, fallback to the Stored Procedure"""
    
    def __init__(self, notification_id: int, env: TEnv):
        self.notification_id = notification_id
        self.env = env
        self.engine = self._create_engine(env)
        
    @classmethod
    def _create_engine(cls, env: TEnv) -> 'Engine':
        from common_tasks.settings import SettingsBase
        base_settings = SettingsBase(env=env, warehouse=Warehouse.x_small)

        db_url = base_settings.get_db_url()
        session_parameters = {"abort_detached_query": True}
        return create_engine(
            db_url,
            connect_args={
                "log_max_query_length": 10_000,
                "session_parameters": session_parameters,
            },
        )
    
    def _make_payload(self, messages: Optional[list], notification_category: Optional[TStatus]):
        payload = {
            "notification_id": self.notification_id
        }
        if notification_category is not None:
            payload['notification_category'] = notification_category
        if messages is not None:
            payload['messages'] = messages
        return payload
    
    def _send_payload(self, payload: dict):
        stmt = text("CALL IDENTIFIER(:proc_name)(:payload)").bindparams(
            proc_name=SP_NAME,
            payload=json.dumps(payload, separators=(',', ':'))
        )
        
        with self.engine.begin() as conn:
            try:
                conn.execute(stmt)
            except Exception as e:
                print(f"Error sending log to API: {e!s}")
        
    @staticmethod
    def get_notification_context(new_state: "State") -> "SpNotificationContext":
        """
        In order to function, SpNotificationHandler requires the following context keys / parameters:
        - env: Literal['dev', 'prod']
        - notification_id: int
        """
        
        if prefect.context.get(SP_CONTEXT_KEY) is not None:
            # Already initialized
            return prefect.context.get(SP_CONTEXT_KEY).copy()
        
        logger = prefect.context.get("logger")
        # Flow parameters are stored here
        parameters = prefect.context.get("parameters")
        # This is only used for local testing
        env = parameters.get("env") or prefect.context.get("env") or parameters.get('sf_env') or prefect.context.get('sf_env')
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
        
        
        sp_context = {
            "env": env,
            "notification_id": notification_id,
        }
        prefect.context.update({SP_CONTEXT_KEY: sp_context})
        return sp_context
    
    @classmethod
    def from_context(cls, context: "SpNotificationContext"):
        return cls(
            notification_id=context["notification_id"],
            env=context["env"]
        )
        
        
        
    
    def send_text(self, message: str, *, status: Optional[TStatus] = None):
        payload = self._make_payload(
            messages=[{"type": "text", "data": message}],
            notification_category=status
        )
        return self._send_payload(payload)
    
    def info(self, message: str, *, status: Optional[TStatus] = None):
        """Alias for send_text"""
        return self.send_text(message, status=status)
    
    def send_download_link(self, url: str, *, label: Optional[str]=None, status: Optional[TStatus] = "result"):
        msg= {
            "type": "download",
            "data": {"url": url, "label": label if label is not None else "Download Results"},
        }
        payload = self._make_payload(
            messages=[msg],
            notification_category=status
        )
        return self._send_payload(payload)
    
    def send_table(self, table: dict[str, Union[str, int]], *, status: Optional[TStatus] = None):
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






