import datetime
from enum import Enum
from logging import Logger, getLogger
from typing import NamedTuple, Optional, Union

import httpx
from prefect.blocks.notifications import NotificationBlock
from pydantic import ConfigDict, Field, PrivateAttr
from typing_extensions import TypeAlias

from common_prefect_next.blocks.aws import AwsCognitoCredentials
from common_prefect_next.blocks.environment import Env, TEnv
from common_prefect_next.logging.models.base import isoformat_utc
from common_prefect_next.logging.models.messages import (
    DownloadData,
    DownloadMessage,
    MessageCreateModels,
    MessageModels,
    MessageStatus,
    MessageType,
    TableMessage,
    TextMessage,
    TextMessageCreate,
)
from common_prefect_next.utils import json_dumps

MessagePayloadItem: TypeAlias = Union[MessageModels, MessageCreateModels]
MessagePayload: TypeAlias = list[MessagePayloadItem]

logger = getLogger(__name__)


class DataCanvasEndpointData(NamedTuple):
    domain: str
    update_notification_status: str
    update_notification: str


class DataCanvasDomainEndpoints(str, Enum):
    dev = "devdatacanvaswf.cisco.com"
    prod = "datacanvaswf.cisco.com"

    def __str__(self) -> str:
        return str.__str__(self)


class DataCanvasEndpoints(str, Enum):
    update_notification = "api/v2/workflows/notifications/message/{notification_id}"
    update_status = "api/v2/workflows/notifications/{notification_id}/{status}"


class DataCanvasBlockNames(str, Enum):
    dev = "datacanvas_dev"
    prod = "datacanvas_prod"
    dev_notification = "datacanvas-dev-notification"
    prod_notification = "datacanvas-prod-notification"

    def __str__(self) -> str:
        return str.__str__(self)


class DataCanvasNotificationBlock(NotificationBlock):
    _description = "A block for sending log messages and changing status to Data Canvas Notification"
    _block_type_name = "Data Canvas Notification"
    _client: httpx.Client | None = PrivateAttr(None)
    _data_canvas_domain_data: DataCanvasEndpointData | None = PrivateAttr(None)

    model_config = ConfigDict(json_encoders={datetime.datetime: isoformat_utc})

    aws_cognito_credentials: AwsCognitoCredentials
    env: Env
    notification_id: Optional[int] = Field(
        None, description="The notification ID. Should be set at runtime"
    )

    def block_initialization(self) -> None:
        self._data_canvas_domain_data = get_notification_endpoints(self.env)
        api_key = self.aws_cognito_credentials.get_access_token()
        self._client = self._setup_network_client(api_key)

    def _get_client(self) -> httpx.Client | None:
        if self._client is None:
            self.logger.warning("Client is not setup, notification will not be sent")
            try:
                from prefect import get_run_logger

                prefect_logger = get_run_logger()
            except Exception:
                # If we're not in a Prefect context, we can't get the run logger
                return None
            else:
                prefect_logger.warning(
                    "DataCanvasNotificationBlock Client is not setup, notification will not be sent"
                )
            return None
        if self._data_canvas_domain_data is None:
            self.logger.warning(
                "Data Canvas Domain data is not setup, notification will not be sent"
            )
            return None
        if self.notification_id is None:
            self.logger.warning(
                "Notification ID is not set, notification will not be sent"
            )
            return None
        return self._client

    def _setup_network_client(self, api_key: str) -> httpx.Client:
        # Verify is set to False because the we're using a self-signed certificate
        self._client = httpx.Client(
            headers={
                "Authorization": f"Bearer {api_key}",
                "User-Agent": f"{self._block_type_name}",
                "Content-Type": "application/json",
            },
            verify=False,
            base_url=self._data_canvas_domain_data.domain,
            timeout=httpx.Timeout(connect=60, read=60, write=60, pool=60),
            follow_redirects=True,
            limits=httpx.Limits(
                max_connections=16, max_keepalive_connections=8, keepalive_expiry=25
            ),
        )
        return self._client

    def _make_payload(
        self, messages: list[dict] | None, notification_category: MessageStatus | None
    ) -> str:
        payload = {}
        if notification_category is not None:
            payload["notification_category"] = str(notification_category)
        if messages is not None:
            payload["data"] = messages
        return json_dumps(payload)

    def _send_payload(self, payload: str) -> httpx.Response | None:
        """Send the payload to the API. Payload should be a JSON string"""
        client = self._get_client()
        if client is None:
            self.logger.warning("Client is not setup, notification will not be sent")
            return None
        try:
            endpoint = self._data_canvas_domain_data.update_notification.format(
                notification_id=self.notification_id
            )
            response = client.patch(endpoint, content=payload)
            response.raise_for_status()
            self.logger.debug("Response from API: %s", response.text)
        except httpx.HTTPStatusError as e:
            self.logger.exception("Error sending log to API: %s", e.response.text)
            return None
        except Exception:
            self.logger.exception("Error sending log to API")
            return None
        else:
            self.logger.info(
                "Successfully sent message to Data Canvas Notification API"
            )
            return response

    def send_text(
        self,
        message: str | TextMessageCreate | TextMessage,
        *,
        status: MessageStatus | None = None,
    ) -> httpx.Response | None:
        try:
            match message:
                case str(msg):
                    message = TextMessageCreate(
                        type=MessageType.text, data=msg
                    ).model_dump()
                case _model if hasattr(message, "model_dump"):
                    message = _model.model_dump()

            payload = self._make_payload([message], notification_category=status)
            return self._send_payload(payload)

        except Exception:
            self.logger.exception("Error sending message to API")
            return None

    def send_table(
        self,
        message: dict | TableMessage,
        *,
        status: MessageStatus | None = None,
    ) -> httpx.Response | None:
        try:
            match message:
                case dict():
                    message = TableMessage(
                        type=MessageType.table, data=message
                    ).model_dump(mode="json")
                case _model if hasattr(message, "model_dump"):
                    message = _model.model_dump(mode="json")

            payload = self._make_payload([message], notification_category=None)
            return self._send_payload(payload)
        except Exception:
            self.logger.exception("Error sending message to API")
            return None

    def info(
        self, message: str | TextMessageCreate, *, status: MessageStatus | None = None
    ) -> httpx.Response | None:
        """Alias for send_text"""
        return self.send_text(message, status=status)

    def notify(
        self, body: str | TextMessageCreate, subject: Optional[str] = None
    ) -> httpx.Response | None:
        """
        Not ideal but needed to implement the abstract method. Subject is ignored but kept for compatibility
        """
        return self.send_text(body)

    def send_download_link(
        self,
        url: str,
        *,
        label: str | None = None,
        status: MessageStatus | None = MessageStatus.result,
    ) -> httpx.Response | None:
        try:
            label = label or "Download Results"
            data = DownloadData(url=url, label=label)
            message = DownloadMessage(type=MessageType.download, data=data).model_dump()
            payload = self._make_payload([message], notification_category=status)
            return self._send_payload(payload)
        except Exception:
            self.logger.exception("Error sending message to API")
            return None

    def send_exception(
        self, message: str | None, *, exception: Exception | None = None
    ) -> httpx.Response | None:
        match exception:
            case None:
                e_body = ""
            case str():
                e_body = exception
            case Exception() as e:
                e_body = str(e)
            case _:
                e_body = ""
        try:
            text = (
                f"{message}: {e_body}"
                if message is not None
                else f"An exception occurred : {e_body}"
                if e_body
                else "An exception occurred."
            )
            message = TextMessageCreate(type=MessageType.text, data=text).model_dump()
            payload = self._make_payload(
                [message], notification_category=MessageStatus.error
            )
            return self._send_payload(payload)
        except Exception:
            self.logger.exception("Error sending message to API")
            return None

    def mark_successful(
        self, *, message: str | TextMessageCreate | TextMessage | None = None
    ) -> httpx.Response | None:
        try:
            match message:
                case str(msg):
                    message = TextMessageCreate(
                        type=MessageType.text, data=msg
                    ).model_dump()
                case _model if hasattr(message, "model_dump"):
                    message = _model.model_dump()
            payload = self._make_payload(
                [message], notification_category=MessageStatus.result
            )
            return self._send_payload(payload)
        except Exception:
            self.logger.exception("Error sending message to API")
            return None

    def mark_error(self, *, messages: list[str] | None = None) -> httpx.Response | None:
        try:
            payload = self._make_payload(
                messages, notification_category=MessageStatus.error
            )
            return self._send_payload(payload)
        except Exception:
            self.logger.exception("Error sending message to API")
            return None

    @property
    def logger(self) -> Logger:
        """
        We use our own logger here
        """
        return logger


def get_notification_endpoints(env: Env) -> DataCanvasEndpointData:
    """
    Get the notification endpoints for the given environment
    """

    from prefect.variables import Variable

    result: dict = Variable.get(name=DataCanvasBlockNames[str(env)])
    return DataCanvasEndpointData(
        domain=result["domain"],
        update_notification_status=result["update_notification_status"],
        update_notification=result["update_notification"],
    )


def get_notification_block(env: Union[Env, TEnv]) -> DataCanvasNotificationBlock:
    if env == Env.dev:
        block = DataCanvasNotificationBlock.load(
            DataCanvasBlockNames.dev_notification.value
        )
        return block
    elif env == Env.prod:
        block = DataCanvasNotificationBlock.load(
            DataCanvasBlockNames.prod_notification.value
        )
        return block
    else:
        msg = f"Invalid environment: {env}. Expected Env.dev or Env.prod."
        raise ValueError(msg)


__all__ = [
    "DataCanvasBlockNames",
    "DataCanvasDomainEndpoints",
    "DataCanvasEndpointData",
    "DataCanvasEndpoints",
    "DataCanvasNotificationBlock",
    "get_notification_block",
    "get_notification_endpoints",
]
