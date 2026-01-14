import datetime
from typing import Literal, Optional

from pydantic import Field

from . import MessageType, Model


class TextMessage(Model):
    type: Literal[MessageType.text] = Field(MessageType.text.value)
    data: str
    timestamp: Optional[datetime.datetime]


class TextMessageCreate(TextMessage):
    timestamp: datetime.datetime = Field(
        default_factory=lambda _: datetime.datetime.now(datetime.UTC)
    )
