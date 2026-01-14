import datetime
from typing import Literal, Optional

from pydantic import Field

from . import MessageType, Model


class ParametersMessage(Model):
    type: Literal[MessageType.parameters] = Field(MessageType.parameters.value)
    data: dict = Field(..., description="The human readable parameters")
    timestamp: Optional[datetime.datetime]
    form_data: Optional[dict] = Field(
        None, description="The form data that can be used to rehydrate the form"
    )


class ParametersMessageCreate(ParametersMessage):
    timestamp: datetime.datetime = Field(
        default_factory=lambda _: datetime.datetime.now(datetime.UTC)
    )
