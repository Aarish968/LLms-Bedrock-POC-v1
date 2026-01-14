from typing import Literal

from pydantic import Field

from . import Model
from .enums import MessageType


class TableMessage(Model):
    type: Literal[MessageType.table] = Field(default=MessageType.table.value)
    data: dict = Field(
        ...,
        description="The data to be displayed in the table.",
        examples=[{"id": 1, "name": "Collector File #1"}],
    )
