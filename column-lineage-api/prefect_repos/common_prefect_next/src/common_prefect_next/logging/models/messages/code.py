from typing import Literal, Union

from pydantic import Field

from . import MessageType, Model


class CodeMessage(Model):
    type: Literal[MessageType.code] = Field(MessageType.code.value)
    data: Union[list[dict], dict, str]
