from typing import Union

from pydantic import Field, RootModel

from .enums import MessageType, MessageStatus, UiEnum
from .. import Model
from .code import CodeMessage
from .download import DownloadData, DownloadMessage
from .parameters import ParametersMessage, ParametersMessageCreate
from .table import TableMessage
from .text import TextMessage, TextMessageCreate

MessageModels = Union[
    DownloadMessage,
    TableMessage,
    CodeMessage,
    TextMessage,
    ParametersMessage,
]

MessageCreateModels = Union[
    DownloadMessage,
    TableMessage,
    CodeMessage,
    TextMessageCreate,
    ParametersMessageCreate,
]


class CreateMessage(RootModel[MessageCreateModels]):
    root: MessageCreateModels = Field(discriminator="type")


class Message(RootModel[MessageModels]):
    root: MessageModels = Field(discriminator="type")


__all__ = [
    "CodeMessage",
    "CreateMessage",
    "DownloadData",
    "DownloadMessage",
    "Message",
    "MessageCreateModels",
    "MessageModels",
    "MessageStatus",
    "MessageType",
    "Model",
    "ParametersMessage",
    "ParametersMessageCreate",
    "TableMessage",
    "TextMessage",
    "TextMessageCreate",
    "UiEnum",
]
