from enum import Enum
from typing import Any, NewType

from pydantic import BaseModel, ConfigDict, GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema


class BaseEnum(str, Enum):
    def __str__(self):
        return str.__str__(self)


class Model(BaseModel):
    model_config = ConfigDict(
        use_enum_values=True,
        populate_by_name=True,
        arbitrary_types_allowed=True,
        extra="allow",
    )


class TypedStr(str):
    """
    https://docs.pydantic.dev/latest/concepts/types/#as-a-method-on-a-custom-type
    """

    __slots__ = ()

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_after_validator_function(cls, handler(str))


class TableName(TypedStr):
    __slots__ = ()


class SerialNumber(TypedStr):
    __slots__ = ()


__all__ = ["BaseEnum", "Model", "SerialNumber", "TableName"]
