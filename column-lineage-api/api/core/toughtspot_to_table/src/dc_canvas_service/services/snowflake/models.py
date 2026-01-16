from datetime import datetime
from typing import TYPE_CHECKING, Type, TypeVar

from pydantic import BaseModel, model_validator

from dc_canvas_service.services.snowflake.exceptions import DBModelException

from .enums import (
    LiveboardType,  # noqa: TC001 (LiveboardRowInsert reference must be defined)
)

if TYPE_CHECKING:
    from sqlalchemy.engine.cursor import Result


CanvasModelType = TypeVar("CanvasModelType", bound="BaseModel")


def apply_model(result: "Result", model: Type[BaseModel]) -> list[CanvasModelType]:
    return [model(**row) for row in result.mappings().all()]


class LiveboardRow(BaseModel):
    liveboard_id: int
    display_name: str | None
    liveboard_type: str | None
    location: str | None
    create_dtm: datetime | None
    created_by: str | None
    update_dtm: datetime | None
    updated_by: str | None
    is_deleted: str | None
    liveboard_type_value: str | None
    liveboard_name: str | None
    guid: str | None
    canvas_id: int | None
    canvas_import_status: str | None


class LiveboardRowInsert(BaseModel):
    parent_liveboard_id: int  # Used for lineage table
    liveboard_id: int
    display_name: str
    liveboard_type: LiveboardType
    location: str
    created_by: str
    liveboard_type_value: str  # "" for canvas, "eng_{eng_id}" for engagement, "someuser@email.com" for user
    liveboard_name: str  # f"{liveboard_id}.tml"
    guid: str | None = None
    canvas_id: int | None = None

    # noinspection PyNestedDecorators
    @model_validator(mode="before")
    @classmethod
    def validate_liveboard_type(cls, values: dict) -> dict:
        """
        If liveboard_type is 'canvas', then GUID and canvas_id must not be None
        """
        liveboard_type = values.get("liveboard_type")
        if liveboard_type == "canvas" and (
            values.get("guid") is None or values.get("canvas_id") is None
        ):
            msg = "GUID and canvas_id must be provided for liveboard_type 'canvas'"
            raise DBModelException(msg)

        return values


class LiveboardRowUpdate(BaseModel):
    liveboard_id: int
    display_name: str
    updated_by: str


class ActiveLiveboard(BaseModel):
    liveboard_id: int
    parent_liveboard_id: int | None = None
    guid: str | None = None
    location: str
    type: str = "LIVEBOARD"


class ThoughSpotObjects(BaseModel):
    object_id: int
    object_name: str
    canvas_id: int
    canvas_name: str
    link: str
    guid: str
    is_deleted: bool


class ThoughtSpotObjectRowInsert(BaseModel):
    pinboard_name: str
    canvas_id: int
    dashboard_name: str
    link: str
    object_uuid: str
    created_by: str


class CanvasAction(BaseModel):
    request_id: int
    canvas_name: str
    changes_json: dict
    status: str
