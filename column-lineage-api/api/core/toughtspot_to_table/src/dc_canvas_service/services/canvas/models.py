from typing import Literal, TypeVar

from pydantic import BaseModel, Field, model_validator

from dc_canvas_service.services.snowflake import LiveboardRow

CreateSRCType = Literal["common", "custom_eng", "custom_user", "engagement"]
CopyDestType = Literal["common", "custom_eng", "custom_user", "engagement"]
MoveSRCType = Literal["common", "custom_eng", "custom_user", "engagement"]

SRCDestType = Literal[
    "common",
    "custom_eng",
    "custom_user",
    "engagement",
    "currently_in_ts",
    "canvas",
    "delete",
]

UpdateActionType = Literal["display_name", "share_content"]
UpdateShareMode = Literal["NO_ACCESS", "MODIFY", "READ_ONLY"]

ActionType = TypeVar("ActionType", bound="Action")


class Action(BaseModel):
    def __str__(self):
        return f"{self.__class__.__name__}<{self.__repr_str__(', ')}>"


class CreateAction(Action):
    src_type: CreateSRCType
    liveboard_id: int
    liveboard_name: str
    liveboard_row: LiveboardRow | None


class CloneAction(Action):
    src_type: SRCDestType
    dest_type: SRCDestType
    liveboard_id: int
    liveboard_name: str
    liveboard_row: LiveboardRow | None


class CopyAction(Action):
    src_type: SRCDestType
    dest_type: CopyDestType
    liveboard_id: int
    liveboard_name: str
    liveboard_row: LiveboardRow | None


class UpdateAction(BaseModel):
    action_type: UpdateActionType
    new_name: str | None
    users: list[str] | None
    share_mode: UpdateShareMode | None

    # noinspection PyNestedDecorators
    @model_validator(mode="after")
    @classmethod
    def validate_action_type(cls, values: dict):
        """
        Validate the corresponding action_types has all required values
        """
        action_type = values.get("action_type")
        match action_type:
            case "display_name":
                if not values.get("new_name"):
                    raise ValueError(f"For {action_type=}, new_name should be provided")

            case "share_content":
                if not values.get("users") or not values.get("share_mode"):
                    raise ValueError(
                        f"For {action_type=}, users & share_mode should be provided"
                    )

        return values


class UpdateJSONAction(BaseModel):
    liveboard_id: int
    action: UpdateAction


class MoveAction(Action):
    src_type: MoveSRCType
    dest_type: Literal["delete"]
    liveboard_id: int
    liveboard_row: LiveboardRow | None


class RenameAction(Action):
    liveboard_id: int
    liveboard_name: str
    liveboard_row: LiveboardRow | None


class DeleteAction(Action):
    src_type: Literal["currently_in_ts", "canvas"]
    dest_type: Literal["delete"]
    liveboard_id: int
    liveboard_row: LiveboardRow | None


class ParsedCopy(BaseModel):
    create: list[CreateAction]
    duplicate: list[CopyAction]
    clone: list[CloneAction]


class ParsedDelete(BaseModel):
    delete: list[DeleteAction]
    move: list[MoveAction]


class ParsedNameChange(BaseModel):
    rename: list[RenameAction]


class ParsedActions(BaseModel):
    create: list[CreateAction] = Field(default_factory=list)
    duplicate: list[CopyAction] = Field(default_factory=list)
    clone: list[CloneAction] = Field(default_factory=list)
    delete: list[DeleteAction] = Field(default_factory=list)
    move: list[MoveAction] = Field(default_factory=list)
    rename: list[RenameAction] = Field(default_factory=list)

    def __len__(self):
        return (
            len(self.create)
            + len(self.duplicate)
            + len(self.clone)
            + len(self.delete)
            + len(self.move)
            + len(self.rename)
        )


class CanvasParseActions(BaseModel):
    request_id: int
    canvas_name: str
    changes_json: dict
    status: str
    parsed_actions: ParsedActions
