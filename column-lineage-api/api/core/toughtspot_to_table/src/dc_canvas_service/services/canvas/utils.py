from typing import TYPE_CHECKING

from dc_canvas_service.services.snowflake import get_liveboard

from .exceptions import CanvasLiveboardNotFound
from .models import (
    CloneAction,
    CopyAction,
    CreateAction,
    DeleteAction,
    MoveAction,
    ParsedActions,
    ParsedCopy,
    ParsedDelete,
    ParsedNameChange,
    RenameAction,
)

if TYPE_CHECKING:
    from sqlalchemy import Connection


def formulate_ts_liveboard_name(
    liveboard_name: str, engagement_id: int, canvas_id: int
) -> str:
    """
    Function to add Canvas_ID tag & Engagement_ID tag to Liveboard Name
    Args:
        liveboard_name: Liveboard Name
        engagement_id: Engagement ID
        canvas_id: Canvas ID

    Returns:
         str: New Liveboard with Canvas & Engagement ID Tags
    """
    engagement_tag = f"E_{engagement_id}"
    canvas_tag = f"C_{canvas_id}"

    if engagement_tag not in liveboard_name:
        liveboard_name += f" - {engagement_tag}"

    if canvas_tag not in liveboard_name:
        liveboard_name += f" - {canvas_tag}"

    return liveboard_name


def parse_copy_json(conn: "Connection", copy_json: dict) -> ParsedCopy:
    """
    Parses Copy Json from Change Json
    Args:
        conn (Connection): Snowflake Connection
        copy_json (dict): contains copy details

    Returns:
        dict: formatted actions as create, clone, copy
    """

    create_actions = []
    clone_actions = []
    copy_actions = []

    for liveboard_id, copy_detail in copy_json.items():
        src_type = copy_detail.get("from")
        dest_type = copy_detail.get("to")
        display_name = copy_detail.get("display_name")

        liveboard_row = get_liveboard(conn=conn, liveboard_id=liveboard_id)

        if not liveboard_row:
            raise CanvasLiveboardNotFound("Liveboard ID =" + str(liveboard_id))

        action = {
            "src_type": src_type,
            "dest_type": dest_type,
            "liveboard_id": liveboard_id,
            "liveboard_name": display_name,
            "liveboard_row": liveboard_row,
        }
        match dest_type:
            case "currently_in_ts" | "canvas":
                match src_type:
                    case "currently_in_ts" | "canvas":
                        clone_actions.append(CloneAction(**action))
                    case _:
                        create_actions.append(CreateAction(**action))
            case _:
                copy_actions.append(CopyAction(**action))

    return ParsedCopy(
        create=create_actions, duplicate=copy_actions, clone=clone_actions
    )


def parse_delete_json(conn: "Connection", delete_json: dict) -> ParsedDelete:
    """
    Parses Delete Json from Change Json
    Args:
        conn (Connection): Snowflake Connection
        delete_json (dict): contains delete details

    Returns:
        dict: formatted actions as delete, move
    """

    delete_actions = []
    move_actions = []

    for liveboard_id, delete_detail in delete_json.items():
        src_type = delete_detail.get("from")
        dest_type = delete_detail.get("to", "delete")

        liveboard_row = get_liveboard(conn=conn, liveboard_id=liveboard_id)

        action = {
            "src_type": src_type,
            "dest_type": dest_type,
            "liveboard_id": liveboard_id,
            "liveboard_row": liveboard_row,
        }

        match src_type:
            case "currently_in_ts" | "canvas":
                delete_actions.append(DeleteAction(**action))
            case _:
                move_actions.append(MoveAction(**action))

    return ParsedDelete(delete=delete_actions, move=move_actions)


def parse_name_change_json(
    conn: "Connection", name_change_json: dict
) -> ParsedNameChange:
    """
    Parses Name Change Json from Change Json
    Args:
        conn (Connection): Snowflake Connection
        name_change_json (dict): contains name_change details

    Returns:
        dict: formatted actions as update
    """

    rename_actions = []

    for liveboard_id, name_change_detail in name_change_json.items():
        display_name = name_change_detail.get("new_display_name")
        liveboard_row = get_liveboard(conn=conn, liveboard_id=liveboard_id)

        action = RenameAction(
            liveboard_id=liveboard_id,
            liveboard_name=display_name,
            liveboard_row=liveboard_row,
        )

        rename_actions.append(action)

    return ParsedNameChange(rename=rename_actions)


def parse_change_json(conn: "Connection", change_json: dict) -> ParsedActions:
    """
    Parses Change Json from the File Management Runs
    Args:
        conn (Connection): Snowflake Connection
        change_json (dict): contains copy, delete, name_change details

    Returns:
        dict: formatted actions as create, clone, copy, delete, move, update
    """

    copy_json = change_json.get("copy")
    delete_json = change_json.get("delete")
    name_change_json = change_json.get("display_name_change")

    parsed_actions = ParsedActions(
        **parse_copy_json(conn, copy_json).model_dump(),
        **parse_delete_json(conn, delete_json).model_dump(),
        **parse_name_change_json(conn, name_change_json).model_dump(),
    )

    return parsed_actions
