from typing import TYPE_CHECKING, Sequence

from . import queries
from .models import (
    ActiveLiveboard,
    CanvasAction,
    LiveboardRow,
    LiveboardRowInsert,
    LiveboardRowUpdate,
    ThoughSpotObjects,
    ThoughtSpotObjectRowInsert,
    apply_model,
)

if TYPE_CHECKING:
    from sqlalchemy import Connection, RowMapping


def get_liveboards(
    conn: "Connection",
    liveboard_ids: int | list[int] | None = None,
    liveboard_types: str | list[str] | None = None,
) -> list[LiveboardRow]:
    """
    Get liveboard data from DB.
    :param conn: DB connection
    :param liveboard_ids: (optional) List of liveboard ids to return.
    :param liveboard_types: (optional) List of liveboard types to return
    :return: List of LiveboardRows
    """
    result = conn.execute(queries.query_liveboards(liveboard_ids, liveboard_types))
    return apply_model(result, LiveboardRow)


def get_liveboard(
    conn: "Connection",
    liveboard_id: int,
    liveboard_type: str | None = None,
) -> LiveboardRow | None:
    """
    Get liveboard data from DB.
    :param conn: DB connection
    :param liveboard_id: liveboard id to return
    :param liveboard_type: liveboard type to return
    :return: LiveboardRow
    """
    result = (
        conn.execute(
            queries.query_liveboards(
                liveboard_ids=liveboard_id, liveboard_types=liveboard_type
            )
        )
        .mappings()
        .first()
    )
    if result:
        return LiveboardRow(**result)


def get_liveboard_via_guid(
    conn: "Connection",
    guid: str,
) -> LiveboardRow | None:
    """
    Get liveboard data from DB.
    :param conn: DB connection
    :param guid: liveboard GUID to return
    :return: LiveboardRow
    """
    result = (
        conn.execute(queries.query_liveboard_via_guid(guid=guid)).mappings().first()
    )
    if result:
        return LiveboardRow(**result)


def get_liveboard_nextval(conn: "Connection") -> int:
    """
    Get a nextval for liveboard in DC_FILE_MANAGEMENT_SEQ table.
    :return: a new liveboard_id
    """
    return conn.execute(queries.get_liveboard_nextval()).scalar_one()


def create_liveboard(conn: "Connection", liveboard: LiveboardRowInsert) -> int:
    """
    Create a new liveboard in DB.
    :param conn: DB connection
    :param liveboard: LiveboardRowInsert data
    :return: id of a new liveboard
    """
    conn.execute(queries.create_liveboard(liveboard))
    conn.execute(
        queries.create_liveboard_lineage(
            parent_id=liveboard.parent_liveboard_id, child_id=liveboard.liveboard_id
        )
    )
    return liveboard.liveboard_id


def update_liveboard(conn: "Connection", liveboard: LiveboardRowUpdate) -> None:
    """
    Updates an old Liveboard Row in DB.
    :param conn: DB connection
    :param liveboard: LiveboardRowUpdate Data
    :return:
    """

    conn.execute(queries.update_liveboard(liveboard))


def delete_liveboard(
    conn: "Connection", liveboard_id: int, location: str, updated_by: str
) -> None:
    """
    Soft delete liveboard from DB.
    :param conn: DB connection
    :param liveboard_id: DB ID of the liveboard
    :param location: S3 Location
    :param updated_by: Updated by
    :return:
    """
    conn.execute(
        queries.delete_liveboard_id(
            liveboard_id=liveboard_id, location=location, updated_by=updated_by
        )
    )


def delete_liveboard_guid(conn: "Connection", guid: str, updated_by: str) -> None:
    """
    Soft delete liveboard from DB by guid
    :param conn: DB connection
    :param guid: Liveboard GUID
    :param updated_by: Updated by
    :return:
    """
    conn.execute(queries.delete_liveboard_guid(guid, updated_by))


def get_active_liveboards(conn: "Connection", canvas_id: int) -> list[ActiveLiveboard]:
    """
    Get a list of active liveboard for a canvas_id.
    :param conn: DB connection
    :param canvas_id: Canvas ID
    :return:
    """
    result = conn.execute(queries.get_active_liveboards(canvas_id))
    return apply_model(result, ActiveLiveboard)


def get_canvas_metadata(conn: "Connection", canvas_id: int) -> "RowMapping":
    """
    Get metadata for Canvas
    :param conn: DB connection
    :param canvas_id: Canvas ID
    :return:
    """
    return conn.execute(queries.query_canvas_metadata(canvas_id)).mappings().first()


def get_ts_objects(
    conn: "Connection",
    canvas_id: int,
    object_names: str | list[str] = (),
    is_deleted: bool = False,
) -> list[ThoughSpotObjects]:
    """
    Get Thoughtspot objects including table, worksheets and liveboards
    :param conn: DB connection
    :param canvas_id: Canvas ID
    :param object_names: (optional) Object Name in SF (DC_LIVEBOARDS.PINBOARD_ID),
           e.g. "table", "worksheet", or name of a liveboard. Pass name (str) or a list of names.
    :param is_deleted: (optional) Flag to filter deleted objects
    :return:
    """
    result = conn.execute(queries.get_ts_objects(canvas_id, object_names, is_deleted))
    return apply_model(result, ThoughSpotObjects)


def create_ts_object(conn: "Connection", ts_object: ThoughtSpotObjectRowInsert) -> None:
    """
    Create a TS Object in DB Table
    :param conn: DB connection
    :param ts_object:ThoughtSpotObjectRowInsert
    :return:
    """

    conn.execute(queries.create_ts_object(ts_object))


def get_thoughtspot_data_tables(
    conn: "Connection", schema: str, canvas_id: int
) -> Sequence["RowMapping"]:
    """
    Get Thoughtspot data table and view names.
    :param conn: DB connection
    :param schema: DB schema
    :param canvas_id: Canvas ID
    :return:
    """

    return (
        conn.execute(queries.get_thoughtspot_data_tables(schema, canvas_id))
        .mappings()
        .all()
    )


def get_pending_actions(
    conn: "Connection", canvas_id: int, request_id: int | None = None
) -> list[CanvasAction] | None:
    """
    Get Canvas actions for a given canvas_id and request_id.
    If request_is is not provided, return all pending actions for a canvas..
    :param conn: DB connection
    :param canvas_id: Canvas ID
    :param request_id: Request ID for the Runs
    :return:
    """
    result = conn.execute(queries.get_pending_actions(canvas_id, request_id))

    return apply_model(result, CanvasAction)


def mark_success_request_id(
    conn: "Connection", request_id: int, updated_by: str
) -> None:
    """
    Mark request_id as success in DC_FILE_MANAGEMENT_RUNS.status.
    :param conn: DB connection
    :param request_id: Request ID
    :param updated_by: The Cisco CCO ID of the user requesting the change
    """
    conn.execute(queries.mark_success_request_id(request_id, updated_by))


def mark_success_canvas_id(conn: "Connection", canvas_id: int, updated_by: str) -> None:
    """
    Mark canvas_id as success in dc_canvas_hdr.canvas_status.
    :param conn: DB connection
    :param canvas_id: Canvas ID
    :param updated_by: The Cisco CCO ID of the user requesting the change
    """
    conn.execute(queries.mark_success_canvas_id(canvas_id, updated_by))


def delete_ts_object(
    conn: "Connection", canvas_id: int, object_id: str, deleted_by: str
) -> None:
    """
    Soft Delete TML Object details present in Snowflake
    :param conn: DB connection
    :param canvas_id: Canvas ID
    :param object_id: TML Object GUID to be deleted
    :param deleted_by: User Requested to Delete
    :return:
    """

    conn.execute(
        queries.delete_tml_object(
            canvas_id=canvas_id, object_id=object_id, deleted_by=deleted_by
        )
    )


def get_column_types(conn: "Connection", schema: str, canvas_id: int) -> dict:
    """
    Get columns with their data types for the canvas view.
    :param conn DB connection
    :param schema DB schema
    :param canvas_id: Canvas ID
    :return: a dictionary of column names and their data types.
    """
    result = (
        conn.execute(queries.get_column_types(schema=schema, canvas_id=canvas_id))
        .mappings()
        .all()
    )
    return {col.get("column_name"): col.get("data_type") for col in result}


def get_active_canvases(conn: "Connection") -> set[int]:
    """
    Get a set of active canvases.
    :param conn: DB Connection
    :return: a list of canvas IDs
    """
    return set(conn.scalars(queries.get_active_canvases()).all())


def get_deleted_canvases(conn: "Connection") -> set[int]:
    """
    Get a set of deleted canvases.
    :param conn: DB Connection
    :return: a list of canvas IDs
    """
    return set(conn.scalars(queries.get_deleted_canvases()).all())


def drop_canvas_live_view(conn: "Connection", canvas_id: int) -> None:
    """
    Drop Canvas live view
    :param conn: DB Connection
    :param canvas_id Canvas ID
    :return:
    """
    conn.execute(queries.drop_canvas_live_view(canvas_id))


def drop_canvas_data_table(conn: "Connection", canvas_id: int) -> None:
    """
    Drop Canvas data table
    :param conn: DB Connection
    :param canvas_id Canvas ID
    :return:
    """
    conn.execute(queries.drop_canvas_data_table(canvas_id))
