from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Integer, String, TextClause, TextualSelect, text

from dc_canvas_service.common.sql_types import JSONVarchar

if TYPE_CHECKING:
    from sqlalchemy.sql.elements import TextClause

    from .models import (
        LiveboardRowInsert,
        LiveboardRowUpdate,
        ThoughtSpotObjectRowInsert,
    )


def query_liveboards(
    liveboard_ids: int | list[int] | None = None,
    liveboard_types: str | list[str] | None = None,
) -> TextClause:
    """
    Query Liveboards.
    :param liveboard_ids: (optional) List of liveboard ids to return.
    :param liveboard_types: (optional) List of liveboard types to return.
    :return TextualSelect
    """

    sql = """
           SELECT liveboard_id,
                  display_name,
                  liveboard_type,
                  IFF(location like 's3://%', location, NULL) as location,
                  create_dtm, created_by,
                  update_dtm, updated_by, is_deleted, liveboard_type_value, liveboard_name,
                  guid, canvas_id, canvas_import_status
           FROM DC_FILE_MANAGEMENT_LIVEBOARDS
           WHERE is_deleted = 'F'
    """

    params = {}

    if liveboard_ids:
        if isinstance(liveboard_ids, int):
            liveboard_ids = [liveboard_ids]
        sql += " AND liveboard_id IN (:liveboard_ids)"

        params.update({"liveboard_ids": liveboard_ids})

    if liveboard_types:
        if isinstance(liveboard_ids, str):
            liveboard_types = [liveboard_types]
        sql += " AND liveboard_type IN (:liveboard_types)"

        params.update({"liveboard_types": liveboard_types})

    query = text(sql).bindparams(**params)

    return query


def query_liveboard_via_guid(guid: str) -> TextClause:
    """
    Query Liveboard via GUID
    :param guid: Liveboard GUID
    :return:
    """

    stmt = text(
        """
        SELECT
            liveboard_id,
            display_name,
            liveboard_type,
            IFF(location like 's3://%', location, NULL) as location,
            create_dtm, created_by,
            update_dtm, updated_by, is_deleted, liveboard_type_value, liveboard_name,
            guid, canvas_id, canvas_import_status
        FROM 
            DC_FILE_MANAGEMENT_LIVEBOARDS
        WHERE 
            is_deleted = 'F'
            AND guid = :guid
        """
    ).bindparams(guid=guid)

    return stmt


def get_liveboard_nextval() -> TextClause:
    """
    Query to get a next sequence value for a liveboard_id (DC_FILE_MANAGEMENT_SEQ).
    :return: int
    """

    return text("select DC_FILE_MANAGEMENT_SEQ.NEXTVAL")


def create_liveboard(row: LiveboardRowInsert) -> TextClause:
    """
    Query to create a Liveboard in DC_FILE_MANAGEMENT_LIVEBOARDS
    :param row: LiveboardRowInsert object
    :return: TextualSelect
    """

    stmt = text(
        """INSERT INTO dc_file_management_liveboards
                  (LIVEBOARD_ID, DISPLAY_NAME, LIVEBOARD_TYPE, LIVEBOARD_TYPE_VALUE,
                   LOCATION, CREATED_BY, GUID, LIVEBOARD_NAME, CANVAS_ID,
                   CANVAS_IMPORT_STATUS, CREATE_DTM, IS_DELETED)
           VALUES (:liveboard_id, :display_name, :liveboard_type, :liveboard_type_value,
                   :location, :created_by, :guid, :liveboard_name, :canvas_id,
                   :canvas_import_status, CURRENT_TIMESTAMP, 'F')
        """
    ).bindparams(
        liveboard_id=row.liveboard_id,
        display_name=row.display_name,
        liveboard_type=row.liveboard_type.value,
        liveboard_type_value=row.liveboard_type_value,
        location=row.location,
        created_by=row.created_by,
        guid=row.guid,
        liveboard_name=row.liveboard_name,
        canvas_id=row.canvas_id,
        canvas_import_status="Success",
    )
    return stmt


def create_liveboard_lineage(parent_id: int, child_id: int) -> TextClause:
    """
    Query to create liveboard lineage (DC_FILE_LINEAGE_TABLE).
    :param parent_id:
    :param child_id:
    :return:
    """

    stmt = text(
        """
        INSERT INTO dc_file_lineage_table (parent_liveboard_id, child_liveboard_id, create_dtm)
        VALUES (:parent_id, :child_id, CURRENT_TIMESTAMP)
        """
    ).bindparams(
        parent_id=parent_id,
        child_id=child_id,
    )
    return stmt


def update_liveboard(row: LiveboardRowUpdate) -> TextClause:
    """
    Query to update a Liveboard in DC_FILE_MANAGEMENT_LIVEBOARDS
    :param row: LiveboardRowUpdate object
    :return: TextClause
    """

    stmt = text(
        """
        UPDATE dc_file_management_liveboards
        SET
            display_name = :display_name,
            updated_by = :updated_by
        WHERE
            liveboard_id = :liveboard_id
        """
    ).bindparams(
        display_name=row.display_name,
        updated_by=row.updated_by,
        liveboard_id=row.liveboard_id,
    )

    return stmt


def delete_liveboard_id(
    liveboard_id: int, location: str, updated_by: str
) -> TextClause:
    """
    Query to soft delete a liveboard id.
    :param liveboard_id: ID of the Liveboard
    :param updated_by: Updated by
    :param location: S3 Location
    :return:
    """
    stmt = text(
        """
        UPDATE 
            dc_file_management_liveboards
        SET 
            is_deleted = 'T', 
            liveboard_type = 'delete',
            canvas_id = NULL,
            guid = NULL,
            liveboard_type_value = NULL,
            update_dtm = current_timestamp,
            updated_by = :updated_by,
            location = :location
         WHERE 
            liveboard_id = :liveboard_id
        """
    ).bindparams(updated_by=updated_by, liveboard_id=liveboard_id, location=location)
    return stmt


def delete_liveboard_guid(guid: str, updated_by: str) -> TextClause:
    """
    Query to soft delete a liveboard id.
    :param guid: Liveboard GUID
    :param updated_by: Updated by
    :return:
    """
    stmt = text(
        """
        UPDATE 
            dc_file_management_liveboards
        SET 
            is_deleted = 'T', 
            liveboard_type = 'delete',
            canvas_id = NULL,
            guid = NULL,
            liveboard_type_value = NULL,
            update_dtm = current_timestamp,
            updated_by = :updated_by,
            location = :location
         WHERE 
            guid = :guid
        """
    ).bindparams(
        updated_by=updated_by,
        guid=guid,
    )
    return stmt


def get_active_liveboards(canvas_id: int) -> TextClause:
    """
    Query to get active liveboard for a canvas_id.
    :param canvas_id:
    :return:
    """
    stmt = text(
        """
        SELECT
            LIVEBOARD_ID,
            PARENT_LIVEBOARD_ID,
            GUID, 
            LOCATION
        FROM 
            DC_FILE_MANAGEMENT_LIVEBOARDS AS DC_C
            JOIN DC_FILE_LINEAGE_TABLE AS DC_R
                ON DC_C.LIVEBOARD_ID = DC_R.CHILD_LIVEBOARD_ID
        WHERE 
            LIVEBOARD_TYPE = 'canvas'
            AND IS_DELETED = 'F'
            AND CANVAS_ID = :canvas_id
            AND GUID IS NOT NULL
        """
    ).bindparams(canvas_id=canvas_id)
    return stmt


def query_canvas_metadata(canvas_id: int) -> TextualSelect:
    """
    Query Canvas metadata by id.

    :param canvas_id: Canvas ID.
    :return: TextualSelect
    """
    stmt = (
        text(
            """
            WITH
            CANVAS_ENGAGEMENT AS (
                SELECT 
                    CANVAS_ID, DC_ENGAGEMENT_ID
                FROM
                    DC_CANVAS_HDR
                WHERE
                    CANVAS_ID = :canvas_id
            ),
            ENGAGEMENT_USERS AS (
                SELECT 
                    CE.CANVAS_ID, USER_ID
                FROM
                    DC_CAM_TO_ENGAGEMENT C2E
                    JOIN CANVAS_ENGAGEMENT CE
                        ON C2E.DC_ENGAGEMENT_ID = CE.DC_ENGAGEMENT_ID
                WHERE 
                    C2E.IS_DELETED = 'F'
            ),
            NAMED_USERS AS (
                SELECT
                    EU.CANVAS_ID, CISCO_CCO_ID
                FROM
                    DC_USERS USR
                    JOIN ENGAGEMENT_USERS EU 
                        ON USR.USER_ID = EU.USER_ID
                WHERE
                    IS_DELETED = 'F'
            )
            SELECT
                H.CANVAS_ID, H.DC_ENGAGEMENT_ID, H.CANVAS_TYPE, H.IS_DELETED, 
                ARRAY_AGG(NU.CISCO_CCO_ID) AS USERS
            FROM
                DC_CANVAS_HDR H
                LEFT JOIN NAMED_USERS NU 
                    ON H.CANVAS_ID = NU.CANVAS_ID
            WHERE
                H.CANVAS_ID = :canvas_id
            GROUP BY
                H.CANVAS_ID, DC_ENGAGEMENT_ID, CANVAS_TYPE, IS_DELETED
        """
        )
        .bindparams(canvas_id=canvas_id)
        .columns(
            canvas_id=Integer,
            dc_engagement_id=Integer,
            canvas_type=String,
            users=JSONVarchar,
        )
    )

    return stmt


def get_ts_objects(
    canvas_id: int, object_names: str | list = (), is_deleted: bool = False
) -> TextClause:
    """
    Query TS Objects by Canvas ID and Specific Params
    :param canvas_id: Canvas ID
    :param object_names: (optional) Object Name in SF (DC_LIVEBOARDS.PINBOARD_ID),
           e.g. "table", "worksheet", or name of a liveboard. Pass name (str) or a list of names.
    :param is_deleted: (optional) filter deleted objects
    :return: TextClause
    """

    sql = """
        SELECT 
            pinboard_id object_id,
            pinboard_name object_name,
            canvas_id,
            dashboard_name canvas_name,
            link,
            object_uuid guid,
            is_deleted
        FROM 
            DC_LIVEBOARDS
         WHERE 
            CANVAS_ID = :canvas_id
    """
    params = {"canvas_id": canvas_id}

    if not is_deleted:
        sql += " AND IS_DELETED = 'F'"

    if object_names:
        if isinstance(object_names, str):
            object_names = [object_names]
        sql += " AND pinboard_name IN (:object_names)"
        params["object_names"] = object_names

    query = text(sql).bindparams(**params)

    return query


def create_ts_object(row: ThoughtSpotObjectRowInsert) -> TextClause:
    """
    Query to create a Pinboard TS Object in DC_LIVEBOARDS
    :param row: ThoughtSpotObjectRowInsert object
    :return: TextualSelect
    """

    stmt = text(
        """INSERT INTO dc_liveboards
                  (pinboard_name, canvas_id, dashboard_name, link, object_uuid,
                    created_by, create_dtm, is_deleted)
           VALUES (:pinboard_name, :canvas_id, :dashboard_name, :link, :object_uuid,
                    :created_by, CURRENT_TIMESTAMP, 'F')
        """
    ).bindparams(
        pinboard_name=row.pinboard_name,
        canvas_id=row.canvas_id,
        dashboard_name=row.dashboard_name,
        link=row.link,
        object_uuid=row.object_uuid,
        created_by=row.created_by,
    )
    return stmt


def get_thoughtspot_data_tables(schema: str, canvas_id: int) -> TextClause:
    """
    Query INFORMATION_SCHEMA to get Thoughtspot table and view.
    :param schema: DB schema
    :param canvas_id: Canvas ID
    :return: TextClause
    """
    sql = """
    SELECT table_name, table_type
      FROM information_schema.tables
     WHERE table_name IN (:canvas_table, :canvas_view)
       AND table_schema = :schema
     """
    query = text(sql).bindparams(
        canvas_table=f"CANVAS_{canvas_id}_THOUGHT_SPOT",
        canvas_view=f"CANVAS_{canvas_id}_THOUGHT_SPOT_V",
        schema=schema,
    )
    return query


def get_pending_actions(canvas_id: int, request_id: int | None = None) -> TextualSelect:
    """
    Get Canvas actions for a given canvas_id and request_id.
    If request_is is not provided, return all pending actions for a canvas..
    :param canvas_id Canvas ID
    :param request_id Request IDs
    """
    sql = """
        SELECT 
            request_id, 
            canvas_name, 
            status, 
            changes_json
        FROM 
            dc_file_management_runs
        WHERE 
            canvas_name = :canvas_name
    """

    if request_id:
        sql += f" AND request_id = {request_id}"
    else:
        sql += " AND status = 'Pending'"

    sql += " ORDER BY NVL(UPDATE_DTM, CREATE_DTM)"

    return (
        text(sql)
        .bindparams(canvas_name=f"CANVAS-{canvas_id}")
        .columns(
            request_id=Integer,
            canvas_name=String,
            status=String,
            changes_json=JSONVarchar,
        )
    )


def mark_success_request_id(request_id: int, updated_by: str) -> TextClause:
    """
    Query to mark request_id as success in DC_FILE_MANAGEMENT_RUNS.status.
    :param request_id: Request ID
    :param updated_by: The Cisco CCO ID of the user requesting the change
    :return:
    """

    sql = """
    UPDATE dc_file_management_runs
       SET status = 'Success', 
           update_dtm = current_timestamp,
           updated_by = :updated_by
     WHERE request_id = :request_id 
     """
    query = text(sql).bindparams(request_id=request_id, updated_by=updated_by)
    return query


def mark_success_canvas_id(canvas_id: int, updated_by: str) -> TextClause:
    """
    Query to mark canvas_id as success in dc_canvas_hdr.canvas_status.
    :param canvas_id: Request ID
    :param updated_by: The Cisco CCO ID of the user requesting the change
    :return:
    """

    sql = """
    UPDATE dc_canvas_hdr
       SET canvas_status = 'success', 
           update_dtm = current_timestamp,
           updated_by = :updated_by
     WHERE canvas_id = :canvas_id 
     """
    query = text(sql).bindparams(canvas_id=canvas_id, updated_by=updated_by)
    return query


def delete_tml_object(canvas_id: int, object_id: str, deleted_by: str) -> TextClause:
    """
    Query to soft delete an object id.
    :param canvas_id: Canvas id
    :param object_id: TML Object id
    :param deleted_by: User Requested for Delete
    :return:
    """
    stmt = text(
        """
        UPDATE dc_liveboards
           SET is_deleted = 'T', 
               update_dtm = current_timestamp,
               updated_by = :deleted_by
         WHERE canvas_id = :canvas_id
           AND object_uuid = :object_id
        """
    ).bindparams(canvas_id=canvas_id, object_id=object_id, deleted_by=deleted_by)
    return stmt


def get_column_types(schema: str, canvas_id: int) -> TextClause:
    """
    Query to get columns with their data types for the canvas view.
    :param schema: DB schema
    :param canvas_id: Canvas ID
    :return:
    """
    stmt = text(
        """
        SELECT column_name, data_type
         FROM information_schema.columns
        WHERE table_name= :canvas_table
          AND table_schema = :schema
    """
    ).bindparams(canvas_table=f"CANVAS_{canvas_id}_THOUGHT_SPOT_V", schema=schema)
    return stmt


def get_active_canvases() -> TextClause:
    """
    Query to get a list of active canvases.
    :return:
    """
    return text("""
        SELECT canvas_id
          FROM dc_canvas_hdr
         WHERE is_deleted = 'F'
         ORDER BY 1
    """)


def get_deleted_canvases() -> TextClause:
    """
    Query to get a list of deleted canvases.
    :return:
    """
    return text("""
        SELECT canvas_id
          FROM dc_canvas_hdr
         WHERE is_deleted = 'T'
         ORDER BY 1
    """)


def drop_canvas_live_view(canvas_id: int) -> TextClause:
    """
    SQL to drop Canvas live view
    :param canvas_id Canvas ID
    :return:
    """

    return text("""DROP VIEW IF EXISTS IDENTIFIER(:view_name)""").bindparams(
        view_name=f"CANVAS_{canvas_id}_THOUGHT_SPOT_V"
    )


def drop_canvas_data_table(canvas_id: int) -> TextClause:
    """
    SQL to drop Canvas data table
    :param canvas_id Canvas ID
    :return:
    """

    return text("""DROP TABLE IF EXISTS IDENTIFIER(:table_name)""").bindparams(
        table_name=f"CANVAS_{canvas_id}_THOUGHT_SPOT"
    )
