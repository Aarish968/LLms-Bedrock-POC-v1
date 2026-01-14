from typing import TYPE_CHECKING

from prefect import get_run_logger

from dc_canvas_retention.common import is_view_empty
from dc_canvas_retention.common.queries import (
    get_old_canvases,
    make_deactivate_canvas_stmt,
    make_empty_view_stmt,
)

if TYPE_CHECKING:
    from sqlalchemy import Connection, Engine

    from dc_canvas_retention.common.models import CanvasModel


def deactivate_canvases(engine: "Engine", n_days: int, db_schema: str) -> None:
    """
    Process to deactivate all pending canvases.
    :param engine DB Engine
    :param n_days Number of days to look back for canvas deactivation
    :param db_schema DB schema which has the live view.
    return
    """
    logger = get_run_logger()

    with engine.begin() as conn:
        canvases = get_old_canvases(conn=conn, n_days=n_days, db_schema=db_schema)
        canvases = [canvas for canvas in canvases if not canvas.is_deactivated]
        msg = f"{len(canvases)} canvases to deactivate..."
        logger.info(msg)

        for canvas in canvases:
            logger.info(f"Deactivating {canvas.canvas_id=}")
            deactivate_canvas(
                conn=conn,
                canvas_id=canvas.canvas_id,
                view_definition=canvas.view_definition,
            )
            logger.info(f"Deactivated {canvas.canvas_id=}")


def deactivate_canvas(conn: "Connection", canvas_id: int, view_definition: str) -> None:
    """
    Process to deactivate a specific canvas
    :param conn DB Connection
    :param canvas_id Canvas ID
    :param view_definition View definition (DDL) of canvas live view.
    """

    stmt = make_deactivate_canvas_stmt(canvas_id=canvas_id)
    conn.execute(stmt)

    if is_view_empty(view_definition):
        return

    deactivate_view_stmt = make_empty_view_stmt(view_ddl=view_definition)
    conn.execute(deactivate_view_stmt)
