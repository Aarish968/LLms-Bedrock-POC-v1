from typing import TYPE_CHECKING

import requests
from common_prefect_next.events.canvas import emit_canvas_cleanup_requested
from prefect import get_run_logger
from requests import Session

from dc_canvas_retention.common.queries import get_old_canvases, make_delete_canvas_stmt

if TYPE_CHECKING:
    from common_canvas_next.models import TEnv
    from sqlalchemy import Connection, Engine

    from dc_canvas_retention.common.models.canvas import TGetApiAccessToken


def get_requests_session(get_api_access_token: "TGetApiAccessToken") -> Session:
    """
    Creates HTTPS request session with auth token to call DC API.
    """
    req_session = requests.Session()
    req_session.headers.update(
        {
            "Authorization": f"Bearer {get_api_access_token()}",
            "User-Agent": "DcCanvasRetentionDeleteCanvases",
        }
    )
    return req_session


def delete_canvases(
    env: "TEnv",
    engine: "Engine",
    n_days: int,
    db_schema: str,
) -> None:
    """
    Delete old canvases.
    :param env ENV name
    :param engine DB engine
    :param n_days Number of days to look back for canvas deletion
    :param db_schema DB schema which has the live view.
    return
    """

    logger = get_run_logger()

    with engine.begin() as conn:
        canvases = get_old_canvases(conn=conn, n_days=n_days, db_schema=db_schema)
        msg = f"{len(canvases)} canvases to delete..."
        logger.info(msg)

        for canvas in canvases:
            logger.info(f"Deleting {canvas.canvas_id=}")
            delete_canvas(env=env, conn=conn, canvas_id=canvas.canvas_id)
            logger.info(f"Deleted {canvas.canvas_id=}")


def delete_canvas(env: "TEnv", conn: "Connection", canvas_id: int) -> None:
    """
    Process to delete a specific canvas
    :param env Env name
    :param conn DB Connection
    :param canvas_id Canvas ID
    """

    stmt = make_delete_canvas_stmt(canvas_id=canvas_id)
    conn.execute(stmt)

    emit_canvas_cleanup_requested(env=env, canvas_id=canvas_id)
