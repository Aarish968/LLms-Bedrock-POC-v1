from typing import TYPE_CHECKING

from prefect import get_run_logger

from dc_canvas_retention.common.queries import (
    get_soft_deactivation_candidates,
    make_soft_deactivate_canvas_stmt,
)

if TYPE_CHECKING:
    from sqlalchemy import Engine


def soft_deactivate_canvases(
    engine: "Engine", row_count_threshold: int, db_schema: str
) -> None:
    """
    Soft deactivate canvases that exceed the cumulative row count threshold.
    This sets enabled=FALSE on canvases without modifying their DDL or names.
    :param engine DB Engine
    :param row_count_threshold Row count threshold for cumulative deactivation
    :param db_schema DB schema which has the live view.
    """
    logger = get_run_logger()

    with engine.begin() as conn:
        canvas_ids = get_soft_deactivation_candidates(
            conn=conn, row_count_threshold=row_count_threshold, db_schema=db_schema
        )

        if not canvas_ids:
            logger.info("No canvases to soft deactivate")
            return

        msg = f"{len(canvas_ids)} canvases to soft deactivate due to row count threshold: {canvas_ids}"
        logger.info(msg)

        stmt = make_soft_deactivate_canvas_stmt(canvas_ids)
        result = conn.execute(stmt, {"canvas_ids": canvas_ids})
        logger.info(f"Soft deactivated {result.rowcount} canvases successfully")
