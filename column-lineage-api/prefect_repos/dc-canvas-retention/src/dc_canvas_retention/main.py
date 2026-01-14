from common_prefect_next.blocks.database import TWarehouse, Warehouse
from common_prefect_next.blocks.environment import Env, TEnv  # noqa: TC002
from common_prefect_next.utils import setup_extra_loggers
from prefect import flow

from dc_canvas_retention.common import (
    Settings,
)
from dc_canvas_retention.deactivate_canvases import deactivate_canvases
from dc_canvas_retention.delete_canvases import delete_canvases
from dc_canvas_retention.soft_deactivate_canvases import soft_deactivate_canvases


@flow(flow_run_name="dc_canvas_retention")
@setup_extra_loggers
def dc_canvas_retention_flow(
    env: TEnv | Env,
    row_count_threshold: int = 1_750_000_000,
    warehouse: TWarehouse | Warehouse = Warehouse.small,
    n_days_deactivate: int = 70,
    n_days_delete: int = 120,
) -> None:
    """
    This flow runs a three-stage process:

    1. Deactivates canvases older than `n_days_deactivate` by updating DDL and prepending "(DEACTIVATED)" to name.
    2. Deletes canvases older than `n_days_delete` that are already deactivated.
    3. Soft deactivates canvases exceeding the cumulative row count threshold by setting enabled=FALSE.
    """

    settings = Settings(
        n_days_deactivate=n_days_deactivate,
        n_days_delete=n_days_delete,
        env=env,
        warehouse=warehouse,
        row_count_threshold=row_count_threshold,
    )

    engine = settings.get_engine()

    # Stage 1: Deactivate old canvases
    deactivate_canvases(
        engine, n_days=settings.deactivate_lookback_days, db_schema=settings.db_schema
    )

    # Stage 2: Delete old deactivated canvases
    delete_canvases(
        env=settings.env,
        engine=engine,
        n_days=settings.delete_lookback_days,
        db_schema=settings.db_schema,
    )

    # Stage 3: Soft deactivate canvases exceeding row count threshold
    soft_deactivate_canvases(
        engine=engine,
        row_count_threshold=settings.row_count_threshold,
        db_schema=settings.db_schema,
    )
