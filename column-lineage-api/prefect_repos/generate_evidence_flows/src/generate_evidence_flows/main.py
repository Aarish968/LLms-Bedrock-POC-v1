from typing import TYPE_CHECKING

from prefect import flow, get_run_logger
from common_prefect_next.blocks.database import TWarehouse, Warehouse
from common_prefect_next.blocks.environment import Env, TEnv

from generate_evidence_flows.common.settings import Settings
from generate_evidence_flows.common.sqls import make_aggregates_query

if TYPE_CHECKING:
    from sqlalchemy import Engine


@flow(name="evidence_zone_aggregates")
def evidence_zone_aggregates(
        env: TEnv | Env,
        warehouse: TWarehouse | Warehouse = Warehouse.x_small
    ) -> None:
    """
    Generates evidence zones aggregates
    :param env: env
    :param warehouse: Snowflake db warehouse
    :return: None
    """
    logger = get_run_logger()
    logger.info(f"Generating evidence zone aggregates for {env}")

    settings = Settings(env=env, warehouse=warehouse)
    engine = settings.get_engine()

    get_aggregates(engine=engine)


def get_aggregates(engine: "Engine") -> None:
    """
    Generate aggregates
    :param engine: Snowflake connection object
    :return: None
    """
    logger = get_run_logger()

    with engine.begin() as conn:
        stmt = make_aggregates_query()
        conn.execute(stmt)
        logger.info(stmt)

