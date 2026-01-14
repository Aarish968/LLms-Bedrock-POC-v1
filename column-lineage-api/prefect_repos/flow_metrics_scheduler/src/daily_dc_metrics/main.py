import functools
import logging
import pandas as pd
import datetime
from prefect import flow, task, get_run_logger

from common_prefect_next.blocks.database import TWarehouse, Warehouse
from common_prefect_next.blocks.environment import Env, TEnv

from common.settings import Settings
from common.sqls import fetch_active_engagement_tag_tables, fetch_daily_metrics

# Set display options for pandas
pd.set_option("display.max_columns", 400)
pd.set_option("display.max_rows", 400)
pd.set_option('display.max_colwidth', 4000)


@flow(name="daily-dc-metrics")
def daily_dc_metrics(env: TEnv | Env, warehouse: TWarehouse | Warehouse, schema: str):
    """
    Generates Daily Metrics
    :param env: Job run env
    :param warehouse: Snowflake db warehouse
    :param schema: Snowflake db schema
    :return: None
    """
    logger = get_run_logger()
    logger.info("Starting the daily-dc-metrics flow")

    settings = Settings(env=env, warehouse=warehouse, schema=schema)
    engine = settings.get_engine()

    tag_view = build_unified_tag_view(engine)
    daily_metrics(tag_view, engine)


# Task to build the unified tag view
@task(name="build_tag_view")
def build_unified_tag_view(engine) -> str:
    """
    Create tag_view and related tables in snowflake db.
    :param engine: Snowflake connection object
    :return: tag_view
    """
    logger = get_run_logger()

    today = pd.Timestamp(datetime.date.today())
    tag_view = f"CPS_DSCI_API.ALL_TAGS_TBL_{today.strftime('%Y_%m_%d')}"

    active_engagements_sql = fetch_active_engagement_tag_tables
    with engine.begin() as conn:

        engagement_tables = pd.read_sql(active_engagements_sql, conn)
        union_sql = engagement_tables.tag_tbls.to_list()
        union_sql = " union select * from ".join(union_sql)
        union_sql = f"""create or replace TABLE {tag_view} as select * from {union_sql};"""

        logger.info(f"Executing SQL to create the tag view table: {union_sql}")
        conn.execute(union_sql)
        logger.info(f"Unified tag view {tag_view} created successfully.")

    return tag_view


# Task to generate daily_metrics
@task(name="daily_metrics")
def daily_metrics(tag_view, engine) -> None:
    """
    Generates dc metrics
    :param tag_view: tag
    :param engine: snowflake connection object
    :return: None
    """
    logger = get_run_logger()
    metrics_sql = fetch_daily_metrics.format(view=tag_view)
    with engine.begin() as conn:
        for sql in metrics_sql.split(';'):
            logger.info(f"Executing SQL: {sql}")
            conn.execute(sql)
    logger.info(f"Daily metrics successfully executed.")
