import logging

from common_prefect_next.blocks.database import TWarehouse, Warehouse
from common_prefect_next.blocks.environment import Env, TEnv
from common_prefect_next.utils import setup_extra_loggers
from prefect import flow, task

from canvas_optimized_tables.common import Settings
from canvas_optimized_tables.common.models import (
    DailyViewStatements,
    SQLStatement,
    SQLStatements,
)
from canvas_optimized_tables.common.sql_templates import prepare_last_updated_stmts
from canvas_optimized_tables.common.sql_util import (
    prepare_sql_statements,
)
from canvas_optimized_tables.common.tasks import (
    create_daily_view,
    make_optimized_tables,
    update_snapshot_table, update_last_update_core_tables,
)

logger = logging.getLogger(__name__)


@task()
def make_optimized_tables_task(settings: Settings, statements: SQLStatements):
    make_optimized_tables(settings=settings, statements=statements)


@task()
def create_daily_view_task(settings: Settings, statements: DailyViewStatements):
    create_daily_view(settings=settings, statements=statements)


@task()
def update_snapshot_data_task(settings: Settings, statement: SQLStatement):
    update_snapshot_table(settings=settings, statement=statement)
    
@task()
def update_last_updated_core_tables_task(settings: Settings, statements: list[SQLStatement]):
    update_last_update_core_tables(settings=settings, statements=statements)

@flow()
@setup_extra_loggers
def make_optimized_tables_flow(env: TEnv | Env, warehouse: TWarehouse | Warehouse):
    settings = Settings(env=env, warehouse=warehouse)
    statements = prepare_sql_statements()
    make_optimized_tables_task(settings=settings, statements=statements.sql_statements)
    create_daily_view_task(
        settings=settings, statements=statements.daily_view_statements
    )
    update_snapshot_data_task(
        settings=settings, statement=statements.snapshot_statement
    )

@flow()
@setup_extra_loggers
def update_last_updated_core_tables_flow(
    env: TEnv | Env, warehouse: TWarehouse | Warehouse
):
    settings = Settings(env=env, warehouse=warehouse)
    statements = prepare_last_updated_stmts()
    update_last_updated_core_tables_task(settings=settings, statements=statements)
    