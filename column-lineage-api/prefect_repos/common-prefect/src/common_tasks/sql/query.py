from typing import Callable, Literal, Optional, Any

import pandas as pd
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs
from sqlalchemy.engine import Engine, Row

T_ENV = Literal["dev", "stage", "prod"]
T_WAREHOUSE = str
T_SCHEMA = str
T_ENG_FACTORY = Callable[[T_ENV, T_WAREHOUSE, T_SCHEMA], Engine]
T_RESULT_TYPE = Literal["rows", "scalar", "dataframe"]


class SnowflakeQueryTask(Task):
    """
    Task that handles executing a query against Snowflake (using CLO semantics)
    """

    def __init__(
        self,
        engine_factory: T_ENG_FACTORY,
        env: Optional[T_ENV] = None,
        warehouse: Optional[T_WAREHOUSE] = None,
        schema: Optional[T_SCHEMA] = None,
        result_type: T_RESULT_TYPE = None,
    ):
        """
        Parameters
        ----------
        engine_factory: T_ENG_FACTORY
            A function that returns an SQLAlchemy Engine. It should have the following signature:
            ``engine_factory(env: T_ENV, warehouse: T_WAREHOUSE, schema: T_SCHEMA) -> Engine``
        env : T_ENV
            The environment to connect to
        warehouse : T_WAREHOUSE
            The warehouse to connect to
        schema : T_SCHEMA
            The schema to connect to
        result_type: T_RESULT_TYPE
            The type of result to return. If not provided, the result will be a list of ``Row`` objects.

        """
        super().__init__()
        self.engine_factory = engine_factory
        self.env = env
        self.warehouse = warehouse
        self.schema = schema
        self.result_type = result_type

    @defaults_from_attrs("env", "warehouse", "schema", "result_type")
    def run(
        self,
        query: Any,
        env: Optional[T_ENV] = None,
        warehouse: Optional[T_WAREHOUSE] = None,
        schema: Optional[T_SCHEMA] = None,
        result_type: Optional[T_RESULT_TYPE] = None,
    ):
        """
        Execute a query against Snowflake
        Parameters
        ----------
        query : Any
            The query to execute. This can be a string or a SQLAlchemy query object.
        env : Optional[T_ENV]
            The environment to pass to the engine factory. If not provided, the value from the constructor will be used.
        warehouse : Optional[T_WAREHOUSE]
            The warehouse to pass to the engine factory. If not provided, the value from the constructor will be used.
        schema : Optional[T_SCHEMA]
            The schema to pass to the engine factory. If not provided, the value from the constructor will be used.
        result_type : Optional[T_RESULT_TYPE]
            The type of result to return. If not provided, the value from the constructor will be used.

        Returns
        -------
        list[Row] | Any | pd.DataFrame
            The result of the query. The type of the result depends on the ``result_type`` parameter.

        Notes
        -----
        To include parameters in the query, use the ``bindparams`` method on the query object.
        """
        engine = self.engine_factory(env, warehouse, schema)

        if result_type not in ("rows", "scalar", "dataframe"):
            raise ValueError(
                f"Invalid result_type: {result_type}. Must be one of 'rows', 'scalar', 'dataframe'"
            )

        with engine.begin() as conn:
            if result_type == "dataframe":
                return pd.read_sql(query, conn)
            elif result_type == "scalar":
                return conn.execute(query).scalar()
            else:
                return conn.execute(query).fetchall()
