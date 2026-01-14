import logging
import os
import platform
from pathlib import Path
from typing import TYPE_CHECKING, Literal, TypedDict

from prefect.variables import Variable
from typing_extensions import TypeGuard, Unpack

from common_prefect_next.blocks.database import TWarehouse, Warehouse
from common_prefect_next.blocks.environment import Env, TEnv

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


logger = logging.getLogger(__name__)


def _determine_environment() -> Literal["local", "cloud"]:
    # This is a simple way to determine if we are running in a local or cloud environment
    # Windows or Mac ? Then Yes
    if platform.system() in ("Windows", "Darwin"):
        return "local"
    if Path("./dockerenv").exists() or os.getenv("KUBERNETES_SERVICE_HOST") is not None:
        return "cloud"
    return "local"


TConnectionTypeParam = Literal["local", "cloud", "auto"]
TConnectionType = Literal["local", "cloud"]


def _is_connection_type(connection_type: str) -> TypeGuard[TConnectionType]:
    return connection_type in {"local", "cloud"}


def _is_connection_type_param(connection_type: str) -> TypeGuard[TConnectionTypeParam]:
    return connection_type in {"local", "cloud", "auto"}


def get_db_url_template(connection_type: TConnectionTypeParam = "auto") -> str:
    """
    Retrieve a database connection string suitable for templating. The template should have two placeholders:
    - {schema} - the schema name
    - {wh} - the warehouse name
    """
    connection_type = (
        connection_type if connection_type != "auto" else _determine_environment()
    )

    if connection_type == "local":
        from common_prefect_next.blocks.aws import get_local_connection

        return get_local_connection()
    elif connection_type == "cloud":
        from common_prefect_next.blocks.aws import get_cloud_connection

        return get_cloud_connection()
    else:
        msg = f'Connection type {connection_type} not supported. Please use "local" or "cloud"'
        raise ValueError(msg)


def get_db_schema(env: TEnv | Env, schema: str | None) -> str:
    """Retrieve the env:schema mapping"""
    if schema:
        return schema
    schemas = Variable.get("db_schemas")
    return schemas[str(env).lower()]


def _get_warehouse(warehouse: str | Warehouse | TWarehouse) -> str:
    """If warehouse is not a member of Warehouse, load from variables"""

    match warehouse:
        case str(warehouse_name) if warehouse_name.lower() in Warehouse._member_names_:
            return str(Warehouse[warehouse_name.lower()])
        case str(warehouse_name) if (
            warehouse_name.lower() in Warehouse._value2member_map_
        ):
            return warehouse_name.lower()
        case str(warehouse_name):
            warehouses = Variable.get("warehouses")
            matched = warehouses.get(warehouse_name)
            if matched:
                return matched
            logger.warning(
                f"Unknown warehouse {warehouse_name}. Using default {Warehouse.small}"
            )
            return str(Warehouse.small)
        case Warehouse(warehouse):
            return str(warehouse)
        case _:
            logger.warning(
                f"Unknown warehouse {warehouse}. Using default {Warehouse.small}"
            )
            return str(Warehouse.small)


class GetDbUrlKwargs(TypedDict, total=False):
    schema: str
    connection_type: TConnectionTypeParam


def get_db_url(
    env: TEnv | Env,
    warehouse: str | Warehouse | TWarehouse,
    **kwargs: Unpack[GetDbUrlKwargs],
) -> str:
    connection_type = kwargs.get("connection_type", "auto")
    connection_type_parsed: TConnectionTypeParam = (
        connection_type if _is_connection_type_param(connection_type) else "auto"
    )
    schema = kwargs.get("schema")
    db_url_template = get_db_url_template(connection_type=connection_type_parsed)
    try:
        db_schema = get_db_schema(env, schema)
    except KeyError as e:
        msg = f"Schema {schema} not found in db_schemas"
        raise ValueError(msg) from e
    db_warehouse = _get_warehouse(warehouse)
    return db_url_template.format(schema=db_schema, wh=db_warehouse)


def get_engine(db_url: str, **session_kwargs: Unpack[dict]) -> "Engine":
    from sqlalchemy import create_engine

    session_parameters = {"abort_detached_query": True, **session_kwargs}
    return create_engine(
        db_url,
        connect_args={
            "log_max_query_length": 10_000,
            "session_parameters": session_parameters,
            "disable_ocsp_checks": True,
            "insecure_mode": True,
        },
    )
