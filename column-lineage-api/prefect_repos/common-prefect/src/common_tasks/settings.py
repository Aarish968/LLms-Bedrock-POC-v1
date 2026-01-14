from __future__ import annotations

import json
import os
import platform
from enum import Enum
from typing import Any, Literal, Optional, Union, cast

import boto3


class Env(str, Enum):
    dev = "dev"
    prod = "prod"
    
    def __str__(self) -> str:
        return str.__str__(self)


TEnv = Literal["dev", "prod"]


class Warehouse(str, Enum):
    x_small = "cps_dsci_etl_ext1_wh"
    small = "cps_dsci_etl_ext2_wh"
    medium = "cps_dsci_etl_wh"
    large = "cps_dsci_etl3_wh"
    
    def __str__(self) -> str:
        return str.__str__(self)
    
TWarehouse = Literal["cps_dsci_etl_ext1_wh", "cps_dsci_etl_ext2_wh", "cps_dsci_etl_wh", "cps_dsci_etl3_wh"]


def get_boto_session(**kwargs):
    session = boto3.session.Session(**kwargs)
    return session


def get_secret(secret_name: str):
    from common_tasks.aws_tasks import get_boto3_session
    session = get_boto3_session()
    client = session.client("secretsmanager")
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    secret = get_secret_value_response["SecretString"]
    return json.loads(secret)


class SettingsBase:
    
    """
    Base class for settings. It implements frequently used methods for determining
    database schemas, connection types, and warehouse names.
    """
    
    def __init__(
        self,
        env: Union[Env, TEnv],
        warehouse: Warehouse = Warehouse.medium,
    ):
        self.env = self._get_env(env)
        self.db_schema = self._get_db_schema()
        self.warehouse = self._get_warehouse(warehouse)
        self._db_url_template = self._get_db_url_template()
    
    @classmethod
    def _is_running_locally(cls) -> bool:
        # Windows or Mac ? Then Yes
        return platform.system() in ("Windows", "Darwin")
        
    def _get_env(self, env: Any) -> TEnv:
        env = str(env)
        if env in {"dev", "prod"}:
            return cast(TEnv, env)
        raise ValueError(f"Invalid env: {env!r}")
    
    def _get_warehouse(self, warehouse: Union[Warehouse, str]) -> Warehouse:
        warehouse = str(warehouse)
        if warehouse.lower() in {
            "cps_dsci_etl_ext1_wh",
            "cps_dsci_etl_ext2_wh",
            "cps_dsci_etl_wh",
        }:
            return cast(Warehouse, warehouse)
        raise ValueError(f"Invalid warehouse: {warehouse!r}")
    
    
    def _get_db_schema(self):
        if self.env == "prod":
            return "CPS_DSCI_API"
        elif self.env == "dev":
            return "CPS_DSCI_BR"
        else:
            raise ValueError(f"Invalid env: {self.env!s}")
    
    def _get_db_secret_name(self):
        if self._is_running_locally():
            print("Running locally - using local connection string")
            return "prd_cps_dsci_etl_svc_local_conn_str"
        else:
            print("Running in cloud - using cloud connection string")
            return "prd_cps_dsci_etl_svc_cloud_conn_str"
    
    def _get_db_url_template(self):
        secret_name = self._get_db_secret_name()
        db_secret = get_secret(secret_name)[secret_name]
        return db_secret
    
    @property
    def db_url(self):
        return self._db_url_template.format(schema=self.db_schema, wh=self.warehouse)
    
    def get_db_url(
        self, warehouse: Optional[Warehouse] = None, schema: Optional[str] = None
    ):
        warehouse = warehouse or self.warehouse
        schema = schema or self.db_schema
        return self._db_url_template.format(schema=schema, wh=str(warehouse))
    
    

__all__ = ["Env", "TEnv", "Warehouse", "SettingsBase", "TWarehouse"]