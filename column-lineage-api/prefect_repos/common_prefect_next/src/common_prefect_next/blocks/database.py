from enum import Enum
from typing import Literal

TSchema = Literal["CPS_DSCI_API", "CPS_DSCI_BR"]
TWarehouse = Literal[
    "cps_dsci_etl_ext1_wh",
    "cps_dsci_etl_ext2_wh",
    "cps_dsci_etl_wh",
    "cps_dsci_etl_ext3_wh",
    "cps_dsci_etl_ext4_wh",
    "cps_dsci_etl_ext5_wh",
]


class Warehouse(str, Enum):
    x_small = "cps_dsci_etl_ext1_wh"
    small = "cps_dsci_etl_ext2_wh"
    medium = "cps_dsci_etl_wh"
    large = "cps_dsci_etl_ext3_wh"
    x_large = "cps_dsci_etl_ext4_wh"
    huge = "cps_dsci_etl_ext5_wh"

    def __str__(self) -> str:
        return str.__str__(self)


class DbSchemas(str, Enum):
    prod = "CPS_DSCI_API"
    dev = "CPS_DSCI_BR"

    def __str__(self) -> str:
        return str.__str__(self)
