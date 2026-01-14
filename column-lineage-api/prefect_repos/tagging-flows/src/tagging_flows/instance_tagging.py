from typing import TYPE_CHECKING

from common_prefect_next.blocks.aws import S3StageFileUri
from common_prefect_next.blocks.database import TWarehouse

from tagging_flows.common.models import (
    DdlAction,
    StoredProcedureResult,
    TagInstancesProcedureParams,
)
from tagging_flows.common.models.enums import StoredProcedureNames
from tagging_flows.common.models.procedures import UntagInstancesProcedureParams
from tagging_flows.common.queries import run_stored_procedure

if TYPE_CHECKING:
    from sqlalchemy import Connection


def run_instance_tagging(
    conn: "Connection",
    sp_params: TagInstancesProcedureParams | UntagInstancesProcedureParams,
    warehouse: TWarehouse,
) -> "StoredProcedureResult":
    return run_stored_procedure(
        proc_name=StoredProcedureNames.tag_instances,
        params=sp_params,
        conn=conn,
        warehouse=warehouse,
    )
