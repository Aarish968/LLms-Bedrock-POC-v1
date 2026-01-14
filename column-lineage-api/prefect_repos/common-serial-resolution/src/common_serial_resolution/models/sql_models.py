from typing import Annotated, Literal

from pydantic import Field, TypeAdapter

from . import Model, TableName


class SerialResolutionProcedureParams(Model):
    request_id: int
    dc_engagement_id: int
    cisco_cco_id: str
    comment: str = Field(default="", serialization_alias="comments")
    ddl_action: str = Field(default="set")
    snowflake_uri: str


class SerialResolutionProcedureSuccessResponse(Model):
    success: Literal[True]
    message: str
    code: int
    logs: list[str] = Field(default_factory=list)
    resolved_ranked_temp_tbl: TableName
    resolved_temp_tbl: TableName


class SerialResolutionProcedureFailureResponse(Model):
    success: Literal[False]
    message: str
    code: int
    logs: list[str] = Field(default_factory=list)


TSerialResolutionResponse = Annotated[
    SerialResolutionProcedureFailureResponse | SerialResolutionProcedureSuccessResponse,
    Field(discriminator="success"),
]
SerialResolutionResponse = TypeAdapter(TSerialResolutionResponse)
