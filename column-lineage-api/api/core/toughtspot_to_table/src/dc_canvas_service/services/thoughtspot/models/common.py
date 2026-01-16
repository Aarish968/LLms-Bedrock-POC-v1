from typing import Literal, TypedDict

from pydantic import BaseModel
from thoughtspot_tml.types import TMLType

from ..enums import TSActionContextE

TSExportMetadataType = Literal[
    "LIVEBOARD",
    "ANSWER",
    "LOGICAL_TABLE",
    "CONNECTION",
    "CUSTOM_ACTION",
    "USER",
    "USER_GROUP",
    "ROLE",
]

TSShareModeType = Literal["NO_ACCESS", "MODIFY", "READ_ONLY"]

TSShareMetadataType = Literal["LIVEBOARD", "ANSWER", "LOGICAL_TABLE", "LOGICAL_COLUMN"]

TSUserMetadataType = Literal[
    "LIVEBOARD", "ANSWER", "LOGICAL_TABLE", "LOGICAL_COLUMN", "CONNECTION"
]


class TSSearchPayload(TypedDict):
    type: TSExportMetadataType | None
    identifier: str | None
    name_pattern: str | None


class TSExportPayload(TypedDict):
    type: TSExportMetadataType
    identifier: str


class TSMetadataUserSearchPayload(TypedDict):
    type: TSUserMetadataType
    identifier: str


class TSUserSearchPayload(TypedDict):
    name_pattern: str


class TSIdentity(BaseModel):
    id: str | None = None
    name: str | None = None
    fqn: str | None = None


class TSActionObjAssociation(BaseModel):
    action_id: str | None = None
    action_name: str | None = None
    context: TSActionContextE | None = None
    enabled: bool | None = None


class TSDateFilterRange(BaseModel):
    start_date: str | None = None
    end_date: str | None = None


class TSDateFilter(BaseModel):
    type: str | None = None
    number: int | None = None
    date: str | None = None
    oper: str | None = None
    date_range: TSDateFilterRange | None = None
    date_period: str | None = None
    for_each_period: str | None = None
    year: int | None = None
    quarter: str | None = None
    month: str | None = None
    week_day: str | None = None
    year_name: str | None = None
    quarter_name: str | None = None
    month_name: str | None = None
    week_day_name: str | None = None


class TSFilter(BaseModel):
    column: list[str] | None = None
    oper: str | None = None
    values: list[str] | None = None
    excluded_visualizations: list[str] | None = None
    is_mandatory: bool | None = None
    date_filter: TSDateFilter | None = None
    is_single_value: bool | None = None
    display_name: str | None = None


class TSJoin(BaseModel):
    id: str | None = None
    name: str | None = None
    source: str | None = None
    destination: str | None = None
    on: list[str] | None = None
    type: str | None = None
    is_one_to_one: bool | None = None


class TSRelation(BaseModel):
    name: str | None = None
    description: str | None = None
    source: TSIdentity | None = None
    destination: TSIdentity | None = None
    on: str | None = None
    type: str | None = None
    is_one_to_one: bool | None = None


class TSFormulaProperties(BaseModel):
    column_type: str | None = None
    aggregation: str | None = None


class TSFormula(BaseModel):
    id: str | None = None
    name: str | None = None
    expr: str | None = None
    properties: TSFormulaProperties | None = None
    was_auto_generated: bool | None = None


class TSTablePathJoin(BaseModel):
    join: list[str] | None = None


class TSTablePath(BaseModel):
    id: str | None = None
    table: str | None = None
    join_path: list[TSTablePathJoin] | None = None
    column: list[str] | None = None
