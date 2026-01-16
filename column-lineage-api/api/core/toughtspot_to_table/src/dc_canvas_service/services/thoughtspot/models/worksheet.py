from pydantic import BaseModel, Field

from .column import TSColumnProperties
from .common import (
    TSActionObjAssociation,
    TSFilter,
    TSFormula,
    TSIdentity,
    TSJoin,
    TSRelation,
    TSTablePath,
)
from .parameter import TSParameter


class TSLessonPlan(BaseModel):
    lesson_id: int | None = None
    lesson_plan_string: str | None = None


class TSSchemaInPlaceJoin(BaseModel):
    with_: str | None = None
    referencing_join: str | None = None
    on: str | None = None
    type: str | None = None
    cardinality: str | None = None


class TSSchemaTable(BaseModel):
    name: str | None = None
    alias: str | None = None
    fqn: str | None = None
    joins: list[TSSchemaInPlaceJoin] | None = None


class TSSchema(BaseModel):
    tables: list[TSSchemaTable] | None = None


class TSWorksheetColumn(BaseModel):
    name: str | None = None
    description: str | None = None
    column_id: str | None = None
    formula_id: str | None = None
    properties: TSColumnProperties | None = None


class TSWorksheetQueryProperties(BaseModel):
    is_bypass_rls: bool | None = None
    join_progressive: bool | None = None


class TSWorksheetUseCase(BaseModel):
    name: str | None = None
    display_name: str | None = None
    description: str | None = None


class TSWorksheet(BaseModel):
    name: str
    description: str | None = None
    tables: list[TSIdentity]
    joins: list[TSJoin] | None = None
    db_schema: TSSchema | None = Field(serialization_alias="schema", default=None)
    model_tables: list[TSSchemaTable] | None = None
    table_paths: list[TSTablePath] | None = None
    formulas: list[TSFormula] | None = None
    filters: list[TSFilter] | None = None
    worksheet_columns: list[TSWorksheetColumn] | None = None
    columns: list[TSWorksheetColumn] | None = None
    properties: TSWorksheetQueryProperties | None = None
    joins_with: list[TSRelation] | None = None
    generation_type: str | None = None
    lesson_plans: list[TSLessonPlan] | None = None
    parameters: list[TSParameter] | None = None
    action_object_associations: list[TSActionObjAssociation] | None = None
    use_cases: list[TSWorksheetUseCase] | None = None
