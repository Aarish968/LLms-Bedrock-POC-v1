from pydantic import BaseModel, Field

from .column import TSColumn
from .common import TSIdentity, TSRelation
from .parameter import TSParameter
from .rule import TSRLSRule


class TSTable(BaseModel):
    name: str
    description: str | None = None
    db: str
    db_schema: str = Field(serialization_alias="schema")
    db_table: str
    connection: TSIdentity
    columns: list[TSColumn] | None = None
    rls_rules: TSRLSRule | None = None
    joins_with: list[TSRelation] | None = None
    parameters: list[TSParameter] | None = None
