from pydantic import BaseModel

from .common import TSIdentity, TSJoin, TSTablePath


class TSRule(BaseModel):
    name: str | None = None
    expr: str | None = None


class TSRLSRule(BaseModel):
    tables: list[TSIdentity] | None = None
    joins: list[TSJoin] | None = None
    table_paths: list[TSTablePath] | None = None
    rules: list[TSRule] | None = None
    table: TSIdentity | None = None
