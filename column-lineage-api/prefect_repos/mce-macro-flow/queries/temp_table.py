from typing import Literal

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import Executable, ClauseElement

T_TABLE_TYPE = Literal["temporary", "transient"]


class TempTableFromSelect(Executable, ClauseElement):
    inherit_cache = False

    def __init__(self, table, select, table_type: T_TABLE_TYPE = "transient"):
        self.table = table
        self.select = select
        self.table_type = table_type


@compiles(TempTableFromSelect)
def visit_insert_table_from_select(element, compiler, **kw):
    return (
        f"CREATE OR REPLACE {element.table_type} TABLE {compiler.process(element.table, asfrom=True)}"
        f" AS {compiler.process(element.select)}"
    )
