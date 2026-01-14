from typing import TYPE_CHECKING

from sqlalchemy import Connection, Integer, String, TextClause, TextualSelect, text

if TYPE_CHECKING:
    from common_serial_resolution.models import TableName


def _make_get_resolved_serials_stmt(resolved_table: str) -> "TextualSelect":
    """
    Returns the SQL statement to get the resolved serial numbers.
    """
    return (
        text(
            """
        SELECT SERIAL_NUMBER, INSTANCE_ID
        FROM IDENTIFIER(:resolved_table)
        WHERE SERIAL_NUMBER IS NOT NULL AND INSTANCE_ID IS NOT NULL
        """
        )
        .bindparams(resolved_table=resolved_table)
        .columns(serial_number=String, instance_id=Integer)
    )


def get_resolved_serials(
    resolved_table: "TableName", conn: "Connection"
) -> dict[str, int]:
    """
    Returns a mapping of Serial Numbers to Instance IDs that were resolved.
    """
    stmt = _make_get_resolved_serials_stmt(resolved_table=str(resolved_table))

    results = conn.execute(stmt).mappings().all()

    return {result["serial_number"]: result["instance_id"] for result in results}


__all__ = ["get_resolved_serials"]
