from . import Model, TableName


class TableNames(Model):
    """
    loader_table: We simply place serial_numbers into this table
    resolved_table: Stored procedure will create this table
    """

    resolved_table: TableName
    ranked_table: TableName
    engagement_tags_table: TableName

    @classmethod
    def from_params(cls, *, request_id: int, dc_engagement_id: int) -> "TableNames":
        """
        Create table names from request_id and dc_engagement_id.
        """
        return cls(
            resolved_table=TableName(
                f"sn_serial_resolved_{request_id}_tmp",
            ),
            ranked_table=TableName(
                f"sn_serial_ranked_{request_id}_tmp",
            ),
            engagement_tags_table=TableName(
                f"dc_engagement_tags_{dc_engagement_id}",
            ),
        )


__all__ = ["TableNames"]
