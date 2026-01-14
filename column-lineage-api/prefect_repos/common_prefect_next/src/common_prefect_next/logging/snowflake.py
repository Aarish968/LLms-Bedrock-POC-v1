import logging
import re


class SnowflakeQueryFilter(logging.Filter):
    """
    Filter for snowflake query logs. Written as class to make checks for if this is already added to a logger easier.

    This filter is intended to work with snowflake-connector python v3.7.1 and above.

    Notes
    -----
    By default, this filter will filter out the following lines:
    - Rollback and commit statements
    - Desc table statements, which are used to get table metadata
    - Current database and current schema statements, which are used to get the current database and schema
    """

    substr_ignore_pattern = re.compile(
        r"""
                    ^# Matches start of string
                    (?:rollback|commit|desc\stable)  # Matches 'rollback', 'commit', 'desc table' at start.
                    |                                 # or
                    (?:current_database|current_schema) # Matches 'current_database', 'current_schema'.
                    |
                    (?:number\sof\sresults) # Matches 'Number of results in first chunk'.
                    """,
        re.IGNORECASE | re.VERBOSE,
    )

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Filter for snowflake query logs. Hardcoded parameters for now, but could be made more flexible in __init__.
        """
        if not isinstance(record.msg, str):
            return False
        if not record.msg.startswith("running query"):
            return False
        record_args = record.args
        try:
            query = record_args[0]
        except (IndexError, TypeError):
            return False
        if not isinstance(query, str):
            return False
        if self.substr_ignore_pattern.search(query):
            return False
        record.msg = "%s"
        record.levelno = logging.INFO
        record.levelname = "INFO"
        return True
