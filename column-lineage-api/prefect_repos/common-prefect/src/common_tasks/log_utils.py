import logging
import re
import sys
from functools import wraps


class SnowflakeQueryFilterLegacy(logging.Filter):
    """
    Filter for snowflake query logs. Written as class to make checks for if this is already added to a logger easier.

    This filter is intended to work with snowflake-connector python v3.7.1 and below.

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
        return True


class SnowflakeQueryFilter:
    """
    Filter for snowflake query logs. Written as class to make checks for if this is already added to a logger easier.

    This filter is intended to work with snowflake-connector python v3.7.1 and below.

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


def log_queries(func):
    
    """Log Snowflake queries. To ensure the full query is logged, when creating the engine
    pass  "log_max_query_length": 10_000, (or some other value) to the connect_args parameter of the create_engine function.
    """
    
    @wraps(func)
    def wrapped_func(*args, **kwargs):
        try:
            from snowflake.connector import VERSION as snowflake_version
            
            major, minor, *rest = snowflake_version
            legacy_logging = major <= 3 and minor <= 7
        except ImportError:
            legacy_logging = False
        stream_handler = logging.StreamHandler(stream=sys.stdout)
        sf_logger = logging.getLogger("snowflake.connector.cursor")
        sf_logger.setLevel(logging.DEBUG if not legacy_logging else logging.INFO)
        sf_filter = (
            SnowflakeQueryFilterLegacy() if legacy_logging else SnowflakeQueryFilter()
        )
        if not sf_logger.filters:
            sf_logger.addFilter(sf_filter)
        if not sf_logger.handlers:
            sf_logger.addHandler(stream_handler)
        return func(*args, **kwargs)

    return wrapped_func



