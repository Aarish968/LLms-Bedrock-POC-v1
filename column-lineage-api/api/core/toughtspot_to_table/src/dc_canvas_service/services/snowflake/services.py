import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING, Callable, Any, Generator

from sqlalchemy import Engine, create_engine, text, Connection

from dc_canvas_service.services.snowflake.exceptions import DBException

if TYPE_CHECKING:
    from dc_canvas_service.common import Settings

logger = logging.getLogger(__name__)


class SnowflakeService:
    """
    Snowflake Service is the service class for interactions with Snowflake Database.
    """

    def __init__(
        self,
        settings: "Settings",
        get_engine: Callable[[], Engine] | None = None,
        **session_params,  # noqa: ANN003
    ):
        self.settings = settings
        self.session_params = session_params
        self.get_engine = get_engine
        self.engine = None
        self._schema_validated = False

    def _get_engine(self) -> Engine:
        """
        Get SQL Alchemy engine for Snowflake."""
        db_url = self.settings.get_sf_url()
        session_parameters = {"abort_detached_query": True, **self.session_params}

        return create_engine(
            db_url,
            connect_args={
                "log_max_query_length": 10_000,
                "session_parameters": session_parameters,
                "disable_ocsp_checks": True,
            },
        )

    def _validate_engine_schema(self) -> None:
        """Validate that engine's schema matches settings.sf_schema (only when using custom get_engine)."""
        if self._schema_validated or not self.get_engine:
            return

        with self.engine.connect() as conn:
            current_schema = conn.execute(text("SELECT CURRENT_SCHEMA()")).scalar()
            expected_schema = str(self.settings.sf_schema)
            assert current_schema == expected_schema, (
                f"Schema mismatch: engine uses '{current_schema}' but settings expects '{expected_schema}'"
            )

        self._schema_validated = True
        logger.info(f"Validated engine schema: {expected_schema}")

    @contextmanager
    def conn_transaction(self) -> Generator[Connection, Any, None]:
        """
        Generates a context manager for a connection that controls a db transaction.
        Eventually, COMMIT or ROLLBACK.

        Yields: a session object.
        """
        if self.engine is None:
            get_engine = self.get_engine or self._get_engine
            self.engine = get_engine()
            self._validate_engine_schema()

        transaction = conn = None

        try:
            conn = self.engine.connect()
            transaction = conn.begin()
            yield conn
        except Exception:
            if transaction:
                logger.exception("Rolling back due to an error")
                transaction.rollback()
            raise
        else:
            if transaction:
                transaction.commit()
        finally:
            if conn:
                conn.close()
