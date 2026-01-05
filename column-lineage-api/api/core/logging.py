"""Logging configuration."""

import logging
import sys
from typing import Any, Dict

import structlog
from rich.console import Console
from rich.logging import RichHandler

from api.core.config import get_settings


def setup_logging() -> None:
    """Setup structured logging configuration."""
    settings = get_settings()
    
    # Configure standard library logging
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL.upper()),
        format="%(message)s",
        datefmt="[%X]",
        handlers=[
            RichHandler(
                console=Console(stderr=True),
                show_time=True,
                show_path=True,
                markup=True,
                rich_tracebacks=True,
            )
        ] if settings.LOG_FORMAT != "json" else [],
    )
    
    # Configure structlog
    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
    ]
    
    if settings.LOG_FORMAT == "json":
        processors.extend([
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ])
    else:
        processors.extend([
            structlog.dev.ConsoleRenderer(colors=True),
        ])
    
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, settings.LOG_LEVEL.upper())
        ),
        logger_factory=structlog.WriteLoggerFactory(sys.stderr),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.BoundLogger:
    """Get a structured logger instance."""
    return structlog.get_logger(name)


class LoggerMixin:
    """Mixin class to add logging capabilities."""
    
    @property
    def logger(self) -> structlog.BoundLogger:
        """Get logger instance for this class."""
        return get_logger(self.__class__.__name__)