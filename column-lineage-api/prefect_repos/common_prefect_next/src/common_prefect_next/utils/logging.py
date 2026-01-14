import functools
import logging
from typing import Callable, ParamSpec, TypeVar

P = ParamSpec("P")
R = TypeVar("R")


def setup_extra_loggers(func: Callable[[P], R]) -> Callable[[P], R]:
    """
    Using PREFECT_LOGGING_EXTRA_LOGGERS setting, set the log level to PREFECT_LOGGING_LEVEL
    for the specified loggers.
    """

    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        from prefect.settings import (
            PREFECT_LOGGING_EXTRA_LOGGERS,
            PREFECT_LOGGING_LEVEL,
        )

        try:
            level = PREFECT_LOGGING_LEVEL.value()
        except Exception:
            level = logging.INFO

        for logger_name in PREFECT_LOGGING_EXTRA_LOGGERS.value():
            logger = logging.getLogger(logger_name)
            logger.setLevel(level)

        return func(*args, **kwargs)

    return wrapper
