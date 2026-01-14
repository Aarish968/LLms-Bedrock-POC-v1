import functools
import importlib.metadata
import logging
from typing import Callable, ParamSpec, TypeVar

P1 = ParamSpec("P1")
R1 = TypeVar("R1")


def log_versions(
    packages: set[str],
) -> Callable[[Callable[[P1], R1]], Callable[[P1], R1]]:
    """
    Decorator that logs the version of the packages in the provided set
    """

    def log_packages_dec(func: Callable[[P1], R1]) -> Callable[[P1], R1]:
        @functools.wraps(func)
        def wrapper(*args: P1.args, **kwargs: P1.kwargs) -> R1:
            logger = logging.getLogger(__name__)

            def get_version(package_name: str) -> str:
                try:
                    return importlib.metadata.version(package_name)
                except Exception:
                    logger.exception("Error getting version for %s", package_name)
                    return "Not Found"

            if packages:
                for package in packages:
                    msg = f"Package: {package}, Version: {get_version(package)}"
                    logger.info(msg)

            return func(*args, **kwargs)

        return wrapper

    return log_packages_dec
