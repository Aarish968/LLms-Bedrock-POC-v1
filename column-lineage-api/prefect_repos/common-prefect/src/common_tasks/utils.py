import io
import logging
import os
import re
import sys
import threading
from functools import wraps
from pathlib import Path
from typing import TYPE_CHECKING, Optional
from urllib.parse import urlparse

import boto3
from botocore.client import BaseClient

if TYPE_CHECKING:
    from prefect.storage import Docker


def parse_s3_uri(uri: str) -> 'tuple[str, str]':
    """Parse an S3 URI into its bucket and key components"""
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError(f"URI {uri} is not an S3 URI")
    return parsed.netloc, parsed.path.lstrip("/")

def download_from_s3(bucket, key, client: Optional["BaseClient"] = None) -> bytes:
    # Read an object into memory
    client = client or boto3.session.Session().client("s3")
    buffer = io.BytesIO()
    client.download_fileobj(bucket, key, buffer)
    return buffer.getvalue()



class UploadProgressCallback:
    def __init__(self, file_path, logger: Optional[logging.Logger] = None):
        self._file_path = file_path
        self._file_name = os.path.basename(file_path)
        self._size = float(os.path.getsize(file_path))
        self._seen_so_far = 0
        self._lock = threading.Lock()
        self._logger = logger or sys.stdout
        self._has_logger = isinstance(self._logger, logging.Logger)
        self._last_pct = 0
        self._write(
            f"Uploading {self._file_name} [{self._human_readable_size(self._size)}]..."
        )

    @staticmethod
    def _human_readable_size(size: float) -> str:
        for unit in ["B", "KB", "MB", "GB"]:
            if size < 1024.0:
                break
            size /= 1024.0
        return f"{size:.2f} {unit}"

    def _write(self, message: str):
        if self._has_logger:
            self._logger.info(message)
        else:
            self._logger.write(message)
            self._logger.flush()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            pct = (self._seen_so_far / self._size) * 100
            if pct - self._last_pct > 20:
                self._write(
                    f"Uploading: {self._file_name} {self._human_readable_size(self._seen_so_far)} /"
                    f" {self._human_readable_size(self._size)}"
                )
                self._last_pct = pct


class SnowflakeQueryFilter(logging.Filter):
    """
    Filter for snowflake query logs. Written as class to make checks for if this is already added to a logger easier.
    """

    substr_pattern = re.compile(
        "^(?:rollback|commit|desc table)|(?:current_database|current_schema)",
        re.IGNORECASE,
    )

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Filter for snowflake query logs. Hardcoded parameters for now, but could be made more flexible in __init__.
        """
        if not record.msg.startswith("query:"):
            return False
        record_args = record.args
        try:
            query = record_args[0]
        except (IndexError, TypeError):
            return False
        if not isinstance(query, str):
            return False
        if self.substr_pattern.search(query):
            return False
        record.msg = "query:\n%s"
        return True


def log_queries(func):
    """
    Decorator to add snowflake query logging to a function.

    Parameters
    ----------
    func

    Notes
    -----
    This decorator is intended to be used in conjunction with the prefect task decorator.
    Additionally, in order to log more than 80 characters of a query, when creating the engine,
    pass the ``log_max_query_length`` parameter to the ``connect_args`` parameter of the ``create_engine`` function.

    Examples
    -------
    @task(log_stdout=True)   # Note that log_stdout=True is required for this to work
    @log_queries

    """

    @wraps(func)
    def wrapped_func(*args, **kwargs):
        stream_handler = logging.StreamHandler(sys.stdout)
        sf_logger = logging.getLogger("snowflake.connector.cursor")
        sf_logger.setLevel(logging.INFO)
        if not sf_logger.filters:
            sf_logger.addFilter(SnowflakeQueryFilter())
        if not sf_logger.handlers:
            sf_logger.addHandler(stream_handler)
        return func(*args, **kwargs)

    return wrapped_func


def add_wheels(s_obj: 'Docker'):
    """
    When building the Docker Image using CodeBuild, are private packages are built as wheels. This allows us to
    take advantage of the git credential helper to pull the private packages from our private git repo(s).

    When Prefect builds the image, ``git`` is not available, so we need to copy the wheels into the image.

    Modify the Docker Storage object to include the wheels.

    Parameters
    ----------
    s_obj : Docker

    Returns
    -------
    Docker
    """

    cwd = Path.cwd() / "wheels"
    tgt = Path("/root/.prefect/wheels")
    mapping = {}
    whl_command = "RUN pip install "
    for wheel in cwd.rglob("**/*.whl"):
        source_abs = str(wheel.resolve())
        target_abs = str((tgt / wheel.name).resolve())
        mapping[source_abs] = target_abs
        whl_command += f"{target_abs} "
        
    if not mapping:
        return s_obj

    if s_obj.files is None:
        s_obj.files = mapping
    else:
        s_obj.files.update(mapping)
    if s_obj.extra_dockerfile_commands is None:
        s_obj.extra_dockerfile_commands = [whl_command]
    else:
        s_obj.extra_dockerfile_commands.append(whl_command)
    return s_obj
