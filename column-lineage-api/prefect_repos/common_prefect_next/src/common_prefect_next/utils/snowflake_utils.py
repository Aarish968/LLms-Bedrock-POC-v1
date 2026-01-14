import json
import logging
from datetime import date, datetime
from zoneinfo import ZoneInfo

from sqlalchemy import VARCHAR, TypeDecorator

UTC = ZoneInfo("UTC")

logger = logging.getLogger(__name__)


def isoformat_utc(o: datetime) -> str:
    """
    If a python datetime is naive, assume it is UTC.
    Can be used with pydantic's json_encoders.

    2021-09-01T12:30:00 -> 2021-09-01T12:30:00+00:00

    Example:
        class Config:
            json_encoders = {datetime: isoformat_utc}

    Note that .replace is used rather than ``.astimezone`` so that the timezone is always UTC.
    """
    if o.tzinfo is None:
        return o.replace(tzinfo=UTC).isoformat()
    return o.isoformat()


class DateCompatibleEncorder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return isoformat_utc(obj)
        return json.JSONEncoder.default(self, obj)


class JSONVarchar(TypeDecorator):
    impl = VARCHAR
    sf_size_limit = 16_777_216

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        try:
            return json.loads(value)
        except Exception:
            logger.exception(f"Row contains invalid JSON: {value}, returning None")
            return None

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if isinstance(value, str):
            return value
        try:
            result = json.dumps(
                value, separators=(",", ":"), cls=DateCompatibleEncorder
            )
            if len(result) >= self.sf_size_limit:
                logger.warning(
                    "JSON size exceeds Snowflake limit of %d bytes", self.sf_size_limit
                )
        except (TypeError, ValueError):
            logger.exception(f"Invalid JSON: {value}")
            return None
        else:
            return result
