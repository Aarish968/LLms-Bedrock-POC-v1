import datetime
import json
from json import JSONEncoder
from typing import Any

from typing_extensions import Unpack


def isoformat_utc(o: datetime.datetime) -> str:
    """
    If a python datetime is naive, assume it is UTC.
    Can be used with Pydantic's json_encoders.

    2021-09-01T12:30:00 -> 2021-09-01T12:30:00+00:00

    Example:
        class Config:
            json_encoders = {datetime: isoformat_utc}

    Note that .replace is used rather than ``.astimezone`` so that the timezone is always UTC.
    """
    if o.tzinfo is None:
        return o.replace(tzinfo=datetime.UTC).isoformat()
    return o.isoformat()


class CustomJSONEncoder(JSONEncoder):
    """Handle encoding of datetime objects"""

    def __init__(self, **kwargs: Unpack[dict]):
        kwargs["separators"] = (",", ":")
        super().__init__(**kwargs)

    def default(self, o: Any) -> Any:
        if isinstance(o, datetime.datetime):
            return isoformat_utc(o)
        return super().default(o)


def json_dumps(obj: Any) -> str:
    """Convenience function to dump an object to a JSON string with the CustomJSONEncoder"""
    return json.dumps(obj, cls=CustomJSONEncoder)


__all__ = ["CustomJSONEncoder", "isoformat_utc", "json_dumps"]
