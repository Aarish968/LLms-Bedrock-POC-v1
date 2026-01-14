import io
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, TypeVar
from urllib.parse import urlparse

from pydantic import field_validator

from .base import Model

T = TypeVar("T", bound=Model)

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


def parse_s3_uri(uri: str) -> tuple[str, str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        msg = f"Invalid S3 URI: {uri}"
        raise ValueError(msg)
    bucket_ = parsed.netloc
    key_ = parsed.path.lstrip("/")
    file_ = Path(key_).name
    return bucket_, key_, file_


class S3UriParsed(Model):
    bucket: str
    key: str
    file: str

    @classmethod
    def from_uri(cls, uri: str) -> "S3UriParsed":
        bucket, key, file = parse_s3_uri(uri)
        return cls(bucket=bucket, key=key, file=file)

    @property
    def uri(self) -> str:
        return f"s3://{self.bucket}/{self.key}"

    def __str__(self) -> str:
        return self.uri

    def __repr__(self) -> str:
        return f"<S3UriParsed {self.uri}>"


class S3RemoteFileMixin(Generic[T]):
    @classmethod
    def check_validate_s3_uri(cls: T, v: Any) -> S3UriParsed:
        match v:
            case str():
                return S3UriParsed.from_uri(v)
            case S3UriParsed():
                return v
            case _:
                msg = "Invalid S3 URI"
                raise ValueError(msg)

    @classmethod
    def model_validate_s3_json(
        cls: T, s3_client: "S3Client", s3_uri: str | S3UriParsed
    ) -> T:
        """Using the generic type, call `model_validate_json` on the data fetched from the S3 URI"""
        uri = cls.check_validate_s3_uri(s3_uri)
        buffer = io.BytesIO()
        s3_client.download_fileobj(uri.bucket, uri.key, buffer)
        buffer.seek(0)
        data = buffer.getvalue().decode("utf-8")
        return cls.model_validate_json(data)


class SerialNumberPayload(Model):
    request_id: int
    dc_engagement_id: int
    serial_numbers: set[str]


class SerialNumberFilePayload(S3RemoteFileMixin, SerialNumberPayload):
    """
    If used, s3_uri will be used to load `SerialNumberPayload`
    """

    request_id: int
    dc_engagement_id: int
    s3_uri: S3UriParsed

    @field_validator("s3_uri", mode="before")
    def validate_s3_uri(cls, v: Any) -> S3UriParsed:
        match v:
            case str():
                return S3UriParsed.from_uri(v)
            case S3UriParsed():
                return v
            case _:
                msg = "Invalid S3 URI"
                raise ValueError(msg)
