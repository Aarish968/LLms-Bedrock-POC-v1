from pathlib import Path
from urllib.parse import urlparse

from pydantic import BaseModel, Field, computed_field


class S3UriComponents(BaseModel):
    bucket: str = Field(..., description="The S3 bucket name", examples=["s3-bucket"])
    key: str = Field(..., description="The S3 key", examples=["path/to/file.csv"])
    file_name: str = Field(..., description="The file name", examples=["file.csv"])

    @computed_field
    @property
    def s3_uri(self) -> str:
        """
        Generate the S3 URI from the bucket and key.
        """
        return f"s3://{self.bucket}/{self.key}"


def parse_s3_uri(uri: str) -> S3UriComponents:
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        msg = f"Invalid S3 URI: {uri}"
        raise ValueError(msg)
    bucket_ = parsed.netloc
    if not bucket_:
        msg = f"Invalid S3 URI: {uri} (missing bucket)"
        raise ValueError(msg)
    key_ = parsed.path.lstrip("/")
    file_ = Path(key_).name
    return S3UriComponents(bucket=bucket_, key=key_, file_name=file_)
