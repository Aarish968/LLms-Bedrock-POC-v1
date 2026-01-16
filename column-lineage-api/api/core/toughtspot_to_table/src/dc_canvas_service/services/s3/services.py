import io
import logging
from collections import namedtuple
from typing import TYPE_CHECKING, Optional
from urllib.parse import urlparse

from botocore.exceptions import ClientError
from mypy_boto3_s3.type_defs import CopySourceTypeDef

from dc_canvas_service.common.models import TCustomEng, TNamedDest
from dc_canvas_service.common.utils import get_aws_session

from .exceptions import (
    S3DeleteObjectException,
    S3DownloadFileException,
    S3UploadFileException,
)

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_s3.type_defs import FileobjTypeDef

    from dc_canvas_service.common import Env


BucketKey = namedtuple("BucketKey", "bucket key")

logger = logging.getLogger(__name__)


class S3Service:
    """
    Snowflake Service is the service class for interactions with Amazon S3.
    """

    def __init__(self, client: Optional["S3Client"] = None, aws_session=None):
        """
        Initialize S3Service using either s3 client or aws_session.
        If neither is provided, a new AWS session is created.
        :param client: S3 Client
        :param aws_session:
        """
        self.client = (
            client or get_aws_session().client("s3")
            if aws_session is None
            else aws_session.client("s3")
        )

    def download_file(self, bucket: str, key: str) -> bytes:
        """
        Download an object from S3 to a file object using BytesIO.

        :param bucket:  The name of the bucket to download from
        :param key: The name of the key to download from
        :return: Contents of buffered object (bytes)
        """

        with io.BytesIO() as file_obj:
            self.download_file_obj(bucket, key, file_obj)
            return file_obj.getvalue()

    def download_file_obj(
        self, bucket: str, key: str, file_obj: "FileobjTypeDef"
    ) -> None:
        """
        Download an object from S3 to any file object.

        :param bucket: The name of the bucket to download from
        :param key: The name of the key to download from
        :param file_obj: File object for output. If not provided, the output is returned as BytesIO.
        :return: Contents of buffered object or None
        """
        logger.info(f"Downloading file from S3 {bucket=} {key=}")
        try:
            self.client.download_fileobj(bucket, key, file_obj)
        except ClientError as e:
            raise S3DownloadFileException from e

    def move_file(self, bucket: str, key: str, new_key: str) -> None:
        """
        Move file from
        :param bucket: The name of the bucket
        :param key: The name of the key to move from
        :param new_key: The name of the key to move to
        :return:
        """
        logger.info(f"Copying file from S3 {bucket=} {key=} to {new_key=}")
        self.client.copy(
            CopySourceTypeDef(Bucket=bucket, Key=key), Bucket=bucket, Key=new_key
        )
        self.delete_file(bucket, key)

    def upload_file(self, bucket: str, key: str, content: bytes) -> None:
        """
        Upload a file using contents of a file-like object.

        :param bucket: The name of the bucket to upload to
        :param key: The name of the key to upload to
        :param content: A file-like object to upload
        """
        logger.info(f"Uploading file to S3 {bucket=} {key=}")
        try:
            file_obj = io.BytesIO(content)
            self.client.upload_fileobj(Fileobj=file_obj, Bucket=bucket, Key=key)
        except ClientError as e:
            raise S3UploadFileException from e

    def delete_file(self, bucket: str, key: str) -> None:
        """
        Delete a specific file in s3 bucket.

        :param bucket: The bucket name of the bucket containing the object
        :param key: Key name of the object to delete
        """

        logger.info(f"Removing a file from S3 {bucket=} {key=}")
        try:
            self.client.delete_object(Bucket=bucket, Key=key)
        except ClientError as e:
            raise S3DeleteObjectException from e

    def exists(self, bucket: str, key: str) -> bool:
        """
        Check if a given key exists in S3 bucket.

        :param bucket: The bucket name of the bucket containing the object
        :param key: Key name of the object to check
        """
        logger.info(f"Checking if file exists in S3 {bucket=} {key=}")
        try:
            self.client.head_object(Bucket=bucket, Key=key)
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "404":
                return False
            raise S3DownloadFileException from e
        return True

    @staticmethod
    def parse_uri(uri: str) -> BucketKey:
        """
        Parse an S3 URI into its bucket and key components.

        :param uri: URI to be parsed
        :return: tuple(bucket_name, key)
        """
        parsed = urlparse(uri)
        if parsed.scheme != "s3":
            raise ValueError(f"URI {uri} is not an S3 URI")
        return BucketKey(bucket=parsed.netloc, key=parsed.path.lstrip("/"))

    @staticmethod
    def make_liveboard_uri(
        bucket: str,
        env: "Env",
        dest_type: "TNamedDest",
        object_id: str,
        liveboard_id: int,
    ) -> str:
        """
        Generate a liveboard url.

        :rtype: object
        :param bucket: The bucket name for the uri
        :param env:  The env name for the uri
        :param dest_type: The destination type for the uri
        :param object_id: The object id of the destination type
        :param liveboard_id: The liveboard id for the uri
        :return: S3 URI
        """

        dest_url = "engagement/eng_" if dest_type == TCustomEng else f"{dest_type}/"
        uri = f"s3://{bucket}/{env!s}/{dest_url}{object_id}/{liveboard_id}.tml"
        logger.info(f"Using {uri=}")
        return uri
