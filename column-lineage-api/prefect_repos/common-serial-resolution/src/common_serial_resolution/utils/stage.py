import gzip
import json
import logging
from io import BytesIO
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

    from common_serial_resolution import CommonResolutionSettings
    from common_serial_resolution.models import SerialNumber
    from common_serial_resolution.models.settings import S3StageFileUri


logger = logging.getLogger(__name__)


def stage_serial_numbers(
    serial_numbers: set["SerialNumber"],
    settings: "CommonResolutionSettings",
    s3_client: "S3Client",
) -> "S3StageFileUri":
    """
    Stage serial numbers to S3 so that snowflake may access the file.

    Args:
        serial_numbers: Set of `SerialNumber` to stage. `SerialNumber` is a NewType[str] that represents a processed serial number.
        settings: CommonResolutionSettings
        s3_client: boto3 S3 client. Caller should ensure it is authenticated.

    Returns:
        S3StagedFile: Information about the staged file
    """
    # Create JSON data with the required format
    json_data = [{"serial_number": sn} for sn in serial_numbers]

    jsonb = json.dumps(json_data, separators=(",", ":")).encode("utf-8")

    # Compress the JSON data with gzip
    compressed_data = BytesIO()
    with gzip.GzipFile(fileobj=compressed_data, mode="wb") as gz:
        gz.write(jsonb)

    compressed_data.seek(0)

    # Get the S3 URI from settings
    s3_staged_file = settings.make_staged_s3_uri()

    logger.info(
        "Staging serial numbers to S3: %s, Snowflake URI: %s",
        s3_staged_file.s3_uri,
        s3_staged_file.snowflake_uri,
    )

    # Upload the compressed data to S3
    s3_client.upload_fileobj(
        compressed_data,
        s3_staged_file.bucket,
        s3_staged_file.key,
        ExtraArgs={
            "ContentType": "application/json",
            "ContentEncoding": "gzip",
        },
    )

    logger.info(
        "Staged serial numbers to S3: %s, Snowflake URI: %s",
        s3_staged_file.s3_uri,
        s3_staged_file.snowflake_uri,
    )

    return s3_staged_file
