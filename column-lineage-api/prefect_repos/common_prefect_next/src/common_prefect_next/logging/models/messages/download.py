from typing import Literal

from pydantic import ConfigDict, Field

from . import MessageType, Model


class DownloadData(Model):
    label: str = Field(
        ..., description="The label of the download.", examples=["Download Report"]
    )
    url: str = Field(
        ...,
        description="The URL of the download.",
        examples=["s3://results/report.csv"],
    )


class DownloadMessage(Model):
    type: Literal[MessageType.download] = Field(default=MessageType.download.value)
    data: DownloadData

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "type": "download",
                    "data": {
                        "label": "Download Report",
                        "url": "s3://results/report.csv",
                    },
                }
            ]
        }
    )
