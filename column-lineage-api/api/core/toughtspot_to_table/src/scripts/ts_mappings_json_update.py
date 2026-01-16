"""
    Script to update Mapping Data
    Required Libraries: [pandas>=2.2.3, openpyxl>=3.1.5]

    How to run ?
        - Just simply run the file updating the Main Guard with required params

    Required Params:
        - env: Environment to which we need to update
        - file_path: Excel File path
        - sheet_name: Sheet Name containing the Data

    Things to make sure:
        - Excel file sheet should contain following columns (Case Sensitive)
            1. column_name
            2. column_type
          Refer: mapping.xlsx

    Make sure your credentials are updated
"""

import ast
import json
import datetime

import pandas as pd

from mypy_boto3_s3.type_defs import CopySourceTypeDef

from dc_canvas_service.common import Settings
from dc_canvas_service.services.s3 import S3Service


def update_mappings_data(env: str, file_path: str, sheet_name: str) -> None:
    # Reads the Excel File
    df = pd.read_excel(file_path, sheet_name=sheet_name)

    # Validates the columns present
    columns_required = ["column_name", "column_type"]
    if not set(columns_required).issubset(df.columns):
        raise Exception(f"Columns Required {columns_required=}")

    # Creates necessary connections
    settings = Settings(env=env)
    s3 = S3Service(aws_session=settings.aws_session)

    # Downloads the existing mapping file
    mappings_file = s3.download_file(
        bucket=settings.ts_data_types_bucket_name, key=settings.ts_data_types_file_path
    )

    timestamp = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d|%H:%M:%S")

    # Backup File
    s3.client.copy(
        CopySourceTypeDef(
            Bucket=settings.ts_data_types_bucket_name,
            Key=settings.ts_data_types_file_path,
        ),
        Bucket=settings.ts_data_types_bucket_name,
        Key=f"{settings.env}/backup/dev_properties_map_{timestamp}.json",
    )

    # Loads the mapping file
    mappings_data = json.loads(mappings_file)

    mappings_data.update(
        {
            k: ast.literal_eval(v) if isinstance(v, str) else v
            for k, v in mappings_data.items()
        }
    )

    # Formats the data for upload
    df.set_index("column_name", inplace=True)
    new_mappings_data = df.to_dict(orient="index")

    # Updates the mapping_data with new_mapping_data
    mappings_data.update(new_mappings_data)

    # Uploads the data to S3 bucket
    s3.upload_file(
        bucket=settings.ts_data_types_bucket_name,
        key=settings.ts_data_types_file_path,
        content=json.dumps(mappings_data).encode("utf-8"),
    )


if __name__ == "__main__":
    update_mappings_data(env="dev", file_path="mapping.xlsx", sheet_name="Sheet1")
