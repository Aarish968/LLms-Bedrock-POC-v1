from __future__ import annotations

from io import BytesIO

import awswrangler as wr
import pandas as pd
from prefect import task

from common.config import RunSettings, TemplateConfig
from common.repo import FlowParams


@task(log_stdout=True)
def read_template_params(
    file_location: str,
    bucket_name: list[str] | str,
    request_id: str,
    settings: RunSettings,
) -> FlowParams:
    """Read the Excel Parameters file"""

    file_obj = BytesIO()
    wr.s3.download(file_location, file_obj)
    file_obj.seek(0)

    df_params = pd.read_excel(
        file_obj,
        sheet_name=TemplateConfig.Sheets.PARAMETERS,
        names=[
            TemplateConfig.Columns.CUSTOMER_NAME,
            TemplateConfig.Columns.MCE_ENGAGEMENT_ID,
        ],
    )

    customer_name = df_params[TemplateConfig.Columns.CUSTOMER_NAME].iloc[0]
    engagement_id = df_params[TemplateConfig.Columns.MCE_ENGAGEMENT_ID].iloc[0]

    if isinstance(bucket_name, list):
        bucket_name = bucket_name[0]

    result_uri = (
        f"s3://{bucket_name}/{settings.env}/output_files/mce_macro_{request_id}.xlsx"
    )

    params = FlowParams(
        customer_name=customer_name,
        mce_engagement_id=engagement_id,
        run_id=request_id,
        output_uri=result_uri,
    )

    print("Flow Params".center(60, "="))
    print(f"Customer Name: {params.customer_name}".ljust(8))
    print(f"MCE Engagement ID: {params.mce_engagement_id}".ljust(8))
    print(f"Run ID: {params.run_id}".ljust(8))
    print(f"Output URI: {params.output_uri}".ljust(8))
    print(f"Transient Table Name: {params.transient_table_name}".ljust(8))
    print("".center(60, "="))

    return params


@task(log_stdout=True)
def validate_flow_params(query_params: dict) -> FlowParams:
    """We're validating the parameters that were passed in via disparate sources"""

    params = FlowParams.parse_obj(query_params)
    print("Flow Params".center(60, "="))
    print(f"Customer Name: {params.customer_name}".ljust(8))
    print(f"MCE Engagement ID: {params.mce_engagement_id}".ljust(8))
    print(f"Run ID: {params.run_id}".ljust(8))
    print(f"Output URI: {params.output_uri}".ljust(8))
    print(f"Transient Table Name: {params.transient_table_name}".ljust(8))
    print("".center(60, "="))
    return params
