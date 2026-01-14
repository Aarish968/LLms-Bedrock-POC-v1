import pandas as pd
import numpy as np
import os
import datetime as dt
from datetime import datetime
import math
from common import file_ops, core_cr_fn
from sqlalchemy import create_engine
from common import sec
import logging
import awswrangler as wr
import prefect
from prefect.engine.results.s3_result import S3Result
from prefect.run_configs.kubernetes import KubernetesRun
from prefect.storage import Docker
from prefect.executors import DaskExecutor
from prefect.executors import LocalDaskExecutor
from prefect import task, unmapped, Flow, Parameter
from prefect.run_configs.kubernetes import KubernetesRun
import parse
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 200)

snowflake_db = "CPS_DB"
dn_key_name = "prd_cps_dsci_etl_svc"
schema = "CPS_DSCI_ARCHIVE"
warehouseMed = "cps_dsci_etl_wh"  # Medium
warehouseXsmall = "CPS_DSCI_ETL_EXT1_WH"  # X-Small
warehouseSmall = "CPS_DSCI_ETL_EXT2_WH"  # Small


logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


prefect_client = prefect.Client(api_key=sec.get_prefect_token())


def get_memory_required():
    default_memory_required = 100000000000
    one_mill = 2040109465
    three_mill =  6120328396
    six_mill =    12025908428
    # try:
    #     memory_required = 0
    #     for file in file_refs:
    #         bucket = file["loc"].replace("s3://", "").split("/")[0]
    #         key = file["loc"].replace(f"s3://{bucket}/", "")
    #         print(bucket, key)
    #         for key in s3_client.list_objects(Bucket=bucket, Prefix=key)["Contents"]:
    #             memory_required = memory_required + key["Size"]
    #     print(f"""Size of file(s) to be ran : {memory_required}, estimated RAM needed : {memory_required*500}""")
    #     return memory_required*500
    # except Exception as ex:
    #     print(f"""Could not infer size of files, reverting to default memory required of : {default_memory_required}""")
    #     print(ex)
    return default_memory_required


def get_requests_to_prep(query_limit):
    engine = create_engine(sec.get_sf_pw(dn_key_name, warehouseXsmall, schema))

    df = pd.read_sql(
            f""" with prep_separators as (
            select UID, ID, CAMCECID, ACCOUNTNAME, SFCFA,SERVICE_LEVEL, EXCLUSION_NOTES,
                   SFC_AGREEMENT_TYPE, CREATED_BY, CREATE_DTM, UPDATED_BY, UPDATE_DTM, IS_ACTIVE, CONTRACT_NUMBER,FILE_UPLOAD_STATUS, FILE_UPLOAD_DTM
                    ,replace(replace(replace(replace(h.ACATACCOUNTID, ' ', ' '),' ', ' '),' ',' '), ' ', ',') as ACATACCOUNTID
            from CPS_BIA_BR.DATA_CANVAS_ENGAGEMENT_HDR_V h
        )
        select * from CPS_DSCI_ARCHIVE.ACAT_CANVAS_DATA_SOURCE_META m where CUSTOMER_ID in (
            SELECT distinct value as ACAT_ACCOUNT_ID
            FROM prep_separators,
                LATERAL STRTOK_SPLIT_TO_TABLE(prep_separators.ACATACCOUNTID, ',')
        )
        and m.IS_PREPPED = 'F' {query_limit}
        ; 
        """,
            engine,
        )


    return df


def trigger_flow_run(
    input_params: str,
    memory_request: int,
) -> dict:
    return prefect_client.create_flow_run(
        version_group_id="e70245e1-d610-449f-9746-92f774e2b49e",
        parameters=dict(
            input_params=input_params
        ),
        run_config=KubernetesRun(memory_request=memory_request)
    )




@task
def run(query_limit):
    logger.info(f"Querying for request ids to prep")

    payload = get_requests_to_prep(query_limit)
    logger.info(f"Request ids to run : {payload.head()}")




    for index, row in payload.iterrows():
        print(row['request_id'])
        result = parse.search("s3://canvas-data-store-dev/ACAT_FILES/{file_name}.parquet", row['file_path'])
        file_name = result["file_name"]
        input_params = {
            "date": str(datetime.now().date()),
            "run_id": file_name,
            "schema": "CPS_DSCI",
            "this_file": f"s3://canvas-data-store-dev/ACAT_FILES/{file_name}.parquet",
            "bucket_name": "canvas-data-store-dev",
            "multi_files": f"s3://canvas-data-store-dev/ACAT_FILES/{file_name}/multis/",
            "single_files": f"s3://canvas-data-store-dev/ACAT_FILES/{file_name}/singles/",
            "full_canvas_out_pth": f"s3://canvas-data-store-dev/canvas_dir/{file_name}/full_canvas/",
            "multi_files_out_pth": f"s3://canvas-data-store-dev/canvas_dir/{file_name}/multis_prepped/",
            "single_files_out_pth": f"s3://canvas-data-store-dev/canvas_dir/{file_name}/singles_prepped/"
        }
        print(input_params)
        memory_required = get_memory_required()
        print(memory_required)

        prefect_response = trigger_flow_run(
            input_params= input_params,
            memory_request = memory_required
        )

        logger.info(
            f"Flow Run ID: {prefect_response}"
        )

    return "Success"









storage_obj = Docker(
    base_image="prefecthq/prefect:0.15.3-python3.8",
    python_dependencies=[
        "pandas==1.1.3",
        "awswrangler==2.10.0",
        "numpy==1.19.2",
        "elasticsearch==7.14.0",
        "boto3==1.18.16",
        "aiohttp",
        "hvac",
        "snowflake-sqlalchemy==1.2.4",
        "s3fs==0.4",
        "hvac>0.11.0",
        "SQLAlchemy==1.3.20",
        "awswrangler>2.10.0",
        "fastparquet>0.7.1",
        "XlsxWriter>3.0.1",
        "orderedset",
        "parse>1.19.0"
    ],
    registry_url="837578041534.dkr.ecr.us-east-1.amazonaws.com/flows",
    files={
        """/Users/ejurotic/PycharmProjects/act-mce-src-and-prep/common/core_cr_fn.py""": "/root/.prefect/flows/common/core_cr_fn.py",
        """/Users/ejurotic/PycharmProjects/canvas-create-flow/canvas-create-flow/common/new_bulkload.py""": "/root/.prefect/flows/common/new_bulkload.py",
        """/Users/ejurotic/PycharmProjects/act-mce-src-and-prep/common/sec.py""": "/root/.prefect/flows/common/sec.py"
    },
    env_vars={"PYTHONPATH": "${PYTHONPATH}:/root/.prefect/flows/"},
)



with Flow(
        "trigger_prep_data_acat",
        storage=storage_obj,
        run_config=KubernetesRun(),
        executor=LocalDaskExecutor(scheduler="processes", num_workers=10),
        result=S3Result(bucket="cam-prefect-results"),
        ) as canvas_prep_data_acat:
        query_limit = Parameter("query_limit", default='limit 10')
        run(query_limit)




if __name__ == "__main__":
    canvas_prep_data_acat.run(
        parameters=dict(
            query_limit="limit 1",)
    )

