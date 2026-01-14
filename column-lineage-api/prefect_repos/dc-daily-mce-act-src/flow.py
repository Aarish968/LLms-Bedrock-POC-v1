import psutil
import pandas as pd
import json
import boto3
from prefect.executors.dask import LocalDaskExecutor
from prefect.run_configs.docker import DockerRun
from prefect.storage import Docker
from prefect import Flow, Parameter, task, case
from sqlalchemy import create_engine
from common import sec
from queries import aggs_query,flatten_qry,mce_src_prep_qry
from prefect.run_configs.kubernetes import KubernetesRun


temp_base_location = "/tmp"

snowflake_db = "CPS_DB"
dn_key_name = "prd_cps_dsci_etl_svc"


schema = "CPS_DSCI_ARCHIVE"
warehouseMed = "cps_dsci_etl_wh"  # Medium
warehouseXsmall = "CPS_DSCI_ETL_EXT1_WH"  # X-Small
warehouseSmall = "CPS_DSCI_ETL_EXT2_WH"  # Small

@task(log_stdout=True, tags=[f"snowflake_large"])
def flatten(schema):
    engine = create_engine(sec.get_sf_pw('prd_cps_dsci_etl_svc', 'CPS_DSCI_ETL_EXT3_WH', schema))

    con = engine.connect()
    flatten_done = con.execute(flatten_qry.flatten_qry)



    return True

@task(log_stdout=True, tags=[f"snowflake_large"])
def aggs(schema):
    engine = create_engine(sec.get_sf_pw('prd_cps_dsci_etl_svc', 'CPS_DSCI_ETL_EXT3_WH', schema))

    con = engine.connect()
    aggs_done = con.execute(aggs_query.aggs_query)
    return True

@task()
def mce_src_prep():
    print(mce_src_prep_qry.mce_src_prep_qry)
    return True

@task()
def mce_evidence():
    return True


@task()
def mce_evidence():
    return True


@task()
def mce_map_to_parquet():
    return True

@task()
def acat_oracle_20():
    return True

@task()
def acat_evidence():
    return True


@task()
def acat_src_prep():
    return True


@task()
def acat_map_to_parquet():
    return True


storage_obj = Docker(
    base_image="prefecthq/prefect:0.15.3-python3.8",
    python_dependencies=[
        "pandas==1.4.2",
        "awswrangler==2.10.0",
        "numpy==1.19.2",
        "elasticsearch==7.14.0",
        "boto3==1.18.16",
        "aiohttp",
        "hvac",
        "snowflake-sqlalchemy==1.2.4",
        "s3fs==0.4",
        "hvac>0.11.0",
        "SQLAlchemy==1.4.37",
        "awswrangler>2.10.0",
        "fastparquet>0.7.1",
        "XlsxWriter>3.0.1",
        "oyaml",
        "thoughtspot_rest_api_v1==1.0.6",
        "thoughtspot_tml==1.0.9",
        "deepdiff",
        ""
    ],
    registry_url="837578041534.dkr.ecr.us-east-1.amazonaws.com/flows",
    files={
        """/Users/ejurotic/PycharmProjects/dc_src_prep_2022/common/new_bulkload.py""": "/root/.prefect/flows/common/new_bulkload.py",
        """/Users/ejurotic/PycharmProjects/dc_src_prep_2022/common/sec.py""": "/root/.prefect/flows/common/sec.py",
        """/Users/ejurotic/PycharmProjects/dc_src_prep_2022/common/sql_pool.py""": "/root/.prefect/flows/common/sql_pool.py",
        """/Users/ejurotic/PycharmProjects/dc_src_prep_2022/queries/aggs_query.py""" : "/root/.prefect/flows/queries/aggs_query.py",
        """/Users/ejurotic/PycharmProjects/dc_src_prep_2022/queries/flatten_qry.py""" : "/root/.prefect/flows/queries/flatten_qry.py",
        """/Users/ejurotic/PycharmProjects/dc_src_prep_2022/queries/mce_src_prep_qry.py""" : "/root/.prefect/flows/queries/mce_src_prep_qry.py",
    },
    env_vars={"PYTHONPATH": "${PYTHONPATH}:/root/.prefect/flows/"},
)

with Flow(
        "dc_src_prep_2022",
        storage=storage_obj,
        run_config=KubernetesRun(),
        executor=LocalDaskExecutor(scheduler="processes", num_workers=psutil.cpu_count(logical=True)),
        # executor=LocalDaskExecutor(scheduler="processes", num_workers=16),
        # result=S3Result(bucket="cam-prefect-results")



) as flow:
    schema = Parameter("schema", required=True)



    flatten = flatten(schema)

    aggs = aggs(schema, upstream_tasks=[flatten])

    # mce_src_prep = mce_src_prep(upstream_tasks=[aggs])
    #
    # mce_evidence = mce_evidence(upstream_tasks=[mce_src_prep])
    #
    # mce_map_to_parquet = mce_map_to_parquet(upstream_tasks=[mce_src_prep])
    #
    # acat_oracle_20 = acat_oracle_20()
    #
    # acat_evidence = acat_evidence(upstream_tasks=[acat_oracle_20])
    #
    # acat_src_prep = acat_src_prep(upstream_tasks=[acat_evidence, aggs])
    #
    # acat_map_to_parquet = acat_map_to_parquet(upstream_tasks=[acat_src_prep])

if __name__ == "__main__":
    flow.run(
        parameters={
            "schema": "CPS_DSCI_ARCHIVE",
        }
    )

