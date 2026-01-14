
from prefect import Flow, Parameter, task
from sqlalchemy import create_engine
from common import sec
import os
temp_base_location = "/tmp"
from prefect.engine.results.s3_result import S3Result
from prefect.executors.dask import LocalDaskExecutor
from prefect.run_configs.kubernetes import KubernetesRun
from prefect.storage import Docker

schema = "CPS_DSCI_ARCHIVE"
warehouseMed = "cps_dsci_etl_wh"  # Medium
warehouseXsmall = "CPS_DSCI_ETL_EXT1_WH"  # X-Small
warehouseSmall = "CPS_DSCI_ETL_EXT2_WH"  # Small
import sqlalchemy
import source_scd


@task(log_stdout=True,tags=["snowflake_xlarge"])
def scd_c3():

    engine = create_engine(
        sec.get_sf_pw('prd_cps_dsci_etl_svc', 'CPS_DSCI_ETL_EXT4_WH', schema)
    )
    con = engine.connect()
    import source_scd
    for sql_block in source_scd.lst_of_refresh_c3:
        for sql in sql_block.split(";"):
            print(f"{sql}; \n")
            ret = con.execute(sql)
            try:
                for row in ret:
                    print(row)
            except:
                pass

    return True


def get_sec_dir(pth):
    return os.path.join(os.getcwd() , pth)

storage_obj = Docker(
    base_image="837578041534.dkr.ecr.us-east-1.amazonaws.com/prefect-image:0.15.3-python3.8",
    python_dependencies=[
        "pandas==1.4.2",
        "awswrangler==2.12.1",
        "numpy==1.22.3",
        "elasticsearch==7.14.0",
        "boto3==1.18.16",
        "aiohttp",
        "snowflake-sqlalchemy==1.2.4",
        "s3fs==0.4",
        "hvac>0.11.0",
        "SQLAlchemy==1.4.35",
        "oyaml",
        
        
        
        
    ],
    registry_url="837578041534.dkr.ecr.us-east-1.amazonaws.com/flows",
    files={
        get_sec_dir('common/sec.py'): "/root/.prefect/flows/common/sec.py",
        get_sec_dir('source_scd.py'): "/root/.prefect/flows/source_scd.py"
    },
    # files={
    #     """/Users/ejurotic/PycharmProjects/scd_flows/source_scd.py""": "/root/.prefect/flows/source_scd.py",
    #     """/Users/ejurotic/PycharmProjects/scd_flows/common/sec.py""": "/root/.prefect/flows/common/sec.py",
    # },
    env_vars={"PYTHONPATH": "${PYTHONPATH}:/root/.prefect/flows/"},
)

with Flow(
    "scd_c3",
    storage=storage_obj,
    run_config=KubernetesRun(),
    executor=LocalDaskExecutor(scheduler="processes", num_workers=1),
    result=S3Result(bucket="cam-prefect-results"),
) as flow:
    complete = scd_c3()


if __name__ == "__main__":
    flow.run()
