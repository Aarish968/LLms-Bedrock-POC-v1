import pandas as pd
from prefect import Flow, Parameter, task
from sqlalchemy import create_engine
from common import sec
temp_base_location = "/tmp"
from prefect.engine.results.s3_result import S3Result
from prefect.executors.dask import LocalDaskExecutor
from prefect.run_configs.kubernetes import KubernetesRun
from prefect.storage import Docker
import os

schema = "CPS_DSCI_ARCHIVE"
warehouseMed = "cps_dsci_etl_wh"  # Medium
warehouseXsmall = "CPS_DSCI_ETL_EXT1_WH"  # X-Small
warehouseSmall = "CPS_DSCI_ETL_EXT2_WH"  # Small
import sqlalchemy
import source_scd

@task(log_stdout=True,tags=["snowflake_large"])
def scd_dc(view):
    engine = create_engine(sec.get_sf_pw('prd_cps_dsci_etl_svc', 'CPS_DSCI_ETL_EXT3_WH', schema))
    con = engine.connect()



    for sql_block in source_scd.lst_of_refresh_dc:
        for sql in  sql_block.split(";"):
            print(f"{sql}; \n")
            ret = con.execute(sql)
            try:
                for row in ret:
                    print(row)
            except:
                pass

    return True



def check_env(env):
    print(env)
    if env == "dev":
        cn = "dev_cps_dsci_etl_svc"
    elif env == "stage":
        cn = "stg_cps_dsci_etl_svc"
    elif env == "prod":
        cn = "prd_cps_dsci_etl_svc"
    return cn


@task(log_stdout=True, tags=['snowflake_large'] )
def build_unified_tag_view(env):
    tag_view = 'CPS_DSCI_API.ALL_TAGS_TBL'
    cn = check_env(env)
    engine = create_engine(sec.get_sf_pw(cn, 'CPS_DSCI_ETL_EXT3_WH', 'cps_dsci_archive'))
    #tblsql = "select TABLE_SCHEMA || '.' || TABLE_NAME as tag_tbls from information_schema.tables where TABLE_SCHEMA = 'CPS_DSCI_API' and TABLE_NAME like 'DATA_CANVAS_ENGAGEMENT_TAGS_%'"
    
    tblsql ="""with active_eng as (
            select  uid from CPS_BIA_BR.DATA_CANVAS_ENGAGEMENT_HDR_V where nvl(IS_ACTIVE,'Y') = 'Y'
            )
        ,exist as (
                select TABLE_SCHEMA || '.' || TABLE_NAME as tag_tbls ,
                    try_to_number(replace(TABLE_NAME, 'DATA_CANVAS_ENGAGEMENT_TAGS_', '') ) as cid
                    from information_schema.tables
                    where TABLE_SCHEMA = 'CPS_DSCI_API' and TABLE_NAME like 'DATA_CANVAS_ENGAGEMENT_TAGS_%'
            )
            select tag_tbls 
            from exist join active_eng on (exist.cid=active_eng.uid)
         where exist.cid is not null and active_eng.uid is not null"""
    
    tbls = pd.read_sql(tblsql, engine)
    union_sql = tbls.tag_tbls.to_list()
    union_sql = " union select * from ".join(union_sql)
    union_sql = f"""create or replace TABLE {tag_view} as 
                select * from {union_sql} ;"""
    print(union_sql)
    con = engine.connect()
    con.execute(union_sql)
    con.close()
    return tag_view


# def build_unified_tag_view(env):
#     tag_view = 'CPS_DSCI_API.ALL_TAGS_TBL'
#     cn = check_env(env)
#     engine = create_engine(sec.get_sf_pw(cn, 'CPS_DSCI_ETL_EXT3_WH', 'cps_dsci_archive'))
#     tblsql = """with active_eng as (
#             select  uid from CPS_BIA_BR.DATA_CANVAS_ENGAGEMENT_HDR_V where nvl(IS_ACTIVE,'Y') = 'Y'
#             )
#         ,exist as (
#                 select TABLE_SCHEMA || '.' || TABLE_NAME as tag_tbls ,
#                     try_to_number(replace(TABLE_NAME, 'DATA_CANVAS_ENGAGEMENT_TAGS_', '') ) as cid
#                     from information_schema.tables
#                     where TABLE_SCHEMA = 'CPS_DSCI_API' and TABLE_NAME like 'DATA_CANVAS_ENGAGEMENT_TAGS_%'
#             )
#             select tag_tbls 
#             from exist join active_eng on (exist.cid=active_eng.uid)
#          where exist.cid is not null and active_eng.uid is not null"""
#     tbls = pd.read_sql(tblsql, engine)
#     union_sql = tbls.tag_tbls.to_list()
#     union_sql = " union select * from ".join(union_sql)
#     union_sql = f"""create or replace TABLE {tag_view} as 
#                 select * from {union_sql} ;"""
#     print(union_sql)
#     con = engine.connect()
#     con.execute(union_sql)
#     con.close()
#     print(union_sql)
#     return tag_view


def get_sec_dir(pth):
    #print(os.getcwd(), pth, os.path.join(os.getcwd(), pth))
    return os.path.join(os.getcwd(), pth)




storage_obj = Docker(
    base_image="837578041534.dkr.ecr.us-east-1.amazonaws.com/prefect-image:0.15.3-python3.8",
    python_dependencies=[
        "requests==2.27.1",
        "pandas==1.4.2",
        "awswrangler==2.12.1",
        "numpy==1.22.3",
        "elasticsearch==7.14.0",
        "boto3==1.18.16",
        "aiohttp",
        "hvac",
        "snowflake-sqlalchemy==1.2.4",
        "s3fs==0.4",
        "hvac>0.11.0",
        "SQLAlchemy==1.4.35",
        "fastparquet>0.7.2",
        "XlsxWriter>3.0.3",
        "oyaml"


    ],
    registry_url="837578041534.dkr.ecr.us-east-1.amazonaws.com/flows",
    files={
       get_sec_dir('common/sec.py') : "/root/.prefect/flows/common/sec.py",
       get_sec_dir('source_scd.py') : "/root/.prefect/flows/source_scd.py"
    },
    env_vars={"PYTHONPATH": "${PYTHONPATH}:/root/.prefect/flows/"},
)

with Flow(
    "scd_datacanvas",
    storage=storage_obj,
    run_config=KubernetesRun(),
    executor=LocalDaskExecutor(scheduler="processes", num_workers=1),
    result=S3Result(bucket="cam-prefect-results"),
) as flow:
    tagview = build_unified_tag_view('prod')
    complete = scd_dc(tagview)

if __name__ == "__main__":
    flow.run()

#flow.register(project_name="ElasticSearch Canvas Load")

