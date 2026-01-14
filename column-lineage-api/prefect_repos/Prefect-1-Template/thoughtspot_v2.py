from typing import Optional, List, Dict
import requests
from prefect.engine.signals import FAIL , SUCCESS
import datetime as dt
from datetime import datetime
from datetime import date as day
import time
import ast
import string
import random
import os
from prefect.client import Client
from prefect.tasks.secrets import PrefectSecret
from prefect.tasks.prefect import RenameFlowRun
from prefect import unmapped
import prefect
from prefect.triggers import all_successful, all_failed, all_finished
from prefect.engine.signals import SKIP
import pandas as pd
import json
from common import aws_sec
from prefect.run_configs.kubernetes import KubernetesRun
import boto3
from time import sleep
from prefect import Flow, Parameter, task, case
from sqlalchemy import create_engine
from common import sec
import psutil
import oyaml
import awswrangler as wr
from collections import OrderedDict

import flow_variables

temp_base_location = "/tmp"
from prefect.engine.results.s3_result import S3Result
from prefect.executors.dask import LocalDaskExecutor
from prefect.run_configs.docker import DockerRun
from prefect.storage import Docker


# TODO use state handlers and state to always log failures
# TODO RUN TMLS in map
# TODO skip all validation
# TODO make logging easier to query
# TODO add each new object to the landding page as its created



def fix_cols(df):
    cols = []
    for c in df.columns:
        cl = c.lower()
        cols.append(
            cl.strip()
            .replace(" ", "_")
            .replace("/", "_")
            .replace("\\", "_")
            .replace("-", "_")
        )
    return cols


def get_json_from_s3(bucket, key):
    session = boto3.Session(
        aws_access_key_id=aws_sec.ACCESS_KEY, aws_secret_access_key=aws_sec.SECRET_KEY
    )

    s3 = session.resource("s3")
    obj = s3.Object(bucket, key)
    data = obj.get()["Body"].read().decode("utf-8")
    json_data = oyaml.safe_load(data)
    return json_data

def delete_json_from_s3(bucket, key):
    key = f'{key}.json'
    session = boto3.Session(
        aws_access_key_id=aws_sec.ACCESS_KEY, aws_secret_access_key=aws_sec.SECRET_KEY
    )

    s3 = session.resource("s3")
    obj = s3.Object(bucket, key)
    obj.delete()

    return True



def check_env(env):
    logger = prefect.context.get("logger")
    logger.info(env)
    if env == "dev":
        cn = "dev_cps_dsci_etl_svc"
    elif env == "stage":
        cn = "stg_cps_dsci_etl_svc"
    elif env == "prod":
        cn = "prd_cps_dsci_etl_svc"
    else:
        cn = env
    logger.info(f"""converted env : {cn}""")
    return cn






@task(log_stdout=True ,tags=["snowflake_xsmall"])
def read_from_sql(sf_env):
    logger = prefect.context.get("logger")
    cn = check_env('prod')
    correct_schema = get_correct_schema(sf_env)
    bia_engine = create_engine(
        sec.get_sf_pw(cn, flow_variables.warehouseXsmall, correct_schema)
    )
    bia_con = bia_engine.connect()


    bia_qry = f"""
    select * from CPS_DB.{correct_schema}.DC_TS_REPORTING ;
    """

    print(bia_qry)


    try:
        df = pd.read_sql(bia_qry,bia_con)
    except Exception as e:
        print(
            f"Data for this REQUEST_ID  is not in CPS_DB.{correct_schema}.DC_TS_REPORTING"
        )
        raise FAIL()

    return df







def update_file_management_liveboards_table(sf_env,new_guid,lb_id,requestedBy):
    cn = check_env('prod')
    correct_schema = get_correct_schema(sf_env)

    engine = create_engine(
        sec.get_sf_pw(cn, flow_variables.warehouseXsmall, correct_schema)
    )

    con = engine.connect()
    time_created = datetime.now().isoformat()

    if isinstance(requestedBy,list):
        requestedBy = requestedBy[0]

    qry = f"""UPDATE {correct_schema}.{flow_variables.main_table} set GUID = '{new_guid}' ,
                                            UPDATE_DTM = '{time_created}' ,
                                            UPDATED_BY = '{requestedBy}'
    where LIVEBOARD_ID  = {lb_id} ;"""


    try:
        con.execute(qry)
    except Exception as e:
        print(e)
        print(
            f"Failed while attempting to update lb_id: {lb_id} to guid: {new_guid}"
        )

    return True





@task(log_stdout=True,tags=["snowflake_xsmall"])
def delete_sf_table(db_table,sf_env,schema,del_sf_table):
    if del_sf_table:
        correct_schema = get_correct_schema(sf_env)
        cn = check_env('prod')
        engine = create_engine(
            sec.get_sf_pw(cn, flow_variables.warehouseXsmall, correct_schema)
        )


        con = engine.connect()
        print(f"""drop view if exists {schema}.{db_table.lower()};""")
        delete_done = con.execute(f"""drop view if exists {schema}.{db_table.lower()};""")

    return True

def get_sec_dir(pth):
    return os.path.join(os.getcwd(), pth)



@task(log_stdout=True)
def delete_folder_in_s3(path_to_delete):

    try:
        wr.s3.delete_objects(path_to_delete)
    except Exception as e:
        print(e)
    return True



def get_correct_schema(env):
    if env == 'prod':
        return 'CPS_DSCI_API'
    else:
        return 'CPS_DSCI_BR'





def write_to_sql(sf_env,request_id, log_message):
    cn = check_env('prod')
    correct_schema = get_correct_schema(sf_env)

    engine = create_engine(
        sec.get_sf_pw(cn, flow_variables.warehouseXsmall, correct_schema)
    )

    con = engine.connect()


    bia_qry = f"""
    insert into {correct_schema}.dc_job_messages(request_id,logged_message) values ({request_id},'{log_message}')
    """

    try:
        con.execute(bia_qry)
    except Exception as e:
        print(e)
        print(
            f"Failed while attempting to log message to : {correct_schema}.dc_job_messages"
        )

    return True

@task(log_stdout=True)
def map_example(number, sf_env):
    mult_num = number * 100
    print(mult_num)

    return mult_num





@task(log_stdout=True)
def create_list_of_nunmber(df_out):
    print(df_out.head())

    list_of_nums = [1,2,3,4,5,6,7,8]

    return list_of_nums

@task(log_stdout=True, nout = 2)
def print_map_output(list_of_multiplied_nums):
    print('all done')
    print(list_of_multiplied_nums)

    return True, 'all done'

storage_obj = Docker(
    # base_image="containers.cisco.com/ejurotic/prefect_15_13_python_3_8",
    base_image="837578041534.dkr.ecr.us-east-1.amazonaws.com/bases/prefect:0.15.13-python3.9",
    python_dependencies=[
        "pandas==1.4.2",
        "awswrangler==2.12.1",
        "numpy==1.25.1",
        "boto3",
        "botocore",
        "aiohttp==3.8.4",
        "hvac==0.11.2",
        "snowflake-sqlalchemy==1.2.4",
        "s3fs==0.4",
        "SQLAlchemy===1.4.41",
        "awswrangler==2.12.1",
        "fastparquet==0.7.2",
        "XlsxWriter==3.1.2",
        "oyaml==1.0",
        "thoughtspot_rest_api_v1==1.3.1",
        "thoughtspot_tml==1.2.0",
        "cloudpickle==2.0.0"


    ],
    # registry_url="containers.cisco.com/ejurotic",
    registry_url="837578041534.dkr.ecr.us-east-1.amazonaws.com/dc/ts/p1",
    path="thoughtspot_v2.py",
    files={
        # get_sec_dir('common/new_bulkload.py') : "/root/.prefect/flows/common/new_bulkload.py",
        get_sec_dir('common/sec.py') : "/common/sec.py",
        get_sec_dir('common/aws_sec.py') : "/common/aws_sec.py",
        get_sec_dir('refresh_and_tags.py') : "/refresh_and_tags.py",
        get_sec_dir('flow_variables.py'): "/flow_variables.py",
        # get_sec_dir('common/sql_pool.py'): "/root/.prefect/flows/common/sql_pool.py",
        # get_sec_dir('common/file_ops.py'): "/root/.prefect/flows/common/file_ops.py",
        get_sec_dir('add_actions_task.py'): "add_actions_task.py",
        get_sec_dir('thoughtspot_v2.py'): "thoughtspot_v2.py",

    },
    env_vars={"PYTHONPATH": "${PYTHONPATH}:/"},
    stored_as_script=True,
    ignore_healthchecks=True,
)


with Flow(
    "name-me-something-relevant",
    storage = storage_obj,
    run_config=DockerRun(),
    # run_config=KubernetesRun(
    #         memory_request="1024Mi",
    #         memory_limit="2048Mi",
    #         cpu_request="1000m",
    #         cpu_limit="2000m",
    #         service_account_name="builder"
    #     ),
    # executor=LocalDaskExecutor(scheduler="processes", num_workers=(psutil.cpu_count(logical=True) - 1)),
    executor=LocalDaskExecutor(scheduler="processes", num_workers=8),
    result=S3Result(bucket="cam-prefect-results", boto3_kwargs={"credentials":{"ACCESS_KEY":aws_sec.ACCESS_KEY ,"SECRET_ACCESS_KEY": aws_sec.SECRET_KEY}})
) as flow:
    sf_env = Parameter("sf_env", required=True)





    df_out = read_from_sql.run(sf_env)

    list_of_nums = create_list_of_nunmber(df_out,upstream_tasks=[df_out])

    list_of_multiplied_nums = map_example.map(
        number=list_of_nums,
        sf_env=unmapped(sf_env),
        upstream_tasks=[list_of_nums],
    )

    done,done_log_message  = print_map_output(list_of_multiplied_nums,upstream_tasks=[df_out])




if __name__ == "__main__":
    flow.run(
        parameters=
        {

            "sf_env": "prod",


        }
    )



