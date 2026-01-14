import os
import math
from prefect.executors import LocalExecutor
import requests
from prefect.engine.results import S3Result
from prefect import Flow, Parameter, task
from prefect.storage import Docker
from sqlalchemy import create_engine
from common import aws_sec, sec
import flow_variables
import pandas as pd
from prefect.run_configs.docker import DockerRun

from prefect.run_configs.kubernetes import KubernetesRun
from common.config import  RunSettings
from common import sec, config

def get_correct_schema(env):
    if env == 'prod':
        return 'CPS_DSCI_API'
    else:
        return 'CPS_DSCI_BR'

def check_env(env):
    if env == "dev":
        cn = "dev_cps_dsci_etl_svc"
    elif env == "stage":
        cn = "stg_cps_dsci_etl_svc"
    elif env == "prod":
        cn = "prd_cps_dsci_etl_svc"
    else:
        cn = env

    return cn

def get_sec_dir(pth):
    return os.path.join(os.getcwd(), pth)

def chunk_df_to_size(data_df, chunk_size):
    if len(data_df) < chunk_size:
        print('df less than chunk_size ')
        return [data_df]

    else:
        total_length = len(data_df)
        total_chunk_num = math.ceil(total_length / chunk_size)
        normal_chunk_num = math.floor(total_length / chunk_size)
        chunks = []
        for i in range(normal_chunk_num):
            chunk = data_df[(i * chunk_size):((i + 1) * chunk_size)]
            chunks.append(chunk)
        if total_chunk_num > normal_chunk_num:
            chunk = data_df[(normal_chunk_num * chunk_size):total_length]
            chunks.append(chunk)
        return chunks

def log_to_dc_job_messages(sf_env,request_id, log_message):
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
def make_api_call(dc_engagement_id, auth_token, logged_user, df_chunks, env, request_id):
        print(env)
        res_log = []
        print("&&&&&&&&&&&&&&&&&&&&")
        print(logged_user)
        iter = 1

        for chunk in df_chunks:
            try:
                print(f"{dc_engagement_id}, {chunk[0]['Tag_ID'].values[:1][0]}, {chunk[0]['instance_id'].tolist()}")

                tag_request_json = {
                    "tag_id": int(chunk[0]['Tag_ID'].values[:1][0]),
                    "instance_ids": chunk[0]['instance_id'].tolist(),
                    "engagement_id": int(dc_engagement_id)
                }
            except:
                print("chunk[0]['Tag_ID'].values[:1][0]  did not work ")

            try:
                print(f"{dc_engagement_id}, {chunk['Tag_ID'].values[:1][0]}, {chunk['instance_id'].tolist()}")

                tag_request_json = {
                    "tag_id": int(chunk['Tag_ID'].values[:1][0]),
                    "instance_ids": chunk['instance_id'].tolist(),
                    "engagement_id": int(dc_engagement_id)
                }
            except:
                print("chunk['Tag_ID'].values[:1][0]  did not work ")

            logged_user_request_param = logged_user.replace('@', '%40')
            dev_endpoint = "devdatacanvaswf.cisco.com"
            prod_endpoint = "datacanvaswf.cisco.com"

            if env == 'prod':
                endpoint = prod_endpoint
            elif env == 'dev':
                endpoint = dev_endpoint

            full_request_uri = f'https://{endpoint}/api/v2/thought_spot/actions/set?logged_user={logged_user_request_param}'

            print(full_request_uri)
            headers = {'Authorization': f'Bearer {auth_token}', 'Content-Type': 'application/json'}

            r = requests.post(full_request_uri,
                              headers=headers, verify=False, json=tag_request_json)

            log_to_dc_job_messages(env, request_id,
                                   f"INFO: Completed API call {iter} for {dc_engagement_id} with response : Status Code: {r.status_code}")
            print(f"Status Code: {r.status_code}, Response: {r.json()}")
            res = r.json()
            res_log.append(res)
            iter += 1

        return res_log[0]

@task(log_stdout=True, nout= 3)
def parse_request_json(request_json):
    print(type(request_json))
    dc_engagement_id = request_json['engagement_id']


    return  dc_engagement_id

@task(log_stdout=True)
def get_user_id(requested_by,env):
    correct_schema = get_correct_schema(env)

    sf_env = check_env('prod')
    engine = create_engine(
        sec.get_sf_pw(sf_env, flow_variables.warehouseXsmall, correct_schema)
    )


    # tagsest_qry = f"""select tag_id, tagset_id from DC_TAGS where TAG_ID in (649,639,640,628,582, 13714, 581, 1414)"""
    tagsest_qry = f"""select * from {correct_schema}.DC_USERS where CISCO_CCO_ID = '{requested_by}'"""

    user_id_df = pd.read_sql(tagsest_qry,engine )

    return int(user_id_df['user_id'][0])



@task(log_stdout=True)
def get_aggregates():

    for env in ['dev','prod']:
        correct_schema = get_correct_schema(env)

        sf_env = check_env('prod')
        engine = create_engine(
            sec.get_sf_pw(sf_env, flow_variables.warehouseXsmall, correct_schema)
        )

        con = engine.connect()

        collector_aggregates_qry = f"""
                            create or replace transient table {correct_schema}.DC_EVIDENCE_ZONES as
                                        with obs as (select 'collector' as src,
                                                            h.DC_ENGAGEMENT_ID,
                                                            INSTANCE_ID::int INSTANCE_ID,
                                                            effective_date,
                                                            collection_date
                                                     from  {correct_schema}.DC_EVIDENCE_COLLECTOR_DETAILS d
                                                              join  {correct_schema}.DC_EVIDENCE_COLLECTOR_HDR h on d.request_id = h.request_id
                                                     where try_to_number(INSTANCE_ID) is not null
                                                     union
                                                     select 'customer' as src,
                                                            h.DC_ENGAGEMENT_ID,
                                                            INSTANCE_ID::int  INSTANCE_ID,
                                                            effective_date,
                                                            effective_date as collection_date
                                                     from  {correct_schema}.DC_EVIDENCE_CUSTOMER_DETAILS d
                                                              join {correct_schema}.DC_EVIDENCE_CUSTOMER_HDR h on d.request_id = h.request_id
                                                     where try_to_number(INSTANCE_ID) is not null
                                        )
                                        select
                                            listagg(distinct src, ',')  as sources ,
                                                DC_ENGAGEMENT_ID,
                                                INSTANCE_ID,

                                               min(collection_date) as first_discovery_date,
                                               MAX( collection_date) as last_discovery_date,  
                                               count(distinct(effective_date)) as discovery_count,  -- still count distinct dates of evidence
                                               datediff(day, last_discovery_date,CURRENT_TIMESTAMP) as DAYS_SINCE_LAST_DISCOVERY_DATE,
                                               CASE
                                                WHEN DAYS_SINCE_LAST_DISCOVERY_DATE <= 90
                                                   THEN 'Discovered <= 90 Days'
                                                WHEN DAYS_SINCE_LAST_DISCOVERY_DATE > 90
                                                   THEN 'Discovered > 90 Days'
                                                END as zone_duration
                                        from obs
                                        group by DC_ENGAGEMENT_ID, instance_id
                                        
                                        
         """


        print(collector_aggregates_qry)
        con.execute(collector_aggregates_qry)


    # customer_aggregates_qry = f"""create or replace table CPS_DSCI_BR.DC_EVIDENCE_CUSTOMER_ZONES as (
    #                         select  MAX(h.DC_ENGAGEMENT_ID) as DC_ENGAGEMENT_ID,
    #                                 MAX(INSTANCE_ID) as instance_id,
    #                                MAX(serial_number) as serial_number,
    #                                MAX(effective_date) as first_discovery_date, min(effective_date) as last_discovery_date,
    #                                count(distinct(effective_date)) as discovery_count,
    #                                datediff(day, CURRENT_TIMESTAMP, last_discovery_date) as DAYS_SINCE_LAST_DISCOVERY_DATE,
    #                                CASE
    #                                 WHEN DAYS_SINCE_LAST_DISCOVERY_DATE > -90
    #                                    THEN 'Discovered < 90 Days'
    #                                 WHEN DAYS_SINCE_LAST_DISCOVERY_DATE <= -90
    #                                    THEN 'Discovered >= 90 Days'
    #                                 END as zone_duration,
    #                                 'TBD' as zone
    #                         from {correct_schema}.DC_EVIDENCE_COLLECTOR_DETAILS d
    #                         join {correct_schema}.DC_EVIDENCE_COLLECTOR_HDR h on d.request_id = h.request_id
    #                         group by DC_ENGAGEMENT_ID, instance_id)"""
    #
    # con.execute(customer_aggregates_qry)



    return True





@task()
def get_global_ultimate(flow_params,run_settings):
    correct_schema = get_correct_schema(flow_params.sf_env)

    sf_env = check_env('prod')
    engine = create_engine(
        sec.get_sf_pw(sf_env, flow_variables.warehouseXsmall, correct_schema)
    )


    gu_qry = f"""select distinct h.GLOBAL_ULTIMATE_ID
                        from CPS_DSCI_API.DC_ENGAGEMENT_HDR e
                                  join CPS_DSCI_API.DC_PARTY_LINKS p  on (p.DC_ENGAGEMENT_ID = e.DC_ENGAGEMENT_ID and p.IS_DELETED = 'F')
                                  join EDW_MASTER_ETL_DB.SS.CRT_XXNGCR_DNM_RELATIONSHIPS h on (h.PARTY_ID=p.CR_PARTY_ID)
                        where e.IS_DELETED = 'F' and e.DC_ENGAGEMENT_ID = {flow_params.engagement_id} and nvl(h.edwsf_source_deleted_flag,'N') = 'N' """

    gu_df  = pd.read_sql(gu_qry,engine )

    return int(gu_df['global_ultimate_id'][0])

@task(log_stdout=True)
def get_run_settings() -> RunSettings:
    return config.RunSettings()

@task(log_stdout=True)
def get_flow_params(sf_env,engagement_id,request_id,requested_by) -> RunSettings:
    return config.FlowParams(engagement_id,sf_env,request_id,requested_by)


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
        "SQLAlchemy===1.4.35",
        "awswrangler==2.12.1",
        "fastparquet==0.7.2",
        "XlsxWriter==3.1.2",
        "oyaml==1.0",
        "networkx==2.8",
        "binpacking==1.5.2",
        "cloudpickle==2.0.0"

    ],
    # registry_url="containers.cisco.com/ejurotic",
    registry_url="837578041534.dkr.ecr.us-east-1.amazonaws.com/dc/p1",
    path="main.py",
    files={
        get_sec_dir('common/new_bulkload.py') : "/common/new_bulkload.py",
        get_sec_dir('common/sec.py') : "/common/sec.py",
        get_sec_dir('common/config.py') : "/common/config.py",
        get_sec_dir('common/aws_sec.py') : "/common/aws_sec.py",
        get_sec_dir('flow_variables.py'): "/flow_variables.py",
        get_sec_dir('common/sql_pool.py'): "/common/sql_pool.py",
        get_sec_dir('main.py'): "main.py",

    },
    env_vars={"PYTHONPATH": "${PYTHONPATH}:/"},
    stored_as_script=True,
    # ignore_healthchecks=True,

)

with Flow(
    "dc-evidence-zones-aggregates",
    storage=storage_obj,
        run_config=KubernetesRun(
            # memory_request="1024Mi",
            # memory_limit="2048Mi",
            # cpu_request="1000m",
            # cpu_limit="2000m",
            # service_account_name="builder",
            labels=["dev"]
        ),
    # run_config=DockerRun(labels=["thought-spot", "ds-server-docker"]),
    executor=LocalExecutor(),
    # executor=LocalDaskExecutor(scheduler="processes", num_workers=8),
    result=S3Result(bucket="cam-prefect-results", boto3_kwargs={"credentials":{"ACCESS_KEY":aws_sec.ACCESS_KEY ,"SECRET_ACCESS_KEY": aws_sec.SECRET_KEY}})
) as flow:
    # dc_engagement_id = Parameter("dc_engagement_id", required=False)
    # env = Parameter("env", required=True,default="dev")
    # list_of_tags = Parameter("list_of_tags", required=False)
    # request_id = Parameter("request_id", required=True),
    # request_json = Parameter("request_json", required=True),
    # bucket_name = Parameter("bucket_name", required=True),
    # file_location = Parameter("file_location", required=True),
    # requested_by = Parameter("requested_by", required=True)

    # dc_engagement_id  = parse_request_json(request_json[0])

    # user_id = get_user_id(requested_by, env)
    #
    # run_settings = get_run_settings(upstream_tasks=[dc_engagement_id ])
    #
    # flow_params = get_flow_params(env,dc_engagement_id,request_id[0],requested_by)

    aggregates_df = get_aggregates( )





if __name__ == "__main__":
    flow.run(

    )


