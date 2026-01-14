import getpass
import json
import logging
import os
import ast
import platform
import re
import sys
from prefect.triggers import all_successful, all_failed, all_finished
import math
from enum import Enum
import string
import random
from log_to_dc_job_messages import log_to_dc_job_messages, final_flow_state_message
from prefect.executors import LocalExecutor
from functools import wraps
from logging import LogRecord
from pathlib import Path
from prefect.tasks.prefect import RenameFlowRun
import requests
import boto3
import oyaml
from wb import package_workbook_for_instance
# import awswrangler as wr
from datetime import date
from prefect.engine.results import S3Result
from prefect import Flow, Parameter, task, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import Docker
import io
from prefect import unmapped
from sqlalchemy import create_engine, text, bindparam, Integer, column, String
from common import aws_sec, sec
import flow_variables
import pandas as pd
from prefect.run_configs.docker import DockerRun
from prefect.engine.signals import SKIP, FAIL
from prefect.run_configs.kubernetes import KubernetesRun
class Environment(str, Enum):
    DEV = "development"
    STG = "stage"
    PROD = "production"
from datetime import datetime
import prefect
import serial_prediction





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

@task()
def create_excel(
        dc_engagement_id: int,
        env : str

):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_sec.ACCESS_KEY,
        aws_secret_access_key=aws_sec.SECRET_KEY,
        region_name="us-east-1",
    )

    correct_schema = get_correct_schema(env)

    sf_env = check_env('prod')
    engine = create_engine(
        sec.get_sf_pw(sf_env, flow_variables.warehouseXsmall, correct_schema)
    )


    def get_engagement_tags(dc_engagement_id: int) -> list:
        get_tags_query = (
            text(
                """      select c.CAMS, 
                                c.BOOKING_CONTRACT, 
                                c.AM_START_DATE, 
                                c.AM_END_DATE, 
                                c.CONTRACT_NUMBER, 
                                c.ALLOWED_SERVICE_LEVELS,
                                ct.ASSET_MANAGEMENT_TYPE, 
                               ctt.SERVICE_CONTRACT_TYPE, 
                               mt.MONITOR_REASON, 
                               c.CREATED_BY, 
                               c.CREATE_DTM, 
                               c.UPDATE_DTM, 
                               c.UPDATED_BY,
                               c.DC_ENGAGEMENT_ID 
            from CPS_DSCI_API.dc_ENGAGEMENT_CONTRACTS c
            left join  CPS_DSCI_API.dc_CONTRACT_MONITOR_TYPES mt on ( mt.monitor_type_id = c.MONITOR_REASON_TYPE_ID  )
            left join  CPS_DSCI_API.dc_CONTRACT_ASSET_MGT_TYPES ct on ( ct.am_type_id =c.ASSET_MANAGEMENT_TYPE_ID )
            left join CPS_DSCI_API.dc_CONTRACT_TYPES ctt on (ctt.CONTRACT_TYPE_ID=c.SERVICE_CONTRACT_TYPE_ID)
            where c.DC_ENGAGEMENT_ID = :dc_engagement_id and c.IS_DELETED = 'F' """

            )
                .bindparams(
                bindparam("dc_engagement_id", dc_engagement_id, type_=Integer),
            )
        )

        with engine.connect() as conn:
            tags = conn.execute(get_tags_query)


        #     tags_parsed = parse_obj_as(list[GetTagRow], tags) if tags else []
        return tags

    tags = get_engagement_tags(dc_engagement_id=dc_engagement_id)

    df = pd.DataFrame(columns=["Asset Manager",
                               "AM Booking Contract",
                               "AM Contract Start Date",
                               "AM Contract End Date",
                               "Contract Number",
                               "Allowed Service Level",
                               "Asset Management Service Type",
                               "Service Contract Type",
                               "Monitor Reason"])

    for item in tags:
        df = df.append({"Asset Manager": item[0],
                        "AM Booking Contract": item[1],
                        "AM Contract Start Date": item[2],
                        "AM Contract End Date": item[3],
                        "Contract Number": item[4],
                        "Allowed Service Level": item[5],
                        "Asset Management Service Type": item[6],
                        "Service Contract Type": item[7],
                        "Monitor Reason": item[8],
                        },
                       ignore_index=True)

    # Placeholder for Contracts
    df_contracts = df[['Asset Manager',
                       'AM Booking Contract',
                       'AM Contract Start Date',
                       'AM Contract End Date',
                       "Contract Number",
                       'Allowed Service Level',
                       'Asset Management Service Type',
                       'Service Contract Type',
                       "Monitor Reason"

                       ]].convert_dtypes()

    # Hidden sheet with info
    df_info_sheet = pd.DataFrame(columns=["File_Type"],
                                 data=["DC_CONTRACTS_V1"])

    with io.BytesIO() as output:
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df_contracts.to_excel(writer, sheet_name="Contract Details", index=False)
            df_info_sheet.to_excel(writer, sheet_name="info", index=False)

            for sheet_name in writer.sheets.keys():
                writer.sheets[sheet_name].autofit()

            info_sheet = writer.sheets["info"]
            info_sheet.hide()

        data = output.getvalue()



        current_date = date.today()
        file_key = f"extract-contracts/Managed_Contract_Type_Data_{dc_engagement_id}_{current_date}.xlsx"
        file_name = f"Managed_Contract_Type_Data_{dc_engagement_id}_{current_date}.xlsx"

        response = s3_client.put_object(
            Bucket="dc-generic-upload-outputs", Key=file_key, Body=data
        )

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 put_object response. Status - {status}")
        else:
            print(f"Unsuccessful S3 put_object response. Status - {status}")







@task(log_stdout=True)
def demo_cognito_api_auth(env,service_name,region_name):
    secret_id = f"{env}/Cognito"


    session = boto3.session.Session(aws_access_key_id=aws_sec.ACCESS_KEY, aws_secret_access_key=aws_sec.SECRET_KEY)
    client_ssm = session.client(
        service_name=service_name,
        region_name=region_name,
        aws_access_key_id=aws_sec.ACCESS_KEY,
        aws_secret_access_key=aws_sec.SECRET_KEY,
    )
    cognito_secret_raw = json.loads(
        client_ssm.get_secret_value(SecretId=secret_id)["SecretString"]
    )


    cognito_client = session.client(
        service_name="cognito-idp",
        region_name=region_name,
    )




    response_raw = cognito_client.admin_initiate_auth(
        UserPoolId=cognito_secret_raw['UserPoolId'],
        ClientId=cognito_secret_raw['ClientId'],
        AuthFlow=cognito_secret_raw['AuthFlow'],
        AuthParameters={
            "USERNAME": cognito_secret_raw['USERNAME'],
            "PASSWORD": cognito_secret_raw['PASSWORD'],
        },
    )


    AuthenticationResult = response_raw['AuthenticationResult']
    Access_Token = AuthenticationResult['AccessToken']

    return Access_Token


@task()
def updated_tags(list_of_tags, env):


    correct_schema = get_correct_schema(env)

    sf_env = check_env('prod')
    engine = create_engine(
        sec.get_sf_pw(sf_env, flow_variables.warehouseXsmall, correct_schema)
    )


    get_tags_query = (
        text(
            f"""update CPS_DB.{correct_schema}.DC_TAGS set IS_DELETED ='T' where TAG_ID in (:list_of_tags)"""

        )
            .bindparams(
            bindparam("list_of_tags", list_of_tags),
        )
    )

    with engine.connect() as conn:
        tags = conn.execute(get_tags_query)



    return True


@task(log_stdout=True,tags=["snowflake_xsmall"])
def update_generic_upload_log_table(request_id, new_path,object_key):

    update_metadata_query = f"""
    UPDATE CPS_DB.CPS_BIA_BR.DATA_CANVAS_GENERIC_UPLOAD set STATUS = 'Success',
                                                OUTPUT_FILE_PATH ='{new_path}'               
    where REQUEST_ID = {request_id};
    """



    print("test 1")
    engine = create_engine(
        sec.get_sf_pw('prd_cps_dsci_etl_svc', 'CPS_DSCI_ETL_EXT1_WH', 'prd_cps_dsci_etl_svc')
    )
    con = engine.connect()

    print("test 2")
    print(update_metadata_query)
    try:
        con.execute(update_metadata_query)
    except Exception as e:
        print(e)
        print("Data for this request_id is not in CPS_DB.CPS_BIA_BR.DATA_CANVAS_GENERIC_UPLOAD")
        pass


    return True






@task()
def update_gu_table(env,request_id,api_response):
    correct_schema = get_correct_schema(env)

    sf_env = check_env('prod')
    engine = create_engine(
        sec.get_sf_pw(sf_env, flow_variables.warehouseXsmall, correct_schema)
    )



    update_gu_table_query = (
        text(
            f"""    UPDATE CPS_DB.{correct_schema}.DC_GENERIC_UPLOAD set STATUS = 'Success',
                                                OUTPUT_FILE_PATH ='Output is in DC'    , ERROR_MESSAGE = '{json.dumps(api_response)}'         
                    where REQUEST_ID = :request_id;
                ;"""

        )
            .bindparams(
            bindparam("request_id", request_id),
        )
    )

    with engine.connect() as conn:
        tags = conn.execute(update_gu_table_query)



    return True






@task(log_stdout=True)
def make_api_call(dc_engagement_id, auth_token, logged_user, df_chunks, env, request_id):
        print(env)
        res_log = []
        print("&&&&&&&&&&&&&&&&&&&&")
        print(logged_user)
        iter_count = 1

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
                                   f"INFO: Completed API call {iter_count} for {dc_engagement_id} with response : Status Code: {r.status_code}",
                                   flow_params.requested_by, flow_params.notification_id)
            print(f"Status Code: {r.status_code}, Response: {r.json()}")
            res = r.json()
            res_log.append(res)
            iter_count += 1

        return res_log[0]

def get_sec_dir(pth):
    return os.path.join(os.getcwd(), pth)




@task(log_stdout=True)
def get_tagset_ids(id_df,env,request_id):



    tag_ids_list = id_df['Tag_ID'].tolist()

    tag_ids_list.append(-1)


    tag_ids_list = set(tag_ids_list)
    tag_ids = tuple(tag_ids_list)

    print(tag_ids)
    correct_schema = get_correct_schema(env)

    sf_env = check_env('prod')
    engine = create_engine(
        sec.get_sf_pw(sf_env, flow_variables.warehouseXsmall, correct_schema)
    )

    if id_df.empty:
        raise FAIL()

    # tagsest_qry = f"""select tag_id, tagset_id from DC_TAGS where TAG_ID in (649,639,640,628,582, 13714, 581, 1414)"""
    tagsest_qry = f"""select tag_id, tagset_id from {correct_schema}.DC_TAGS where TAG_ID in {tag_ids}"""


    tagsets_df = pd.read_sql(tagsest_qry,engine )
    #TODO add logging to check for if this is empty, if so the tagid they entered doesnt exsist


#     tagsets_df = tagsets_df.rename(columns={"tag_id": "Tag_ID"}, errors="raise")

    print(id_df.info())


    try:
        id_df = id_df.astype('int64')
    except ValueError as ve:
        print(ve)
        log_to_dc_job_messages(env, request_id,
                               f"USER ERROR: User input non numerical values in the InstanceID - Tag mapping sheet.",
                               flow_params.requested_by, flow_params.notification_id)

    tagsets_df = tagsets_df.astype('int64')
    id_df['Tag_ID'] = id_df['Tag_ID'].astype('int64')

    joined_df = id_df.merge(tagsets_df, left_on = 'Tag_ID', right_on = 'tag_id')

    joined_df = joined_df.drop(columns=['tag_id'])
    print("JOINED DF ")
    print(joined_df)
    if tagsets_df.empty:
        log_to_dc_job_messages(env, request_id, f"FAILED: Tag_id(s) do not exsist in {correct_schema}.DC_TAGS.",
                               flow_params.requested_by, flow_params.notification_id)
    else:
        log_to_dc_job_messages(env, request_id,
                               f"SUCCESS: Joined uploaded excel with data from {correct_schema}.DC_TAGS.",
                               flow_params.requested_by, flow_params.notification_id)
    return joined_df


@task(log_stdout=True, nout = 2)
def check_if_clean(full_df,env, request_id,):
    grouped = full_df.groupby(['instance_id','tagset_id']).tagset_id.count()
    list_of_dups = grouped[grouped > 1].index.tolist() #this returns a list of tuples

    list_of_ids_tagged_twice_with_same_tagset = []
    for i in list_of_dups: # extracting the instance ids we need to remove.
        list_of_ids_tagged_twice_with_same_tagset.append(i[0])


    if len(list_of_ids_tagged_twice_with_same_tagset) > 0:
        log_to_dc_job_messages(env, request_id,
                               f"FAILED: These Instance_Ids were tagged multiple times with tags from the same tagset {list_of_ids_tagged_twice_with_same_tagset}",
                               flow_params.requested_by, flow_params.notification_id)
        print(f"""These Instance_Ids were tagged multiple times with tags from the same tagset {list_of_ids_tagged_twice_with_same_tagset}""")


    cleaned_df = full_df[~full_df['instance_id'].isin(list_of_ids_tagged_twice_with_same_tagset)] # create a df with all rows where Instance_Id is not in the list

    print(cleaned_df)


    return cleaned_df, list_of_ids_tagged_twice_with_same_tagset


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



@task(log_stdout=True)
def prepare_df_for_api_call(split_dfs):
    """
    This returns a list of lists, where each sublist contains one or more dfs having one tag_id,
    depending on the size of requested split.

    :param split_dfs:
    :return:
    """
    final_list = []
    for i in split_dfs:
        final_list.append(chunk_df_to_size(i, 100000))

    # print(final_list)
    return final_list


@task()
def split_df_by_tag_id(cleaned_df):
    gb = cleaned_df.groupby(['Tag_ID'])
    split_dfs = [gb.get_group(x) for x in gb.groups]


    return split_dfs


def split_s3_path(s3_path):
    path_parts=s3_path.replace("s3://","").split("/")
    bucket=path_parts.pop(0)
    key="/".join(path_parts)
    return bucket, key

@task(log_stdout=True)
def get_json_from_s3(url):
    print("access_key=", aws_sec.ACCESS_KEY)

    print("bucket",url)


    bucket, key = split_s3_path(url)

    session = boto3.Session(
        aws_access_key_id=aws_sec.ACCESS_KEY, aws_secret_access_key=aws_sec.SECRET_KEY
    )

    s3 = session.resource("s3")
    obj = s3.Object(bucket, key)
    data = obj.get()["Body"].read().decode("utf-8")
    json_data = oyaml.safe_load(data)
    return json_data


@task(nout=2)
def make_params(json):
    # print(json)
    logged_user = f"{json['requestedBy']}"
    dc_engagement_id = json['engagementId']
    return dc_engagement_id, logged_user




@task()
def rename_flow(request_id):
    letters = string.ascii_letters
    random_string = '{}'.format(''.join(random.choice(letters) for i in range(10)))
    RenameFlowRun().run(flow_run_name=f"""{request_id}-{random_string}""")
    return True


# def log_to_dc_job_messages(sf_env,request_id, log_message):
#     cn = check_env('prod')
#     correct_schema = get_correct_schema(sf_env)
#
#     engine = create_engine(
#         sec.get_sf_pw(cn, flow_variables.warehouseXsmall, correct_schema)
#     )
#
#     con = engine.connect()
#
#
#     bia_qry = f"""
#     insert into {correct_schema}.dc_job_messages(request_id,logged_message) values ({request_id},'{log_message}')
#     """
#
#     try:
#         con.execute(bia_qry)
#     except Exception as e:
#         print(e)
#         print(
#             f"Failed while attempting to log message to : {correct_schema}.dc_job_messages"
#         )
#
#
#
#
#     return True


@task(log_stdout=True)
def looped_task(dc_engagement_id,demo_cognito_api_auth_result,logged_user,df_chunks,env, request_id):
    responses = []

    for i in df_chunks:
        api_response = make_api_call.run(dc_engagement_id = dc_engagement_id,
                                     auth_token = demo_cognito_api_auth_result,
                                     logged_user = logged_user,
                                     df_chunks = i,
                                     env=env,
                                         request_id=request_id,
                                         )
        responses.append(api_response)

    return responses



@task()
def call_tagging_resolution_flow(request_json):
    PREFECT_AUTH_TOKEN = 'TKj2Eq9X0FJmV8LN2k3hPA'

    prefect_client = prefect.Client(api_key=PREFECT_AUTH_TOKEN)



    triggered = prefect_client.create_flow_run(
        version_group_id="43e9bc60-ebfd-4c41-ba38-f64fc68964c7",
        labels=["dev"],
        parameters=dict(
            env=request_json['env'],
            engagement_id=request_json['engagement_id'],
            requested_by=request_json['requested_by'],
            serial_numbers=request_json['cols'],
        ))



    return True



@task(log_stdout=True, nout = 8)
def parse_request_json(request_json_loc):
    request_json = get_json_from_s3.run(request_json_loc)

    serial_numbers = []
    try:
        for i in request_json['rows']:
            if i['serial_number']:
                serial_numbers.append(i['serial_number'])
    except:
        print("No serial numbers found")


    instance_ids = []
    try:
        for i in request_json['rows']:
                instance_ids.append(int(i['instance_id']))
    except:
        print("No instance_ids found")


    if len(serial_numbers) == 0:
        run_type = 'instance_id'
    else:
        run_type = 'serial_number'



    effective_date = request_json['effective_date']

    file_name_id =request_json['file_name_id']
    source = request_json['source']
    note = request_json['note']





    return serial_numbers, effective_date, run_type, file_name_id, source, note, request_json,instance_ids



@task(log_stdout=True, nout = 2)
def build_df_for_wb(multi_same_parent_df,multi_resolved_df,df_resolved_result,flow_params,request_json):
    print(multi_same_parent_df.head())
    print(multi_resolved_df.head())
    print(df_resolved_result.head())



    multi_same_parent_df_a = multi_same_parent_df.drop(multi_same_parent_df.columns.difference(['serial_number','instance_id']), 1)

    multi_resolved_df_a = multi_resolved_df.drop(multi_resolved_df.columns.difference(['serial_number','instance_id']), 1)

    df_resolved_result_a = df_resolved_result.drop(df_resolved_result.columns.difference(['serial_number','instance_id']), 1)


    df = pd.concat([multi_same_parent_df_a, multi_resolved_df_a,df_resolved_result_a], ignore_index=True,  axis=0
              )

    # cehck for missing serials
    # missing_serials_df = ~df[df['serial_number'].isin(flow_params.serial_numbers)]

    found_serial_set= set(df["serial_number"])
    requested_serial_set = set(flow_params.serial_numbers)
    missing_serials = requested_serial_set.difference(found_serial_set)

    missing_serials = list(missing_serials)
    missing_serials_df = pd.DataFrame(missing_serials, columns=['serial_number'])

    df['dc_engagement_id'] = flow_params.engagement_id


    print("************** DF ")
    print(df)




    full_df = build_df_from_json.run(request_json)


    print("************** full_df ")
    print(full_df)


    full_df = full_df[full_df['serial_number'].isin(df['serial_number'].tolist())]


    full_merge_df = pd.merge(full_df, df[['serial_number','instance_id']], on='serial_number', how='left',suffixes = ('_x',None) )

    full_merge_df = full_merge_df.drop(['instance_id_x'], axis=1)

    print("############## full_merge_df")
    print(full_merge_df)

    return missing_serials_df, full_merge_df

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
def build_query_params_p(full_df, dc_engagement_id, request_id, effective_date):

    query_params=  {
            "run_id": request_id,
            "engagement_id": dc_engagement_id,
            "customer": [
                {"instance_id": 1,
                 "serial_number": "123456789",
                 "effective_date": "2021-01-01"}
            ],
            "collector": [
            ],
            "output_prefix": "s3://cam-prefect-results",
        }

    for row,index in full_df.iterrows():
        query_row = {"instance_id": row["instance_id"],
                 "serial_number": row["serial_number"],
                 "effective_date": effective_date}
        query_params['customer'].append(query_row)


    print(query_params)

    return query_params



@task(log_stdout=True)
def build_df_from_json(request_json):
    request_df = pd.DataFrame(request_json['rows'])
    return request_df

@task(log_stdout=True)
def log_results(full_df,file_name_id, source, note , effective_date, sf_env,requested_by,request_id,flow_params):
    cn = check_env('prod')
    correct_schema = get_correct_schema(sf_env)

    engine = create_engine(
        sec.get_sf_pw(cn, flow_variables.warehouseXsmall, correct_schema)
    )

    con = engine.connect()
    date_created = datetime.now().isoformat()

    hdr_qry = f"""
    insert into {correct_schema}.DC_EVIDENCE_CUSTOMER_HDR(request_id,
                                                            create_dtm,
                                                            created_by,
                                                            file_name_id,
                                                            effective_date,
                                                            source,
                                                            note,
                                                            dc_engagement_id)
                                values ({request_id},
                                    '{date_created}',
                                    '{requested_by}',
                                    '{file_name_id}',
                                    '{effective_date}',
                                    '{source}',
                                    '{note}',
                                    {flow_params.engagement_id}
                                    )
    """

    try:
        con.execute(hdr_qry)
    except Exception as e:
        print(e)
        print(
            f"Failed while attempting to log message to : {correct_schema}.DC_EVIDENCE_CUSTOMER_HDR"
        )

    # if 'full_address' in full_df.columns:
    #     full_df = full_df.reindex(columns=['street_address', 'city', 'state', 'country'])
    # else:
    #     full_df = full_df.reindex(columns=['full_address'])

    # if 'serial_number' not in full_df.columns:
    #     full_df['serial_number'] = None


    full_df['REQUEST_ID'] = request_id

    full_df= full_df[pd.to_numeric(full_df['instance_id'], errors='coerce').notnull()]

    try:
        full_df.to_sql(
            f"DC_EVIDENCE_CUSTOMER_DETAILS".lower(),
            engine,
            schema=correct_schema,
            index=False,
            if_exists="append",
            chunksize=15000

        )
        # log_to_dc_job_messages(sf_env, request_id,
        #                        f"SUCCESS: Adding {pinboard_name} to Data Canvas UI.")
    except Exception as e:
        # log_to_dc_job_messages(sf_env, request_id, f"FAILED: Adding {pinboard_name} to Data Canvas UI.")
        print(e)

    full_df_len = len(full_df)

    return full_df_len

@task(log_stdout=True)
def remove_unknown_instance_ids(unknown_instance_ids_result, full_df):
    cleaned_full_df = full_df[~full_df['instance_id'].isin(unknown_instance_ids_result)]
    print("&&&& cleaned_full_df")
    print(cleaned_full_df)
    return cleaned_full_df

@task(log_stdout=True, nout = 2)
def empty_dfs_for_workbook():
    multi_same_parent_df = pd.DataFrame(columns=['serial_number', 'score_rank']),
    df_query = pd.DataFrame(columns=['serial_number', 'install_base_status']),
    return multi_same_parent_df, df_query

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
        get_sec_dir("log_to_dc_job_messages.py"): "/log_to_dc_job_messages.py",
        get_sec_dir('flow_variables.py'): "/flow_variables.py",
        get_sec_dir('common/sql_pool.py'): "/common/sql_pool.py",
        get_sec_dir('serial_prediction.py'): "serial_prediction.py",
        get_sec_dir('main.py'): "main.py",
        get_sec_dir('wb.py'): "wb.py",

    },
    env_vars={"PYTHONPATH": "${PYTHONPATH}:/"},
    stored_as_script=True,
    ignore_healthchecks=True,

)

with Flow(
    "dc-evidence-customer",
    storage=storage_obj,
    run_config=KubernetesRun(
        labels=["dev"],
        job_template={
        "apiVersion": "batch/v1",
        "kind": "Job",
        "spec": {
            "template": {
                 "spec": {
                     "nodeSelector": {
                        "cam-backend-access": "true"
                      },
                     "containers": [
                         {"name":"flow"}
                     ]
                  }
            }
        }
    }
    ),
    # run_config=DockerRun(labels=["thought-spot", "ds-server-docker"]),
    # executor=LocalDaskExecutor(scheduler="processes", num_workers=8),
    executor=LocalExecutor(),
    result=S3Result(bucket="cam-prefect-results", boto3_kwargs={"credentials":{"ACCESS_KEY":aws_sec.ACCESS_KEY ,"SECRET_ACCESS_KEY": aws_sec.SECRET_KEY}})
) as flow:
    dc_engagement_id = Parameter("dc_engagement_id", required=False)
    env = Parameter("env", required=True)
    # list_of_tags = Parameter("list_of_tags", required=False)
    request_id = Parameter("request_id", required=True),
    request_json_loc = Parameter("request_json_loc", required=True),
    # bucket_name = Parameter("bucket_name", required=True),
    # file_location = Parameter("file_location", required=True),
    requested_by = Parameter("requested_by", required=True)
    notification_id = Parameter("notification_id", required=False, default=0)


##########   Input form, and params ############

    serial_numbers, effective_date, run_type, file_name_id, source, note,request_json,instance_ids  = parse_request_json(request_json_loc[0])


##################################################SERIAL RESOLUTION#####################################################################################################################################

    with case(run_type, 'serial_number'):

        run_settings = serial_prediction.get_run_settings(upstream_tasks=[serial_numbers,dc_engagement_id ])

        #this is only happening for testing, this should be happening in the API layer
        # request_id = serial_prediction.get_next_seq_val(env, run_settings,upstream_tasks=[run_settings])

        flow_params = serial_prediction.get_flow_params(env,dc_engagement_id,serial_numbers,request_id[0],requested_by,instance_ids,notification_id)



        logged = serial_prediction.log_to_serial_resolution_table("Running","insert", run_settings=run_settings,flow_params=flow_params,upstream_tasks=[flow_params]
            )

        user_id = get_user_id(requested_by,env)

        transient_loader_table_result = serial_prediction.create_loader_transient_table(
             run_settings=run_settings, flow_params = flow_params,upstream_tasks=[logged]
        )

        #find all instance_id that has been tagged with 1411 in DC_ENGAGEMENT_TAGS_
        df_resolved_result = serial_prediction.query_engagement_resolved_serials(
            flow_params=flow_params, run_settings=run_settings, upstream_tasks=[transient_loader_table_result]
        )
        #
        #all where instance id is NULL from SN_LOADER_{flow_params.request_id}_TMP , transient_loader_table_result
        unknown_serials_result = serial_prediction.query_unknown_serials(
            transient_loader_fqn=transient_loader_table_result, flow_params=flow_params,upstream_tasks=[df_resolved_result],
        )
        # create big prepped table
        prepped_transient_table_result = serial_prediction.create_prepped_transient_table(
            flow_params=flow_params,
            run_settings=run_settings,
            upstream_tasks=[unknown_serials_result],
        )

        #get all lines from SN_PREPPED_{flow_params.request_id}_TMP where instance and serial is NULL  in SN_LOADER_{flow_params.request_id}_TMP , transient_loader_table_result
        unscoped_serials_result = serial_prediction.query_unscoped_serials(
            flow_params=flow_params,
            run_settings=run_settings,
            upstream_tasks=[prepped_transient_table_result],
        )

        #gets all lines, marks as multi or not, scores multi lines
        resolved_table_uri = serial_prediction.create_decision_data_table(
            flow_params=flow_params,
            run_settings=run_settings,
            upstream_tasks=[unscoped_serials_result],
        )



        demo_cognito_api_auth_result = serial_prediction.demo_cognito_api_auth(env,
            region_name = "us-east-1", service_name="secretsmanager", upstream_tasks=[resolved_table_uri]
        )


        # querys for multi same parent , and multi ranked 1 from SN_RESOLVED_{flow_params.request_id}_TMP
        multi_same_parent_df,multi_resolved_df, multi_resolved_picks_df, multi_same_parent_picks__df = serial_prediction.get_df_for_api_call(
            df_resolved_result,
            flow_params=flow_params,
            run_settings=run_settings,

            upstream_tasks=[demo_cognito_api_auth_result],
        )


        df_chunks = serial_prediction.prepare_df_for_api_call([multi_same_parent_picks__df,multi_resolved_picks_df], upstream_tasks=[multi_same_parent_df,multi_resolved_df, multi_resolved_picks_df, multi_same_parent_picks__df])



        api_response = serial_prediction.looped_task(run_settings,flow_params,demo_cognito_api_auth_result,df_chunks,env,request_id[0],upstream_tasks=[df_chunks])
        #
        #
        response_to_log = serial_prediction.build_error_response(api_response,flow_params, request_id[0],upstream_tasks=[api_response])

        #builds df of needed shape to pass to Serial Tagging
        missing_serials_df, full_df = build_df_for_wb(multi_same_parent_df,multi_resolved_df,df_resolved_result,flow_params,request_json,
                                                        upstream_tasks=[ response_to_log])

        excel_uri, json_uri,metrics_json = serial_prediction.package_workbook(
            multi_same_parent_df = multi_same_parent_df,
            df_query=multi_resolved_df,
            df_resolved=df_resolved_result,
            #
            unscoped=unscoped_serials_result,
            unknown=missing_serials_df,
            flow_params=flow_params,
            run_settings=run_settings,
            upstream_tasks=[missing_serials_df],)


        logged_2 = serial_prediction.log_to_serial_resolution_table("Success","update", run_settings=run_settings,flow_params=flow_params,api_response = response_to_log, upstream_tasks=[excel_uri, json_uri],
            )




        all_logged_len =log_results(full_df,file_name_id, source, note , effective_date, env,requested_by, request_id[0],flow_params,upstream_tasks=[full_df])

        demo_cognito_api_auth_result2 = demo_cognito_api_auth(env,
                                                              region_name="us-east-1", service_name="secretsmanager",
                                                              upstream_tasks=[all_logged_len]
                                                              )

        all_done = final_flow_state_message(env, notification_id, requested_by, flow_params,upstream_tasks=[demo_cognito_api_auth_result2]
                                            )

        # api_response3 =  serial_prediction.make_api_call_to_notifications(run_settings,
        #                                                                   dc_engagement_id,
        #                                                                   demo_cognito_api_auth_result,
        #                                                                   requested_by,
        #                                                                   metrics_json,
        #                                                                   env,
        #                                                                   request_id[0],
        #                                                                   user_id,
        #                                                                   flow_params,
        #                                                                   all_logged_len,
        #                                                                   run_type,
        #                                                                   upstream_tasks=[demo_cognito_api_auth_result2])

    with case(run_type, 'instance_id'):
        flow_params = serial_prediction.get_flow_params(env,dc_engagement_id,serial_numbers,request_id[0],requested_by,instance_ids,notification_id)
        run_settings = serial_prediction.get_run_settings(upstream_tasks=[serial_numbers, dc_engagement_id])
        user_id = get_user_id(requested_by, env)
        full_df = build_df_from_json(request_json)

        transient_instance_loader_table_result = serial_prediction.create_instance_loader_transient_table(
            flow_params = flow_params,upstream_tasks=[full_df]
        )

        #all where instance id is NULL from SN_LOADER_{flow_params.request_id}_TMP , transient_loader_table_result
        unknown_instance_ids_result ,found_instance_ids_df= serial_prediction.query_unknown_instance_ids(
            transient_loader_fqn=transient_instance_loader_table_result,flow_params=flow_params,upstream_tasks=[transient_instance_loader_table_result ],
        )


        multi_same_parent_df, df_query = empty_dfs_for_workbook(upstream_tasks=[unknown_instance_ids_result ,found_instance_ids_df ])

        excel_uri, json_uri,metrics_json = package_workbook_for_instance(
            multi_same_parent_df = multi_same_parent_df,
            df_query= df_query,
            df_resolved=found_instance_ids_df,
            #
            unscoped=[],
            unknown=unknown_instance_ids_result,
            flow_params=flow_params,
            run_settings=run_settings,
            upstream_tasks=[multi_same_parent_df, df_query ])

        cleaned_full_df = remove_unknown_instance_ids(unknown_instance_ids_result, full_df,upstream_tasks=[excel_uri, json_uri,metrics_json])

        all_logged_len = log_results(cleaned_full_df, file_name_id, source, note, effective_date, env, requested_by, request_id[0],flow_params,
                                 upstream_tasks=[cleaned_full_df])

        demo_cognito_api_auth_result2 = demo_cognito_api_auth(env,
                                                              region_name="us-east-1", service_name="secretsmanager",
                                                              upstream_tasks=[all_logged_len]
                                                              )

        all_done = final_flow_state_message(env, notification_id, requested_by, flow_params,upstream_tasks=[demo_cognito_api_auth_result2]
                                            )

    #     api_response3 = serial_prediction.make_api_call_to_notifications(run_settings,
    #                                                                      dc_engagement_id,
    #                                                                      demo_cognito_api_auth_result2,
    #                                                                      requested_by,
    #                                                                      metrics_json,
    #                                                                      env, request_id[0],
    #                                                                      user_id,
    #                                                                      flow_params,
    #                                                                      all_logged_len,
    #                                                                      run_type,
    #                                                                      upstream_tasks=[demo_cognito_api_auth_result2])
    #
    #
    # all_done = final_flow_state_message(env, notification_id, requested_by)

    # all_done = final_flow_state_message(env, notification_id, requested_by, flow_params,
    #                                    )

    # query_params_p = build_query_params_p(full_df,dc_engagement_id, request_id,effective_date)
    #
    #
    # # query_params_p needs to look like query_param in the above params
    # prog_flow_params = validate_flow_params(params=query_params_p)

    # stored_records_result = store_records(params=flow_params, env=env)

####################################################SERIAL TAGGING###################################################################################################################################

###### Merge in to collector or customer file ########
    #
    #
    #
    # renamed = rename_flow(request_id[0],upstream_tasks=[logged_2])
    #
    #
    # full_df = get_tagset_ids(id_df,env,request_id[0], upstream_tasks=[renamed])
    #
    #
    #
    # # list of dfs split by tag_id
    # split_df = split_df_by_tag_id(full_df, upstream_tasks=[full_df])
    #
    # # for each tag_id chunk, chunk to 100000 for api call
    # df_chunks2 = prepare_df_for_api_call(split_df, upstream_tasks=[split_df])
    #
    #
    # demo_cognito_api_auth_result2 = demo_cognito_api_auth(env,
    #     region_name = "us-east-1", service_name="secretsmanager", upstream_tasks=[df_chunks2]
    # )

    # #df_chunks is a list of df that are smaller than 100000 and only one tag_id per df
    # api_response2 = looped_task(dc_engagement_id,demo_cognito_api_auth_result2,requested_by,df_chunks2,env,request_id[0],upstream_tasks=[demo_cognito_api_auth_result2])
    #
    #
    #
    #
    # response_to_log2 = build_error_response(api_response2,env, request_id[0] ,upstream_tasks=[api_response2])
    #
    #
    # tags_updated = update_gu_table(env,request_id[0],response_to_log , upstream_tasks=[response_to_log2])
    #
    #
    #
    # api_response3 =  serial_prediction.make_api_call_to_notifications(run_settings,dc_engagement_id,demo_cognito_api_auth_result, requested_by, metrics_json, env,request_id[0],user_id,flow_params, upstream_tasks=[tags_updated])





if __name__ == "__main__":
    flow.run(
            parameters=               {
  "dc_engagement_id": 94,
  "env": "dev",
  "notification_id": 10502,
  "request_id": 212257,
  "request_json_loc": "s3://dc-json-requests/dev/customer_file/212257.json",
  "requested_by": "ejurotic@cisco.com"
}
    )



# if __name__ == "__main__":
#     flow.run(
#             parameters=          {
#           "env": "dev",
#         "requested_by": "ejurotic@cisco.com",
#           "request_json": {"tag_ids": [13299,1377 ], "serial_numbers": [
#               "12345",
#               "1",
#               "qwerty",
#               "FOC1927W13W",
#               "ONT204500BL",
#               "WZP23450RHK",
#               "797A8M5400721",
#               "FCH23507J69",
#               "NDG19043286",
#               "FIW235100BT",
#               "788E5A8202276",
#               "DAB2134GQZY",
#               "LIT231722KN",
#               "49M0A02JFJRG",
#               "K13V000022160D191D",
#               "NDG19010234",
#               "JAF1722BHQB",
#               "PST2510M1FA",
#               "FOC1717Y1J5",
#               "DCH1841J0J6",
#               "FOC1949X1LR",
#               "JAE1908026D",
#               "CAT1750S612",
#               "FOC1533V133",
#               "SPC1808038Z",
#               "FCW2327A3JL",
#               "JAE191701AC",
#               "AVD2330D5Y4",
#               "ECL131503FL",
#               "FCW2240G0MW",
#               "AVM2011U4N7",
#               "JUR2017H2TV",
#               "FCW2334A15B",
#               "FNS20180VPT",
#               "AVM1926U0YB",
#               "SSI15420MM5",
#               "AVD21209GL2",
#               "AVM1925U3MB",
#               "FDO2045A020",
#               "AVM1926U0Y8",
#               "JUR2017H0EZ",
#               "SAL17120Y5Q",
#               "DCA202814BE",
#               "AGJ1805RB2B",
#               "FDO2001A036",
#               "FDO22410USN",
#               "JUR2017H0PL",
#               "LIT2606AUW3",
#               "JUR2015H5H9",
#               "Y0YX02002523BD856F",
#               "FCW2239C19M",
#               "FCW2337D05E",
#               "ART2137FCWL",
#               "FDO21382KKU",
#               "FLM2008W14F",
#               "JAE194003TH",
#               "FNS222312VM",
#               "FCH221077LB",
#               "AVM1926U1DD",
#               "FDO2001A0SY",
#               "FDO21010Y9P",
#               "AZS16460CQE",
#               "FDO2024A0WK",
#               "FNS17481X9W",
#               "JAE23080JTY",
#               "FOC2245Q18S",
#               "FOX1632XFJF",
#               "ART2112F6SU",
#               "FDO2042A0K2",
#               "JAE23080JA4",
#               "FOF2217N295",
#               "FOC1744N67J",
#               "FDO2002A04H",
#               "FNS24220HXX",
#               "DAB2635JCEW",
#               "FOF2212N0UC",
#               "FOF2213N0N2",
#               "FDO2040A0LA",
#               "FCW1952D02V",
#               "JAE23051297",
#               "STP254413AQ",
#               "LIT210924S9",
#               "AGA15234LNG",
#               "JAE23080J7Q",
#               "ECL1828002C",
#               "FNS25180V70",
#               "AVD2231D1SP",
#               "FNS24430V35",
#               "FOF2212N0W2",
#               "AVM2132U0KK",
#               "JUR2030G2U0",
#               "AVD2046AWA0",
#               "AVD2342D7SN",
#               "FGE180205DT",
#               "OPM24380FU9",
#                 ], "engagement_id": 1208, "comment": "ej_test"},
#           "request_id": 211475
#         }
#     )
#
# [2024-03-06 15:31:24-0500] INFO - prefect.TaskRunner | # Unknown: 0
# [2024-03-06 15:31:24-0500] INFO - prefect.TaskRunner | # Unscoped: 0
# [2024-03-06 15:31:24-0500] INFO - prefect.TaskRunner | # Single: 20
# [2024-03-06 15:31:24-0500] INFO - prefect.TaskRunner | # Multi: 75
# [2024-03-06 15:31:24-0500] INFO - prefect.TaskRunner | # Multi_Same_Parent: 0
# [2024-03-06 15:31:24-0500] INFO - prefect.TaskRunner | # Resolved: 0


# if __name__ == "__main__":
#     flow.run(
#             parameters=          {
#           "env": "dev",
#             "requested_by": "ejurotic@cisco.com",
#           "request_json": {"tag_ids": [1417,1411 ], "serial_numbers": [
#                     #unkown
#             "FTX1745K1AS123",
#             "FTX1430S38A123",
#             "DCH1827V0GM123",
#             "FTT201101HE123",
#             "FCH2045FYCB123",
#             "FDO1747Y1TE123",
#             "NWG0804007B123",
#             "SAL1424KACS123",
#                     # single
#                     "FTX1745K1AS",
#                     "FTX1430S38A",
#                     "DCH1827V0GM",
#                     "FTT201101HE",
#                     "FCH2045FYCB",
#                     "FDO1747Y1TE",
#                     "NWG0804007B",
#                     "SAL1424KACS",
#                     #multi
#                         "FCH170289FE",
#                         "13A38305",
#                         "K0OM000847244903F8",
#                         "S0RU00044739A86169",
#                         "FOC1126Y2BW",
#                         "FTX1909K0SH",
#                         "LIT18290RQ4",
#                     #resolved
#                             "FNS22271B8M",
#                             "SPC16520A9Y",
#                             "INL26340SM0",
#                             "MSY26012179",
#                             "MTC16060097",
#                             "MTC160600DB",
#                             "MTC160600GD",
#                             "MTC1606008Z",
#                             "MTC160600RJ",
#                             "MTC1606008V",
#                             "MTC160600V3",
#
#                 ], "engagement_id": 43, "comment": "qwe"},
#           # "request_id": 208402
#         }
#     )