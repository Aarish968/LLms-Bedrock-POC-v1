# http://172.18.138.27:8090/notebooks/canvas_refresh_tags.ipynb#
# http://172.18.138.27:8090/notebooks/canvas_to_snowflake_for_thoughtspot_flow-EJ.ipynb
from my_sec import my_sec
import json
import math
import os
from datetime import datetime
from pathlib import Path
import shutil
import awswrangler as wr
import boto3
import numpy as np
import pandas as pd
from prefect import Flow, Parameter, task
from sqlalchemy import create_engine
import oyaml
import sqlalchemy
from common import new_bulkload as bl
from common import sec
import psutil
from prefect.engine.results.s3_result import S3Result
from prefect.executors.dask import LocalDaskExecutor
from prefect.run_configs.kubernetes import KubernetesRun
from prefect.storage import Docker



snowflake_db = "CPS_DB"
dn_key_name = "prd_cps_dsci_etl_svc"
schema = "CPS_DSCI_ARCHIVE"
warehouseMed = "cps_dsci_etl_wh"  # Medium
warehouseXsmall = "CPS_DSCI_ETL_EXT1_WH"  # X-Small
warehouseSmall = "CPS_DSCI_ETL_EXT2_WH"  # Small

temp_base_location = "/tmp"


def dict_back(tags):
    print(json.loads(tags))
    return json.loads(tags)


def get_values_in_str(tag_dict, superset_tags):
    v = ""
    if len(tag_dict) > 0:
        for k in superset_tags:
            v += f"{tag_dict.get(k, '')},"
    return v[:-1]


@task(log_stdout=True)
def clean_working_space(working_space: str):
    # sort fo safe must start with /tmp
    # horrid  code
    if working_space.startswith(temp_base_location):
        shutil.rmtree(working_space, ignore_errors=False, onerror=None)


def create_working_space():
    ws = os.path.join(temp_base_location, bl.gen_temp_stage_name())
    Path(ws).mkdir(parents=True, exist_ok=False)
    return ws


def split_dataframe(df, chunk_size=10000):
    chunks = list()
    num_chunks = math.ceil(len(df) / chunk_size)
    for i in range(num_chunks):
        chunks.append(df[i * chunk_size: (i + 1) * chunk_size])
    return chunks


@task(log_stdout=True)
def split_patquets_local_tmp(this_df, split_size):
    file_num = 0
    f = create_working_space()
    # chunk_size = 50000
    print(f"temp location {f}")
    chunks = split_dataframe(this_df, split_size)
    for ck in chunks:
        fn = os.path.join(f, "chunk_{}.parquet".format(file_num))
        ck.to_parquet(fn, engine="pyarrow", compression="snappy")
        file_num += 1
        # print(fn)
    return f


# @task(log_stdout=True)
def check_env(env):
    print(env)
    if env == "dev":
        cn = "dev_cps_dsci_etl_svc"
    elif env == "stage":
        cn = "stg_cps_dsci_etl_svc"
    elif env == "prod":
        cn = "prd_cps_dsci_etl_svc"
    else:
        cn = env
    print(f"""converted env : {cn}""")
    return cn


def fix_numbers(s):
    s = pd.to_numeric(s.convert_dtypes(), errors='coerce')
    s = pd.to_numeric(s, errors='coerce').convert_dtypes()
    return s


def prep_data(df,data_dict):
    col_list = df.columns
    for k in col_list:
        if k in data_dict:
            if data_dict[k] in ["Int64", "float64"]:  # "str" had this
                df[k] = fix_numbers(df[k])
            if data_dict[k] in ["datetime64[ns]"]:
                df[k] = pd.to_datetime(df[k], errors='coerce')
            if data_dict[k] in ["str"]:
                df[k] = df[k].astype("str")

        elif k.startswith('tag_'):
            df[k] = df[k].astype("str")
        else:
            df[k] = df[k].astype("str")

    return df

def drop_audit_stuff(df):
    # i think i want this..
    cols_to_drop = list(df.loc[:, df.columns.str.contains('audit_')])
    cols_to_drop.append('is_active_signed')
    avail_cols_to_drop = set(df.columns).intersection(set(cols_to_drop))
    return df.drop(columns=avail_cols_to_drop)

def fix_cols(df):
    cols = []
    for c in df.columns:
        cl = c.lower()
        cols.append(cl.strip().replace(' ', '_').replace('/', '_').replace('\\', '_').replace('-', '_'))
    return cols


def get_engagement_tags(engagement_id, cn):
    # source actual tags
    engine = create_engine(sec.get_sf_pw(cn, 'CPS_DSCI_ETL_EXT1_WH', 'CPS_DSCI_STG'))
    sql = f"""
        with sub as (
        select  eng_tags.INSTANCE_ID, eng_tags.engagement_id, tagset.tagset_id, tag.tag_id,tagset.column_name , tag.TAG_NAME
        from
        (select distinct INSTANCE_ID,ENGAGEMENT_ID, lower(f.key) as tagset_id,replace(f.value,'"','') as tag_id
        from CPS_DB.CPS_BIA_BR.DATA_CANVAS_ENGAGEMENT_TAGS, lateral flatten (tags,recursive=>true) f
        where engagement_id = {engagement_id}) eng_tags
        join CPS_DB.CPS_BIA_BR.DATA_CANVAS_TAGSET_V tagset  ON try_to_number(eng_tags.tagset_id) = tagset.tagset_id
        join CPS_DB.CPS_BIA_BR.DATA_CANVAS_TAG_V tag        ON try_to_number(eng_tags.tag_id) = tag.tag_id
        WHERE nvl(tagset.TAGSET_DEL_FLG,'N') = 'N'
        and nvl(tag.TAG_DEL_FLG,'N') = 'N'
        )
        select distinct TAG_ID,TAGSET_ID,INSTANCE_ID,ENGAGEMENT_ID,COLUMN_NAME,TAG_NAME,
        MD5(TO_VARCHAR(ARRAY_CONSTRUCT(TAG_ID,TAGSET_ID,INSTANCE_ID,ENGAGEMENT_ID))) as hash_col
        from sub"""

    tags = pd.read_sql(sql, engine)
    if tags.shape[0] > 0:
        tags = tags.drop_duplicates(subset=['instance_id', 'engagement_id', 'column_name'])
        # try:
        tags = tags.pivot(index='instance_id', columns='column_name')['tag_name']
        tags = tags.fillna('')
        tags = tags.reset_index(drop=False)
        tag_columns = tags.columns
        tags = tags.set_index('instance_id')
    else:
        tags = pd.DataFrame(columns=['instance_id', 'engagement_id', 'column_name'])
        tag_columns = tags.columns
        tags = tags.set_index('instance_id')

    sql = f"""select distinct tagset.column_name, scope
            from CPS_DB.CPS_BIA_BR.DATA_CANVAS_TAGSET_V tagset
            join CPS_DB.CPS_BIA_BR.DATA_CANVAS_TAG_V TAG ON TAGSET.TAGSET_ID = TAG.TAGSET_ID
            join CPS_DB.CPS_BIA_BR.DATA_CANVAS_ENGAGEMENT_HDR_V eng_outer
            on (tagset.scope = 'Global')
            or (tagset.scope = 'Engagement' and tagset.engagement_id = eng_outer.uid)
            or (tagset.scope = 'CAM' and tagset.CREATED_BY in (select c.value::string as cam_cec_id from "CPS_DB"."CPS_BIA_BR"."DATA_CANVAS_ENGAGEMENT_HDR_V" eng_inner, 
            lateral flatten(input=>split(camcecid, ',')) c where eng_inner.uid = '{engagement_id}'))
            where ifnull(tagset.TAGSET_DEL_FLG,'N') <> 'Y'
            and eng_outer.uid = '{engagement_id}';"""
    possible_tags = pd.read_sql(sql, engine)
    if possible_tags.shape[0] > 0:
        possible_tags = possible_tags.sort_values(['scope'])

    # add possible tags that are not used yet
    for c in list(possible_tags.column_name):
        if c not in tag_columns:
            tags[c] = ''





    print("************* TAGS *****************")
    print(tags.columns.values)
    dups_list = []
    rename_dict = {}
    for col in tags.columns:
        new_name = "_".join(col.split("_")[:-1])
        if new_name in rename_dict.values():         #check if new name is in the rename dict, and add to dups list
            dups_list.append(new_name)

        rename_dict[col] = new_name

    #remove all dups from rename dict
    for i in dups_list:
        rename_dict = {key: val for key, val in rename_dict.items() if val != i}

    tags.rename(columns=rename_dict, inplace=True)
    print("************* TAGS2 *****************")
    print(tags.columns.values)

    return tags

@task(log_stdout=True)
def copy_file_local_for_bulkload(aws_location):
    print(aws_location)
    local = create_working_space()
    fls = wr.s3.list_objects(aws_location)
    for f in fls:
        print(f)
        lfn = os.path.join(local, f.split('/')[-1])
        print(lfn)
        wr.s3.download(path=f, local_file=lfn)
    return local




def get_json_from_s3(bucket, key):
    s3 = boto3.resource('s3')
    #     bucket =  'canvas-data-types'
    #     key = 'canvas_col_rename.json'

    obj = s3.Object(bucket, key)
    data = obj.get()['Body'].read().decode('utf-8')

    json_data = oyaml.safe_load(data)

    return json_data

def rename_standard_cols(df):
    #         rename_map = dict(zip(standard_df.col, standard_df.real_name))  #cant use show bc wa only know wheat we DO NOT weant to sheo
    rename_map = get_json_from_s3('canvas-data-types', 'canvas_col_rename.json')
    df.rename(columns=rename_map, inplace=True)
    return df

def remove_hidden_cols(df):
    hidden_cols = get_json_from_s3('canvas-data-types', 'canvas_cols_to_be_hidden.json')
    hidden_list = list(set(df.columns).intersection(set(hidden_cols)))
    print(df.shape)
    df.drop(hidden_list, axis=1, inplace=True)
    print(df.shape)
    return df

def fnnnn(x):
    if x == 'String':
        return sqlalchemy.types.NVARCHAR(length=500000)
    elif x == 'Integer':
        return sqlalchemy.types.INTEGER()
    elif x == 'Date':
        return sqlalchemy.types.Date()
    elif x == 'Float':
        return sqlalchemy.types.Float(precision=2, asdecimal=True)
    else:
        return sqlalchemy.types.NVARCHAR(length=500000)

def fix_numbers(s):
    s = pd.to_numeric(s.convert_dtypes(), errors='coerce')
    s = pd.to_numeric(s, errors='coerce').convert_dtypes()
    return s



def get_json_from_s3(bucket, key):
    client = boto3.client(
        "s3",
        aws_access_key_id=my_sec.ACCESS_KEY,
        aws_secret_access_key=my_sec.SECRET_KEY
    )
    session = boto3.Session(
        aws_access_key_id=my_sec.ACCESS_KEY,
        aws_secret_access_key=my_sec.SECRET_KEY
    )


    s3 = session.resource('s3')
    #     bucket =  'canvas-data-types'
    #     key = 'canvas_col_rename.json'

    obj = s3.Object(bucket, key)
    data = obj.get()['Body'].read().decode('utf-8')

    json_data = oyaml.safe_load(data)

    return json_data


def rename_standard_cols(df):
    rename_map = get_json_from_s3('canvas-data-types', 'canvas_col_rename.json')
    df.rename(columns=rename_map, inplace=True)
    return df


def remove_hidden_cols(df):
    hidden_cols = get_json_from_s3('canvas-data-types', 'canvas_cols_to_be_hidden.json')
    hidden_list = list(set(df.columns).intersection(set(hidden_cols)))
    # print(df.shape)
    df.drop(hidden_list, axis=1, inplace=True)
    # print(df.shape)
    return df

def clean_working_space(working_space: str):
    # sort fo safe must start with /tmp
    # horrid  code
    if working_space.startswith(temp_base_location):
        shutil.rmtree(working_space, ignore_errors=False, onerror=None)
    return True


def create_working_space(temp_base_location):
    ws = os.path.join(temp_base_location, bl.gen_temp_stage_name())
    Path(ws).mkdir(parents=True, exist_ok=False)
    return ws






@task(tags=["snowflake_xsmall"])
def refresh_tags_and_create_ts_table(engagement_id,schema,tbl_name,env,canvas_parquet_path):
    client = boto3.client(
        "s3",
        aws_access_key_id=my_sec.ACCESS_KEY,
        aws_secret_access_key=my_sec.SECRET_KEY
    )
    session = boto3.Session(
        aws_access_key_id=my_sec.ACCESS_KEY,
        aws_secret_access_key=my_sec.SECRET_KEY
    )
    env = check_env(env)
    s3 = session.resource('s3')
    engine = create_engine(
        sec.get_sf_pw(env, warehouseXsmall, env)
    )

    #select FILE_PATH , UID as engagement_id from CPS_BIA_BR.DATA_CANVAS_HDR where CANVAS_ID = 'CANVAS-188'
    df = wr.s3.read_parquet(path=canvas_parquet_path , use_threads=True, boto3_session= session)

    rename_map = get_json_from_s3('canvas-data-types','canvas_col_rename.json')
    sql_data_type_map = get_json_from_s3('canvas-data-types','sql_data_type_map.json')
    pandas_data_type_map = get_json_from_s3('canvas-data-types','pandas_data_type_map.json')

    # tags = get_engagement_tags(engagement_id, check_env(env))

    df.columns = fix_cols(df)

    if df.index.name != 'instance_id':
        df = df.set_index('instance_id')
    # standard prep
    df = rename_standard_cols(df)
    print(df.shape)
    df = df.loc[:, ~df.columns.duplicated()]
    print(df.shape)
    df = remove_hidden_cols(df)

    # do in parallel in reality
    tags = get_engagement_tags(engagement_id, check_env(env))

    print('*************************')
    for c in tags.columns:
        if c.startswith('tag_'):
            print(c)


    print('*************************')

    tags.columns = fix_cols(tags)
    # null out any and make sure all are on the df
    print('*************************')
    for c in tags.columns:
        if c.startswith('tag_'):
            print(c)
            df[c] = ''

    print('*************************')
    print(df.columns.values)
    print(df.shape)
    print(tags.columns.values)
    print(tags.shape)
    print("########################")
    for c in set(tags.columns.values).difference(set(df.columns.values)):
        if c.startswith('tag_'):
            print(c)
        else:
            print("no diff")
    # update with current tags
    df.update(tags)

    df.reset_index(drop=False, inplace=True)
    print(df.columns.values)

    # if "index" in df.columns:
    #     df = df.rename(columns={"index": 'instance_id'})

    #################################################
    #################################################
    if 'canvas_data' in df.columns:
        del df['canvas_data']
    #################################################
    #################################################
    my_to_sql_dtypes = {}
    for c in df.columns:
        my_to_sql_dtypes[c] = fnnnn(sql_data_type_map.get(c, 'String'))

    for k in df.columns:
        print(k, pandas_data_type_map.get(k, 'GO DEFINE IT'))
        if pandas_data_type_map.get(k, 'xxxxx') in ["Int64", "float64"]:  # "str" had this
            df[k] = fix_numbers(df[k])
        if pandas_data_type_map.get(k, 'xxxxx') in ["datetime64[ns]"]:
            df[k] = pd.to_datetime(df[k], errors='coerce')
        if pandas_data_type_map.get(k, 'xxxxx') in ["str"]:
            df[k] = df[k].astype("str")

    df = df.replace(['nan', 'None', '<NA>'], np.nan)

    test_df = df


    ####### CANVAS TO SF TS TABLE CODE##
    #http://172.18.138.27:8090/notebooks/canvas_to_snowflake_for_thoughtspot_flow-EJ.ipynb


    test_df = remove_hidden_cols(test_df)

    test_df = test_df.loc[:, ~test_df.columns.duplicated()]

    test_df = rename_standard_cols(test_df)


    # these are badly typed columns and NOte needs a new type like string/buffer/etc
    # if 'product_list_price' in test_df.columns:
    #     #     del test_df['product_list_price']
    #     #     del test_df['service_list_price']
    #     #     del test_df['column_name']
    #
    #     #     del test_df['maintenance_po_number']
    #     #     del test_df['canvas_data']
    #     del test_df['note']
    #






    # get new name sql alchemy data types
    data_type_map = get_json_from_s3('canvas-data-types', 'sql_data_type_map.json')


    my_to_sql_dtypes = {}
    i = 1
    for c in test_df.columns:
        # print(f" {i} , {c}, --->  {fnnnn( data_type_map.get(c, 'String')  )} ")
        my_to_sql_dtypes[c] = fnnnn(data_type_map.get(c, 'String'))
        # print(len(my_to_sql_dtypes))
        i += 1


    # get new name sql alchemy data types
    pandas_data_type_map = get_json_from_s3('canvas-data-types', 'pandas_data_type_map.json')
    print(pandas_data_type_map)


    # fix data like prep data used to do
    for k in test_df.columns:
        print(k, pandas_data_type_map.get(k, 'GO DEFINE IT'))
        if pandas_data_type_map.get(k, 'xxxxx') in ["Int64", "float64"]:  # "str" had this
            test_df[k] = fix_numbers(test_df[k])
        if pandas_data_type_map.get(k, 'xxxxx') in ["datetime64[ns]"]:
            test_df[k] = pd.to_datetime(test_df[k], errors='coerce')
        if pandas_data_type_map.get(k, 'xxxxx') in ["str"]:
            test_df[k] = test_df[k].astype("str")
            # test_df[k].fillna('',   inplace=True)
            # test_df[k].replace('nan','',inplace=True)


    # clean  null-ish things up
    test_df = test_df.replace(['nan', 'None', '<NA>'], '')

    # gen the first 1000 then trunc and bulkload this typed df as a paquet


    test_df[0:1000].to_sql(tbl_name.lower(),engine,
                          schema='CPS_DSCI_ARCHIVE', if_exists='replace',
                          index=False, dtype=my_to_sql_dtypes
                          , chunksize=1000)


    temp_base_location = "/tmp"

    tmp_local = create_working_space(temp_base_location)

    # cn = check_env(env)
    #ideally split to 4 and use the correct wh size
    test_df.to_parquet(f"""{tmp_local}/a.parquet""", index=False)
    bl.generic_bulk_load_snowflake(tmp_local,
                                   schema,
                                   tbl_name,
                                   env,
                                   warehouseXsmall,
                                   create_table_from_file=False,  truncate_table=True)

    clean = clean_working_space(tmp_local)
    return tbl_name



# storage_obj = Docker(
#     base_image="prefecthq/prefect:0.15.3-python3.8",
#     python_dependencies=[
#         "pandas==1.1.3",
#         "awswrangler==2.10.0",
#         "numpy==1.19.2",
#         "elasticsearch==7.14.0",
#         "boto3==1.18.16",
#         "aiohttp",
#         "hvac",
#         "snowflake-sqlalchemy==1.2.4",
#         "s3fs==0.4",
#         "hvac>0.11.0",
#         "SQLAlchemy==1.3.20",
#         "awswrangler>2.10.0",
#         "fastparquet>0.7.1",
#         "XlsxWriter>3.0.1",
#         "oyaml",
#     ],
#     registry_url="837578041534.dkr.ecr.us-east-1.amazonaws.com/flows",
#     files={
#         """/Users/ejurotic/PycharmProjects/canvas-create-flow/canvas-create-flow/common/sec.py""": "/root/.prefect/flows/common/sec.py",
#     },
#     env_vars={"PYTHONPATH": "${PYTHONPATH}:/root/.prefect/flows/"},
# )



# with Flow(
#         "refresh_tags_create_ts_table",
#         storage=storage_obj,
#         run_config=KubernetesRun(memory_request=60000000000),
#         executor=LocalDaskExecutor(scheduler="processes", num_workers=psutil.cpu_count(logical=True)),
#         # executor=LocalDaskExecutor(scheduler="processes", num_workers=16),
#         result=S3Result(bucket="cam-prefect-results")
# ) as flow:
#     engagement_id = Parameter("engagement_id", required=True)
#     tbl_name = Parameter("tbl_name", required=True)
#     env = Parameter("env", required=True)
#     schema = Parameter("schema", required=True)
#     canvas_parquet_path = Parameter("schema", required=True)
#
#     ts_table_name = refresh_tags_and_create_ts_table(engagement_id, schema, tbl_name, env,canvas_parquet_path)



# canvas_id = "CANVAS_188"

#
# if __name__ == "__main__":
#     flow.run(
#         parameters={
#             "engagement_id": 245,
#             "tbl_name" : 'CANVAS_188_thought_spot_test'.lower(),
#             "env": "prod",
#             "schema": "CPS_DSCI_ARCHIVE",
#             "canvas_parquet_path" : "s3://canvas-data-store-prod/245_CANVAS-188_2022-03-29/",
#         }
#     )