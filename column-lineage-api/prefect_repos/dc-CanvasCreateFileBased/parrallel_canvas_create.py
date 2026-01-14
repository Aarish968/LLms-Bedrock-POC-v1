import time
from common.trigger_prefect_flow import trigger_cloud_flow_run
from common.log_to_dc_job_messages import log_to_dc_job_messages
import binpacking
import networkx
# All parts in a flow
import itertools
from prefect.tasks.prefect import RenameFlowRun
import json
from common import aws_sec
import math
from datetime import datetime
from pathlib import Path

import shutil
import awswrangler as wr
import prefect.executors
from prefect import Flow, Parameter, task
from prefect.tasks.aws.s3 import S3Upload
import psutil
from prefect import unmapped
from datetime import date
from prefect.engine.signals import SKIP
from common import file_ops
import string
import random
from common import new_bulkload as bl
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import boto3
import oyaml
import numpy as np
import pandas as pd
import os
from sqlalchemy import create_engine
from common import sec
from enum import Enum

temp_base_location = "/tmp"
from prefect.engine.results.s3_result import S3Result
from prefect.executors.dask import LocalDaskExecutor
from prefect.run_configs.kubernetes import KubernetesRun
from prefect.storage import Docker
from prefect import task
import prefect
from prefect.run_configs.docker import DockerRun


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
def clean_working_space(working_space: str, env, cid, ):
    # sort fo safe must start with /tmp
    # horrid  code
    log_to_dc_job_messages(env, cid,
                           f"SUCCESS: Step 8/12 Loaded all files to Snowflake")

    if working_space.startswith(temp_base_location):
        shutil.rmtree(working_space, ignore_errors=False, onerror=None)
    return True


@task(log_stdout=True)
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


# @task(log_stdout=True)
def check_env(env):
    return "prd_cps_dsci_etl_svc"
    # print(env)
    # if env == "dev":
    #    cn = "dev_cps_dsci_etl_svc"
    # elif env == "stage":
    #    cn = "stg_cps_dsci_etl_svc"
    # elif env == "prod":
    #    cn = "prd_cps_dsci_etl_svc"
    # return cn


import re


def fix_cols(df):
    cols = []
    for c in df.columns:
        cl = re.sub(r'[^a-zA-Z0-9_]', '_', c)
        cols.append(cl.lower())
    return cols


def rename_standard_cols(df):
    #         rename_map = dict(zip(standard_df.col, standard_df.real_name))  #cant use show bc wa only know wheat we DO NOT weant to sheo
    rename_map = get_json_from_s3('canvas-data-types', 'canvas_col_rename.json')
    df.rename(columns=rename_map, inplace=True)
    return df


def rename_real_to_display_name(df):
    rename_map = get_json_from_s3('canvas-data-types', 'ts_display_name_map.json')
    display_map_safe = dict()
    for d, i in rename_map.items():
        display_map_safe[d] = i.strip().replace(' ', '_').replace('/', '_').replace('\\', '_').replace('-',
                                                                                                       '_').replace('(',
                                                                                                                    '').replace(
            ')', '').lower()
    df.rename(columns=display_map_safe, inplace=True)
    return df


def rename_canvas_create_cols(df):
    #         rename_map = dict(zip(standard_df.col, standard_df.real_name))  #cant use show bc wa only know wheat we DO NOT weant to sheo
    rename_map = get_json_from_s3('canvas-data-types', 'canvas_prep_final_name_map.json')
    df.rename(columns=rename_map, inplace=True)
    return df


def remove_hidden_cols(df):
    hidden_cols = get_json_from_s3('canvas-data-types', 'canvas_cols_to_be_hidden.json')

    hidden_list = list(set(df.columns).intersection(set(hidden_cols)))
    # print(df.shape)
    df.drop(hidden_list, axis=1, inplace=True)
    # print(df.shape)
    return df


def get_json_from_s3(bucket, key):
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, key)
    data = obj.get()['Body'].read().decode('utf-8')
    json_data = oyaml.safe_load(data)
    return json_data


def fix_numbers(s):
    s = pd.to_numeric(s.convert_dtypes(), errors='coerce')
    s = pd.to_numeric(s, errors='coerce').convert_dtypes()
    return s


def prep_data_old(df):
    # run after standard rename
    pandas_data_type_map = get_json_from_s3('canvas-data-types', 'pandas_data_type_map.json')
    for k in df.columns:
        # print(k,pandas_data_type_map.get(k, 'GO DEFINE IT') )
        if pandas_data_type_map.get(k, 'xxxxx') in ["Int64", "float64", "int"]:  # "str" had this
            df[k] = fix_numbers(df[k])
        elif pandas_data_type_map.get(k, 'xxxxx') in ["datetime64[ns]"]:
            df[k] = pd.to_datetime(df[k], errors='coerce')
        elif pandas_data_type_map.get(k, 'xxxxx') in ["str"]:
            df[k] = df[k].astype("str")
        else:
            df[k] = df[k].astype("str")
    df = df.replace(['nan', 'None', '<NA>'], np.nan)
    return df


def make_tup(high, low):
    try:
        a = (int(high), int(low))
    except:
        a = ()
    finally:
        return a


@task(nout=2, log_stdout=True)
def get_tuples(df_, child_col, parent_col):
    df_[child_col] = df_[child_col].astype(int)
    df_[parent_col] = df_[parent_col].astype(int)
    print("doing tuple")
    df_['party_tuple'] = df_.apply(lambda x: make_tup(x[child_col], x[parent_col]), axis=1)
    print("doing graph")
    g = networkx.Graph(list(df_.party_tuple))
    dup_trans = []
    key_for_id = 1
    for subgraph in networkx.connected_component_subgraphs(g):
        dup_trans.append([key_for_id, list(subgraph.nodes())])
        if key_for_id % 10000 == 0:
            print(key_for_id)
        key_for_id += 1

    configs_df = pd.DataFrame(dup_trans, columns=['config', child_col])
    configs_df = configs_df.explode(child_col)
    configs_df[child_col] = configs_df[child_col].astype(int)
    configs_df = configs_df.set_index(child_col)
    # todo.. read df here vs outside and then return it
    # or read child, parent and get config df then use splits to process actual  but will stats work?
    #  partition evenly based on CONFIG!!!!  that gets even broken configs and muti teir configs in same file loc
    # can we load these skinny df to get tuples in parallel, for each set of tuples ( we decide based on processors) and pupulate a
    # an incomplete, partial graph then seaalize them, THEN use https://networkx.org/documentation/stable/reference/classes/generated/networkx.Graph.update.html
    # to read the first and update the remaining to speed up graph, config?
    return df_, configs_df


def isMissig(v):
    if pd.isna(v):
        return True
    else:
        return False


# https://stackoverflow.com/questions/60115806/pd-na-vs-np-nan-for-pandas
# https://stackoverflow.com/questions/60280466/merging-two-dataframes-with-pd-na-in-merge-column-yields-typeerror-boolean-val
# https://pandas.pydata.org/pandas-docs/dev/whatsnew/v1.4.0.html
def isMissigL(v):
    if pd.isna(v):
        return np.nan
    else:
        try:
            if str(v) == '<NA>':
                return np.nan
        except Exception as e:
            print(e)
    return v


@task(log_stdout=True)
def write_graph(dfx, child_c, parent_c):
    dfx['party_tuple'] = dfx.apply(lambda x: make_tup(x[child_c], x[parent_c]), axis=1)
    g = networkx.Graph(list(dfx.party_tuple))
    return g


@task(log_stdout=True)
def combie_graph(lst_g, child_col, env, cid):
    initial_G = lst_g.pop()
    sgi = 0
    for g in lst_g:
        print(f"{sgi}")
        sgi += 1
        initial_G.update(edges=g.edges, nodes=g.nodes)

    dup_trans = []
    key_for_id = 1
    for subgraph in networkx.connected_components(initial_G):
        dup_trans.append([key_for_id, list(initial_G.subgraph(subgraph).nodes())])
        if key_for_id % 10000 == 0:
            print(key_for_id)
        key_for_id += 1
    configs_df = pd.DataFrame(dup_trans, columns=['config', child_col])
    configs_df = configs_df.explode(child_col)
    configs_df[child_col] = configs_df[child_col].astype(int)
    ###########################################
    ###########################################
    # just a locat folder liek prep temp apra but not temp?
    ###########################################

    # fn= os.path.join(local_file_loc,f"graphdf_{ident}.parquet" )
    # print(fn)
    # configs_df.to_parquet(fn, engine="pyarrow", compression="snappy")
    log_to_dc_job_messages(env, cid,
                           f"SUCCESS: Step 4/12 Created Combie graph.")
    return configs_df


@task(log_stdout=True, nout=3)
def get_file_segments(files, env, cid, ):
    # quick profile of files to determine superset df
    # random_to_sample is the first 100k of each folder
    # to_update is the complete, sorted list of files each map porces will need to update
    if len(files) < 1:
        log_to_dc_job_messages(env, cid,
                               f"USER_ERROR: Step 1/12 No files selected by user.")
        raise SKIP()

    sorted_file_list = sorted(files, key=lambda k: k["date"])
    to_update = []  # files we need to process
    random_to_sample = []  # files we need to process only for cols so pick 1 per file

    if len(sorted_file_list) == 2:
        file_membership_old = sorted_file_list[0]['loc']
        print(file_membership_old)
    else:
        file_membership_old = None

    res = []
    for i in sorted_file_list:
        if i['loc'] not in res:
            res.append(i['loc'])

    print(res)

    for d in res:
        fls = wr.s3.list_objects(d, boto3_session=boto3.Session())  # need this?
        if len(fls) > 0:
            random_to_sample.append(fls[0])  # pick first file chunk
            for f in fls:
                to_update.append(f)

    log_to_dc_job_messages(env, cid,
                           f"SUCCESS: Step 1/12 Retrieved file segments.")
    return random_to_sample, to_update, file_membership_old


@task(log_stdout=True)
def get_master_cols(random_to_sample, env, canvas_id, ):
    # buid column superset from sampled files
    superset_of_cols = set()
    for samp in random_to_sample:
        df = wr.s3.read_parquet(path=samp)

        df.columns = fix_cols(df)
        # rename to new names-- PRE UPDATE
        df = rename_standard_cols(df)
        df = remove_hidden_cols(df)
        # prep_df = rename_real_to_display_name(prep_df)
        for c in df.columns:
            superset_of_cols.add(c)

    log_to_dc_job_messages(env, canvas_id,
                           f"SUCCESS: Step 3/12 Got master columns from files.")
    return list(superset_of_cols)


def fill_in_simple_covered(product_coverage_status):
    if product_coverage_status in ['ACTIVE', 'SIGNED', 'OVERDUE']:
        return 'Covered'
    elif product_coverage_status in ['NEVER COVERED', 'EXPIRED', 'TERMINATED']:
        return 'Uncovered'
    else:
        return "Not Sure"


def fill_in_LDOS_ANNUAL_DURATION(LDOS):
    LDOS = pd.to_datetime(LDOS, errors='coerce')
    date_diff = (LDOS - pd.to_datetime('today').floor('D')).days
    if date_diff >= 0 and date_diff <= 365:
        return 'b.LDoS <1 year'
    elif date_diff >= 366 and date_diff <= 730:
        return 'c.LDoS <2 years'
    elif date_diff >= 731 and date_diff <= 1095:
        return 'd.LDoS <3 years'
    elif date_diff >= 1096 and date_diff <= 1460:
        return 'e.LDoS <4 years'
    elif date_diff >= 1461 and date_diff <= 1825:
        return 'f.LDoS <5 years'
    elif date_diff >= 1826:
        return 'g.LDoS more >5 years'
    elif date_diff < 0:
        return 'a.Past LDos'
    else:
        return 'h.LDoS Not Known'


def fill_in_COVERAGE_START_ANNUAL_DURATION(coverage_start_date):
    coverage_start_date = pd.to_datetime(coverage_start_date, errors='coerce')
    date_diff = (pd.to_datetime('today').floor('D') - coverage_start_date).days

    if date_diff >= 0 and date_diff <= 365:
        return 'g.Coverage Started <1 year'
    elif date_diff >= 366 and date_diff <= 730:
        return 'f.Coverage Started <2 years'
    elif date_diff >= 731 and date_diff <= 1095:
        return 'e.Coverage Started <3 years'
    elif date_diff >= 1096 and date_diff <= 1460:
        return 'd.Coverage Started <4 years'
    elif date_diff >= 1461 and date_diff <= 1825:
        return 'c.Coverage Started <5 years'
    elif date_diff >= 1826:
        return 'b.Coverage Started >5 years'
    elif date_diff < 0:
        return 'h.Future Coverage'
    else:
        return "a.Never Covered"


def fill_in_COVERAGE_END_ANNUAL_DURATION(coverage_end_date):
    coverage_end_date = pd.to_datetime(coverage_end_date, errors='coerce')
    date_diff = (coverage_end_date - pd.to_datetime('today').floor('D')).days

    if date_diff >= 0 and date_diff <= 365:
        return 'h.Coverage Ends <1 year'
    elif date_diff >= 366 and date_diff <= 730:
        return 'i.Coverage Ends <2 years'
    elif date_diff >= 731 and date_diff <= 1095:
        return 'j.Coverage Ends <3 years'
    elif date_diff >= 1096 and date_diff <= 1460:
        return 'k.Coverage Ends <4 years'
    elif date_diff >= 1461 and date_diff <= 1825:
        return 'l.Coverage Ends <5 years'
    elif date_diff < 1825:
        return 'm.Coverage Ends >5 years'
    elif date_diff < 0 and date_diff <= -365:
        return 'f.Coverage Ended <1 year ago'
    elif date_diff >= -366 and date_diff <= -730:
        return 'e.Coverage Ended <2 years ago'
    elif date_diff >= -731 and date_diff <= -1095:
        return 'd.Coverage Ended <3 years ago'
    elif date_diff >= -1096 and date_diff <= -1460:
        return 'c.Coverage Ended <4 years ago'
    elif date_diff >= -1461 and date_diff <= -1825:
        return 'b.Coverage Ended <5 years ago'
    elif date_diff >= -1826:
        return 'a.Coverage Ended >5 years ago'
    else:
        return "g.Never Covered"


def do_metrics(df, engagement_id, env, correct_schema):
    sf_env = check_env('prod')
    engine = create_engine(
        sec.get_sf_pw(sf_env, warehouseMed, correct_schema)
    )

    if 'coverage_start_date' in df.columns:
        start_date_field = 'coverage_start_date'
    elif 'start_date' in df.columns:
        start_date_field = 'start_date'
    elif 'product_coverage_start_date' in df.columns:
        start_date_field = 'product_coverage_start_date'

    if 'coverage_end_date' in df.columns:
        end_date_field = 'coverage_end_date'
    elif 'end_date' in df.columns:
        end_date_field = 'end_date'
    elif 'last_coverage_end_date' in df.columns:
        end_date_field = 'last_coverage_end_date'
    elif 'contract_end_date' in df.columns:
        end_date_field = 'contract_end_date'

    df['AM_SERVICE_CONTRACT_TYPE'] = 'NA'
    df['AM_OFFER_TYPE'] = 'NA'
    df['AM_CONTRACT_ALLOWED_SRV_LVL'] = 'NA'
    df['RESPONSIBLE_USERS'] = 'NA'
    df['MONITOR_REASON'] = 'NA'
    df['CONTRACT_NAME'] = 'NA'

    contract_data_df_qry = f"""
                select {engagement_id} as ID , mc.CONTRACT_NUMBER,
                       listagg(distinct mc.ALLOWED_SERVICE_LEVELS ,',') WITHIN GROUP (ORDER BY mc.ALLOWED_SERVICE_LEVELS) as ALLOWED_SERVICE_LEVELS,
                       listagg(distinct mc.CONTRACT_NAME,',') WITHIN GROUP (ORDER BY mc.CONTRACT_NAME) as CONTRACT_NAME,
                       listagg(distinct ct.SOLD_AS_SERVICE_NAME,',') WITHIN GROUP (ORDER BY ct.SOLD_AS_SERVICE_NAME) as SOLD_AS_SERVICE_NAME,
                       listagg(distinct ctt.BUYING_PROGRAM_NAME,',') WITHIN GROUP (ORDER BY ctt.BUYING_PROGRAM_NAME) as BUYING_PROGRAM_NAME,
                       listagg(distinct u.CISCO_CCO_ID,',' ) WITHIN GROUP (ORDER BY u.CISCO_CCO_ID ) as responsible_users,
                       'MANAGED' as monitor_reason
                from {correct_schema}.dc_BOOKINGS_CONTRACTS c
                join {correct_schema}.dc_BOOKINGS_CONTRACTS_RESPONSIBLE_USERS r on ( r.BOOKING_CONTRACT=c.BOOKING_CONTRACT)
                join {correct_schema}.dc_ENGAGEMENT_TO_BOOKINGS_RESPONSIBLE_USER eu on ( eu.BOOKING_CONTRACT=r.BOOKING_CONTRACT and eu.DC_USER_ID=r.DC_USER_ID )
                join {correct_schema}.dc_managed_service_contracts mc on ( mc.DC_USER_ID=eu.DC_USER_ID and mc.BOOKING_CONTRACT=eu.BOOKING_CONTRACT and mc.DC_ENGAGEMENT_ID = eu.DC_ENGAGEMENT_ID)
                join {correct_schema}.DC_USERS u on (u.USER_ID=mc.DC_USER_ID)
                left join {correct_schema}.dc_sold_as_service_types ct on ( ct.service_type_id =c.SOLD_AS_SERVICE_TYPE_ID )
                left join {correct_schema}.dc_buying_programs ctt on (ctt.buying_program_type_id=c.BUYING_PROGRAM_TYPE_ID)
                where eu.DC_ENGAGEMENT_ID = {engagement_id} and c.IS_DELETED = 'F' and  r.IS_DELETED = 'F'  and  eu.IS_DELETED = 'F'  and  mc.IS_DELETED = 'F'
                group by CONTRACT_NUMBER
                union
                select {engagement_id} as ID , monc.CONTRACT_NUMBER,
                       'NA' as ALLOWED_SERVICE_LEVELS,
                       'NA' as CONTRACT_NAME,
                       'NA' as SOLD_AS_SERVICE_NAME,
                       'NA' as BUYING_PROGRAM_NAME,
                       'NA' as Responsible_users,
                       mt.MONITOR_REASON
                from {correct_schema}.dc_BOOKINGS_CONTRACTS c
                join {correct_schema}.dc_BOOKINGS_CONTRACTS_RESPONSIBLE_USERS r on ( r.BOOKING_CONTRACT=c.BOOKING_CONTRACT)
                join {correct_schema}.dc_ENGAGEMENT_TO_BOOKINGS_RESPONSIBLE_USER eu on ( eu.BOOKING_CONTRACT=r.BOOKING_CONTRACT and eu.DC_USER_ID=r.DC_USER_ID )
                join {correct_schema}.DC_MONITOR_SERVICE_CONTRACTS monc on ( monc.DC_ENGAGEMENT_ID= eu.DC_ENGAGEMENT_ID)
                join {correct_schema}.dc_CONTRACT_MONITOR_TYPES mt on ( mt.monitor_type_id = monc.MONITOR_TYPE_ID  )
                where eu.DC_ENGAGEMENT_ID = {engagement_id} and c.IS_DELETED = 'F' and  r.IS_DELETED = 'F'   and  eu.IS_DELETED = 'F' and   monc.IS_DELETED = 'F'

    """

    print(contract_data_df_qry)

    contract_data_df = pd.read_sql(contract_data_df_qry, engine)

    # not cool to do direct. they will mess up with zerro validation n UI
    contract_data_df['contract_number'] = fix_numbers(contract_data_df['contract_number'])

    contract_data_df['contract_number'] = contract_data_df['contract_number'].fillna(0)

    contract_data_df.contract_number = contract_data_df.contract_number.astype('int')

    contract_data_df = contract_data_df.rename(
        columns={"buying_program_name": "AM_SERVICE_CONTRACT_TYPE",
                 "sold_as_service_name": "AM_OFFER_TYPE",
                 "allowed_service_levels": "AM_CONTRACT_ALLOWED_SRV_LVL",
                 "responsible_users": "RESPONSIBLE_USERS",
                 "contract_name": "CONTRACT_NAME"})

    c_map = dict(zip(contract_data_df.contract_number, contract_data_df.AM_OFFER_TYPE))
    df["AM_OFFER_TYPE"] = df['contract_number'].map(c_map)
    c_map = dict(zip(contract_data_df.contract_number, contract_data_df.AM_SERVICE_CONTRACT_TYPE))
    df["AM_SERVICE_CONTRACT_TYPE"] = df['contract_number'].map(c_map)
    c_map = dict(zip(contract_data_df.contract_number, contract_data_df.AM_CONTRACT_ALLOWED_SRV_LVL))
    df["AM_CONTRACT_ALLOWED_SRV_LVL"] = df['contract_number'].map(c_map)

    c_map = dict(zip(contract_data_df.contract_number, contract_data_df.RESPONSIBLE_USERS))
    df["RESPONSIBLE_USERS"] = df['contract_number'].map(c_map)

    c_map = dict(zip(contract_data_df.contract_number, contract_data_df.CONTRACT_NAME))
    df["CONTRACT_NAME"] = df['contract_number'].map(c_map)

    # contract_data_df = contract_data_df.set_index('contract_number')
    # df = df.set_index('contract_number')
    # df.update(contract_data_df)
    # df = df.reset_index(drop=False)

    #### SIMPLE_COVERED
    # df['SIMPLE_COVERED'] = df.apply(lambda x: fill_in_simple_covered(x['product_coverage_status']), axis=1)

    ### LDOS_ANNUAL_DURATION
    # default_future_year = date.today() + relativedelta(years=6)  # add 6 years to current year
    df['LDOS_ANNUAL_DURATION'] = ''

    df['LDOS_ANNUAL_DURATION'] = df.apply(lambda x: fill_in_LDOS_ANNUAL_DURATION(x['last_date_of_support']), axis=1)
    df['LDOS_ANNUAL_DURATION'][pd.isnull(
        df.last_date_of_support)] = 'h.LDoS Not Known'

    ### COVERAGE_START_ANNUAL_DURATION
    df['COVERAGE_START_ANNUAL_DURATION'] = ''
    df['COVERAGE_START_ANNUAL_DURATION'][pd.isnull(df[start_date_field])] = "1.Never Covered"
    df['COVERAGE_START_ANNUAL_DURATION'] = df.apply(
        lambda x: fill_in_COVERAGE_START_ANNUAL_DURATION(x[start_date_field]), axis=1)

    ### COVERAGE_END_ANNUAL_DURATION
    df['COVERAGE_END_ANNUAL_DURATION'] = ''
    df['COVERAGE_END_ANNUAL_DURATION'][pd.isnull(df[end_date_field])] = "7.Never Covered"
    df['COVERAGE_END_ANNUAL_DURATION'] = df.apply(
        lambda x: fill_in_COVERAGE_END_ANNUAL_DURATION(x[end_date_field]), axis=1)

    return df


@task(log_stdout=True)
def update_superset_chunk(set_of_keys, set_of_columns, lst_of_files, canvas_id, engagement_id, local_tmp_dir,
                          file_membership, env, correct_schema):
    run_all = True
    rename_map = get_json_from_s3('canvas-data-types', 'canvas_col_rename.json')

    print(f"############################################## {file_membership}")
    st = datetime.now()

    if type(set_of_keys) == list:
        tmp = set_of_keys[0]
    else:
        tmp = set_of_keys

    if tmp.index.name == child_c:
        tmp.reset_index(drop=False, inplace=True)

    prep_df = pd.DataFrame(index=list(tmp[child_c].to_list()), columns=set_of_columns)

    if file_membership is not None:
        prep_df['membership_old'] = 'F'
        prep_df['membership_new'] = 'F'

    min_id_for_identification = bl.gen_temp_stage_name()
    print(f"RAM memory % used before update for {min_id_for_identification} is {psutil.virtual_memory()}")

    for f in lst_of_files:
        df = wr.s3.read_parquet(path=f)
        # process ALL files in the entire list
        # to isolate ourselves from renaming keys we us
        df.columns = fix_cols(df)
        if len(df.columns) > 10:
            # frantic safty move here
            df.drop_duplicates(subset=[child_c], keep='first', inplace=True)
            df = rename_standard_cols(df)
            if file_membership is not None:  # we have 2 files so set 1 to new based on path
                if file_membership in f:  # this is the old record
                    df['membership_old'] = 'T'
                else:
                    df['membership_new'] = 'T'
            df = df.loc[:, ~df.columns.duplicated()]
            df = remove_hidden_cols(df)
            try:
                df[child_c] = df[child_c].astype('int64')
                df.set_index(child_c, inplace=True)
                prep_df.update(df)
                log_to_dc_job_messages(env, canvas_id,
                                       f"SUCCESS: Step 6/12 updated superset chunk with {f}.")
            except Exception as e:
                print(e)
                print(f"This chunk had no PK {f}")
                log_to_dc_job_messages(env, canvas_id,
                                       f"FAILED: Step 6/12 updating superset chunk for {f}.")
            #  pass
            # key based update of the segment superset df

    # add in the group / congig data
    # set_of_keys = contex_segments[0]
    if tmp.index.name != child_c:
        tmp.set_index(child_c, inplace=True)
    prep_df[config_col] = -1
    prep_df.update(tmp[config_col])

    # just dropping to extra col
    if child_c in prep_df.columns:  # todo alanzen lookup
        prep_df = prep_df.drop(columns=child_c)  # todo alanzen lookup

    prep_df.reset_index(drop=False, inplace=True)

    if "index" in prep_df.columns:
        prep_df = prep_df.rename(columns={"index": child_c})  # todo alanzen lookup

    prep_df["canvas_id"] = f"CANVAS-{canvas_id}"
    if "engagement_id" in prep_df.columns:
        prep_df = prep_df.rename(
            columns={"engagement_id": "mce_engagement_id", "engagement_name": "mce_engagement_name"})

    prep_df["engagement_id"] = engagement_id

    # device level below  CAN do it here bc all related instances ( by config are together)
    pid_remap_dict = get_json_from_s3('canvas-data-types', 'pid_remaps_map.json')

    # TODO publish to proid
    prep_df["fixed_product_type"] = prep_df[rename_map['item_name']].map(pid_remap_dict)  # audit only
    prep_df["real_product_type"] = np.NaN
    prep_df["real_product_type"] = prep_df[rename_map['item_name']].map(pid_remap_dict)  # fix what ypu need to
    prep_df.loc[prep_df.real_product_type.isna(), 'real_product_type'] = prep_df[rename_map['ib_product_type']]

    prep_df['is_actual_parent'] = 'N'
    innum = set(prep_df[child_c].values)
    pinnum = set(prep_df[parent_c].values)
    # mark all missing parents as E
    err_configs = prep_df[[config_col]][prep_df[parent_c].isin(pinnum.difference(innum))]
    prep_df.loc[(prep_df[config_col].isin(
        err_configs[config_col].unique())), 'is_actual_parent'] = 'E'  # error bc parent is missing

    # pick a winner for errors parents  TODO get rid of all es after setting a winner
    err_parents = prep_df[[child_c, config_col]][prep_df.is_actual_parent == 'E']
    err_parents = err_parents[[child_c, config_col]].groupby(
        [config_col]).min()  # should prob be better about MAX $ etc
    err_parents.rename(columns={child_c: 'min_instance'}, inplace=True)

    prep_df.loc[(prep_df[child_c].isin(err_parents.min_instance.unique())), 'is_actual_parent'] = 'Y'

    # TODO add for Eric
    prep_df.loc[prep_df.is_actual_parent == 'E', 'is_actual_parent'] = 'N'

    prep_df.loc[(prep_df.instance_id == prep_df.parent_instance_id) & (
            prep_df.is_actual_parent == 'N'), 'is_actual_parent'] = 'Y'

    prep_df['actual_parent_count'] = 0
    prep_df.loc[prep_df.is_actual_parent == 'Y', 'actual_parent_count'] = 1

    #     prep_df.loc[(prep_df.instance_id == prep_df.parent_instance_id) & (prep_df.is_actual_parent=='N')  , 'is_actual_parent' ] ='Y'
    prep_df['config_2'] = np.where(prep_df.instance_id == prep_df.parent_instance_id, prep_df.parent_instance_id,
                                   np.nan)
    tagged = prep_df[['config_2', 'config']][~prep_df.config_2.isna()]
    config_tag = dict(zip(tagged.config, tagged.config_2))
    prep_df["config_2"] = prep_df.config.map(config_tag)

    prep_df = prep_data_old(prep_df)

    def do_count_into_df_parent(df, df_agg, metric):
        df_agg = df_agg[[child_c, config_col]].groupby([config_col]).count()
        df_agg.rename(columns={child_c: 'total'}, inplace=True)
        df_agg.reset_index(drop=False, inplace=True)
        x_dict = dict(zip(df_agg.config, df_agg.total))
        df[metric] = np.where(df.is_actual_parent == 'Y', df.config.map(x_dict), 0)

    # live LDOS

    prep_df['is_ldos_flag'] = 'Y'

    prep_df.loc[prep_df[rename_map['product_last_date_of_support_ldos']].isna(), 'is_ldos_flag'] = 'N'
    prep_df.loc[prep_df[rename_map['product_last_date_of_support_ldos']] == 'None', 'is_ldos_flag'] = 'N'

    # run time LDOS Flag
    prep_df.loc[prep_df[rename_map['product_last_date_of_support_ldos']] >= pd.to_datetime('today').floor(
        'D'), 'is_ldos_flag'] = 'N'

    # line count KNOWING that a canvas only has 1 instance
    do_count_into_df_parent(prep_df, prep_df, 'total_config_lines')
    print("prep_df", prep_df.head())
    # odd combination of the previous ideas CAMs used via Parent or Standalone as a proxy for total devices
    # do_count_into_df_parent(prep_df,
    #                         prep_df[[child_c, config_col]][
    #                             prep_df[rename_map['product_relationship']].isin(['Parent', 'Standalone'])],
    #                         'total_parents')

    # upleveling... count fixed Chassis but Cap the value at 1
    do_count_into_df_parent(prep_df,
                            prep_df[[child_c, config_col]][prep_df.real_product_type == 'CHASSIS'],
                            'total_chassis')

    # business rules Athul for this measure, it should never be more than 1
    prep_df.loc[prep_df.total_chassis > 1, 'total_chassis'] = 1

    # total items in latest install
    do_count_into_df_parent(prep_df,
                            prep_df[[child_c, config_col]][
                                prep_df[rename_map['install_base_status']] == 'Latest-INSTALLED'],
                            'total_latest_installed')

    do_count_into_df_parent(prep_df,
                            prep_df[[child_c, config_col]][
                                prep_df[rename_map['install_base_status']] != 'Latest-INSTALLED'],
                            'not_total_latest_installed')

    do_count_into_df_parent(prep_df,
                            prep_df[[child_c, config_col]][prep_df.real_product_type == 'SOFTWARE'],
                            'total_sw_product_type')

    do_count_into_df_parent(prep_df,
                            prep_df[[child_c, config_col]][prep_df.real_product_type != 'SOFTWARE'],
                            'total_non_sw_product_type')

    # count a sum or list distinct values of items in one pass
    gb_res = (prep_df.sort_values(
        by=[config_col, child_c], ascending=False,
    ).groupby(config_col)
    .agg(
        contract_number_list=(rename_map['contract_number'], "unique"),
        maintenance_so_number_list=(rename_map['maintenance_so_number'], "unique"),
        service_list_price_raw_total=(rename_map['service_list_price_raw'], "sum"),
        product_list_price_total=(rename_map['product_list_price'], "sum"),
        installed_at_site_id_list=(rename_map['installed_at_site_id'], "unique"),
        install_base_status_list=(rename_map['install_base_status'], "unique"),
        # historical_negotiated_price_total=(rename_map['service_list_price_raw'], "sum"),
        quantity_total=(rename_map['quantity'], "sum"),
        # global_product_list_price_total=("global_product_list_price", "sum")
    )
    )
    gb_res.reset_index(drop=False, inplace=True)

    # read to go sums or counts
    quantity_total_dict = dict(zip(gb_res.config, gb_res.quantity_total))
    service_list_price_raw_total_dict = dict(zip(gb_res.config, gb_res.service_list_price_raw_total))
    product_list_price_total_dict = dict(zip(gb_res.config, gb_res.product_list_price_total))
    # historical_negotiated_price_total_dict   = dict(zip(gb_res.config, gb_res.historical_negotiated_price_total))

    # list stats
    install_base_status_length_dict = dict(zip(gb_res.config, gb_res.install_base_status_list.apply(lambda x: len(x))))
    contract_number_list_len_dict = dict(zip(gb_res.config, gb_res.contract_number_list.apply(lambda x: len(x))))
    maintenance_so_number_list_len_dict = dict(
        zip(gb_res.config, gb_res.maintenance_so_number_list.apply(lambda x: len(x))))
    installed_at_site_id_list_len_dict = dict(
        zip(gb_res.config, gb_res.installed_at_site_id_list.apply(lambda x: len(x))))

    # now either populate the total OR 0 based on parent flag and key
    # rename from col to real -> device_level_quantity_total
    prep_df['quantity_total'] = np.where(prep_df.is_actual_parent == 'Y',
                                         prep_df.config.map(quantity_total_dict), 0)

    # col to device_level_service_list_price_raw_total
    prep_df['service_list_price_raw_total'] = np.where(prep_df.is_actual_parent == 'Y',
                                                       prep_df.config.map(service_list_price_raw_total_dict), 0)

    # device_level_product_list_price_total
    prep_df['product_list_price_total'] = np.where(prep_df.is_actual_parent == 'Y',
                                                   prep_df.config.map(product_list_price_total_dict), 0)
    # device_level_install_base_status_length
    prep_df['install_base_status_length'] = np.where(prep_df.is_actual_parent == 'Y',
                                                     prep_df.config.map(install_base_status_length_dict), 0)
    # device_level_contract_number_list_length
    prep_df['contract_number_list_length'] = np.where(prep_df.is_actual_parent == 'Y',
                                                      prep_df.config.map(contract_number_list_len_dict), 0)
    # device_level_maintenance_so_number_list_length
    prep_df['maintenance_so_number_list_length'] = np.where(prep_df.is_actual_parent == 'Y',
                                                            prep_df.config.map(maintenance_so_number_list_len_dict), 0)
    # device_level_installed_at_site_id_list_length
    prep_df['installed_at_site_id_list_length'] = np.where(prep_df.is_actual_parent == 'Y',
                                                           prep_df.config.map(installed_at_site_id_list_len_dict), 0)

    # used to create dynamic aggs based on real (fixed) product type
    # removed bc they are bad
    #     cards = prep_df[[child_c, config_col, 'real_product_type']].groupby([config_col, 'real_product_type']).count()
    #     cards.rename(columns={child_c: 'total_items'}, inplace=True)
    #     cards = cards.reset_index(drop=False)

    #     for c in cards.real_product_type.unique():
    #         # print(c)
    #         temp_agg = cards[['config', 'total_items']][cards.real_product_type == c].groupby(['config']).sum()
    #         metric_name = f'product_type_{str(c).strip().lower().replace(" ", "_")}'
    #         temp_agg.rename(columns={'total_items': metric_name}, inplace=True)
    #         temp_agg = temp_agg.reset_index(drop=False)
    #         tmp_dict = dict(zip(temp_agg.config, temp_agg[metric_name]))
    #         prep_df[f'product_type_{str(c).strip().lower().replace(" ", "_")}'] = np.where(prep_df.is_actual_parent == 'Y',
    #                                                                                        prep_df.config.map(tmp_dict), 0)
    if file_membership is not None:
        prep_df['file_membership'] = 'SAME'
        prep_df.loc[(prep_df['membership_new'] == 'F') & (prep_df['membership_old'] == 'T'), 'file_membership'] = 'OLD'
        prep_df.loc[(prep_df['membership_new'] == 'T') & (prep_df['membership_old'] == 'F'), 'file_membership'] = 'NEW'

    # live correction of old sourced logic
    def convert_covered_status(covered_status):
        if covered_status == 'A':
            return 'ACTIVE'
        if covered_status == 'I':
            return 'EXPIRED'
        if covered_status == 'N':
            return 'NEVER COVERED'

    prep_df.covered_status = prep_df.apply(lambda x: convert_covered_status(x['covered_status']), axis=1)

    # place for POST iteration, DB updates.
    # this will be ALL live flags based on ANY data in SF
    prep_df = do_metrics(prep_df, engagement_id, env, correct_schema)

    prep_df = prep_df.replace(['nan', 'None', '<NA>', None, 'NaN'], np.nan)

    #####   Make all changes to prep_df above here or else all of your columns will be wrong
    #################################
    prep_df.columns = fix_cols(prep_df)
    prep_df = rename_standard_cols(prep_df)  # not needed bc above
    prep_df = prep_df.loc[:, ~prep_df.columns.duplicated()]
    prep_df = prep_data_old(prep_df)
    prep_df = rename_real_to_display_name(prep_df)
    prep_df = prep_df.loc[:, ~prep_df.columns.duplicated()]
    print(f"HEY DROPPING STUFF: {prep_df.columns.duplicated()}")
    # prep_df = prep_df.loc[:, ~prep_df.columns.duplicated()]
    prep_df.columns = fix_cols(prep_df)  # not needed

    if 'offer_ato_suite_name' in prep_df.columns:
        prep_df['offer_ato_suite_name'] = prep_df['offer_ato_suite_name'].replace(['nan', 'None', '<NA>', np.nan], '')

    # prep_df = prep_df.copy()
    ###################################

    print(f, datetime.now() - st)
    print(f"RAM memory % used after update for {min_id_for_identification} is {psutil.virtual_memory()}")

    prep_df['device_level_is_parent_ldos_flag'] = 'N'
    ldos_announced = prep_df[['parent_ldos', 'parent_instance_id']][
        (~prep_df.parent_ldos.isna())  # not null
        & (prep_df.instance_id == prep_df.parent_instance_id)  # parents only
        & (prep_df.parent_ldos <= pd.to_datetime('today').floor('D'))  # only care if Y
        ]

    prep_df.loc[(prep_df.parent_instance_id.isin(
        list(ldos_announced.parent_instance_id))), 'device_level_is_parent_ldos_flag'] = 'Y'

    # this is safer than copy
    local_tmp_file_loc = os.path.join(local_tmp_dir, f"chunk_{min_id_for_identification}.parquet")
    # print(aws_path)
    # wr.s3.to_parquet(prep_df, path=aws_path, compression='snappy', index=False)
    #     prep_df.to_parquet(path=local_tmp_file_loc, compression='snappy', index=False)
    extra_columns = []
    extra_columns.append(("covered_to_ldos", 'pa.string()'))
    extra_columns.append(("is_mss_available", 'pa.string()'))
    extra_columns.append(("existing_mss_coverage", 'pa.string()'))
    extra_columns.append(("mss_available_to_date", 'pa.date32()'))
    extra_columns.append(("mss_service_available", 'pa.string()'))
    extra_columns.append(("dl_parent_product_family", 'pa.string()'))
    extra_columns.append(("device_level_extended_list_price", 'pa.float64()'))
    prep_df['covered_to_ldos'] = '-'
    prep_df['is_mss_available'] = '-'
    prep_df['existing_mss_coverage'] = '-'
    prep_df['mss_available_to_date'] = None
    prep_df['mss_service_available'] = '-'
    prep_df['dl_parent_product_family'] = '-'
    prep_df['device_level_extended_list_price'] = None

    print(prep_df.columns.values)

    write_parquet_with_pa(prep_df, local_tmp_file_loc, name_space_enum.DISPLAY, extra_columns, out_cols_as_lower=True,
                          fill_nulls=True)
    log_to_dc_job_messages(env, canvas_id,
                           f"SUCCESS: Step 7/12 Completed file cleanup and write for this chunk.")
    return True


@task(log_stdout=True, nout=2)
def get_contextual_slices(segs, graph_df, config_col, env, canvas_id, ):
    # get all keys, set to int, indec on child
    num_segments = len(segs)

    all_keys = pd.concat(segs)
    all_keys = all_keys[[child_c, parent_c]]

    all_keys.drop_duplicates(subset=[child_c], keep='first', inplace=True)

    total_lines_in_canvas = all_keys.shape[0]

    all_keys[child_c] = all_keys[child_c].astype(int)
    if all_keys.index.name != child_c:
        all_keys.set_index(child_c, inplace=True)
    all_keys[config_col] = -1

    # get clusters set to int, indec on child
    graph_df[child_c] = graph_df[child_c].astype(int)
    if graph_df.index.name != child_c:
        graph_df.set_index(child_c, inplace=True)
    all_keys.update(graph_df)

    del graph_df
    all_keys[config_col] = all_keys[config_col].astype(int)

    # TOTAL PER CONFIG TO USE FOR BALANCE IN BINS
    config_col = 'config'
    aggs = all_keys.groupby(config_col).size()
    aggs = aggs.reset_index(drop=False)
    aggs.columns = [config_col, 'total']
    # parts = 4
    parts = num_segments
    myDict = pd.Series(aggs['total'].values, index=aggs[config_col]).to_dict()
    bins = binpacking.to_constant_bin_number(myDict, parts)

    contex_segments = []
    if all_keys.index.name != config_col:
        all_keys.reset_index(drop=False, inplace=True)
        all_keys.set_index(config_col, inplace=True)
    for ccnt, b in enumerate(bins):
        sd = pd.DataFrame(index=list(b.keys()))
        good_part = pd.merge(all_keys, sd, left_index=True, right_index=True)
        good_part.reset_index(drop=False, inplace=True)
        good_part.rename(columns={"index": config_col, "Index": config_col}, inplace=True)
        # print(good_part.shape)
        # good_part.drop_duplicates(subset=[child_c], keep='first', inplace=True)
        # print(good_part.shape)
        contex_segments.append(good_part)
    log_to_dc_job_messages(env, canvas_id,
                           f"SUCCESS: Step 5/12 Got contextual slices.")
    return contex_segments, total_lines_in_canvas


@task(log_stdout=True, nout=1)
def get_master_keys(to_update, WH, env, correct_schema, max_rows_per_part, cid):
    superset_of_keys = set()
    cn = check_env('prod')
    total_instances = []  # to hold all instance_ids
    parent_child = []  # to hold what we need for networkx
    for samp in to_update:
        # since v2 of data sourcing cant rely on always lower case cols so test it
        columns_types, partitions_types = wr.s3.read_parquet_metadata(path=[samp])
        if columns_types.get(child_c.upper(), 'x') != 'x':
            df = wr.s3.read_parquet(path=samp, columns=[child_c.upper(), parent_c.upper()])
            df.columns = fix_cols(df)
        else:
            df = wr.s3.read_parquet(path=samp, columns=[child_c, parent_c])

        try:
            total_instances.append(df[child_c])
            parent_child.append(df)
        except Exception as e:
            print(e)
            s3_parq_loc = "/".join(samp.split("/")[:-1]) + "/"
            set_file_as_mising(s3_parq_loc, WH, cn, correct_schema)
    # instance with more than 1 occurance that need resoluton
    all_pks = pd.DataFrame(list(itertools.chain.from_iterable(total_instances)), columns=[child_c])

    parent_child = pd.concat(parent_child)
    print(parent_child.shape[0])
    if parent_child.shape[0] < max_rows_per_part:
        max_rows_per_part = parent_child.shape[0]
    segs = split_dataframe(parent_child, max_rows_per_part)
    log_to_dc_job_messages(env, cid,
                           f"SUCCESS: Step 2/12 Retrieved file segments.")

    return segs  # ,  superset_of_keys


def set_file_as_mising(s3_parq_loc, warehouseXsmall, cn, schema):
    print(f"""Updating {s3_parq_loc} to be marked as MISSING""")
    missing_qry = f"update {correct_schema}.DC_DATA_SOURCES set DISPLAY_NAME = concat('MISSING : ',DISPLAY_NAME ) where FOLDER_PATH = '{s3_parq_loc}';"
    engine = create_engine(sec.get_sf_pw(cn, warehouseXsmall, schema))
    con = engine.connect()

    print(missing_qry)
    con.execute(missing_qry)


@task(log_stdout=True, nout=4)
def prep_vars(engagement_id, canvas_id, env, date, correct_schema):
    letters = string.ascii_letters
    random_string = '{}'.format(''.join(random.choice(letters) for i in range(10)))
    try:
        RenameFlowRun().run(flow_run_name=f"""{canvas_id}-{random_string}""")
    except:
        pass
    date = date[0:min(10, len(date))]
    aws_canvas_out_pth = f"s3://canvas-data-store-{env}/CANVAS_FILES/{date.replace('-', '_')}/{canvas_id}"
    cn = check_env('prod')
    db_table = f"""canvas_{canvas_id}_THOUGHT_SPOT""".upper()

    sf_env = check_env("prod")
    engine = create_engine(
        sec.get_sf_pw(sf_env, warehouseMed, correct_schema)
    )
    requested_by_query = f"select created_by from {correct_schema}.DC_CANVAS_HDR where canvas_id = {canvas_id}"
    print(requested_by_query)
    requested_by_df = pd.read_sql(requested_by_query, engine)
    requested_by = str(requested_by_df.created_by[0])
    return cn, aws_canvas_out_pth, db_table, requested_by


def resolve_schemas_local(loc):
    dataset = ds.dataset(loc)
    schemas = [pq.read_schema(dataset_file) for dataset_file in dataset.files]
    dataset = ds.dataset(loc, schema=pa.unify_schemas(schemas))
    return dataset.to_table().to_pandas()


@task(log_stdout=True)
def write_metadata_table(
        full_canvas_out_pth,
        num_of_instances,
        canvas_excel_s3_pth,
        canvas_id,
        engagement_id,
        cn,
        correct_schema,
        logged,
        run_date,
        s3_parq_loc
):
    engine = create_engine(
        sec.get_sf_pw(cn, warehouseXsmall, correct_schema)
    )
    con = engine.connect()

    json_loc = "s3://messaging.stage.cisco.com/start-canvas-creation/"
    print("####################")
    print(s3_parq_loc)
    date_created = datetime.now().date().isoformat()
    update_metadata_query = f"""
            update {correct_schema}.DC_CANVAS_HDR i
        set i.CANVAS_STATUS = 'success',
            i.FILE_PATH = '{s3_parq_loc}',
            i.create_DTM = current_timestamp
        where i.DC_ENGAGEMENT_ID = {int(engagement_id)} and i.CANVAS_ID = {canvas_id} ;
    """

    try:
        delete_json_from_s3('canvas-lock', canvas_id)
    except:
        print("deleting canvas lock file failed")

    print(update_metadata_query)
    try:
        con.execute(update_metadata_query)
    except Exception as e:
        print(e)
        print(f"Data for this canvas_id is not in {correct_schema}.DC_CANVAS_HDR")
        pass

    update_canvas_processing_log_query = f"""
     UPDATE {correct_schema}.dc_canvas_create_run_log  set PROCESSING = 'complete'
     where CANVAS_ID = {canvas_id} and RUN_DATE = '{run_date}';
     """

    print(update_canvas_processing_log_query)
    try:
        con.execute(update_canvas_processing_log_query)
    except Exception as e:
        print(e)
        print(f"Data for this canvas_id is not in {correct_schema}.dc_canvas_create_run_log")
        pass

    return True


@task(log_stdout=True, nout=2)
def log_run_to_table(canvas_id, files, engagement_id, correct_schema, env, cn, rerun_flag, rerun_run_date):
    input_parameters = build_params_dict(canvas_id, files, engagement_id, correct_schema, env)
    engine = create_engine(
        sec.get_sf_pw(cn, warehouseXsmall, correct_schema)
    )

    run_date = datetime.now()

    log_df = pd.DataFrame(index=[0], columns=['CANVAS_ID', 'INPUT_PARAMETERS', 'PROCESSING', 'RUN_DATE'])
    log_df['CANVAS_ID'] = canvas_id
    log_df['INPUT_PARAMETERS'] = f'{input_parameters}'
    log_df['PROCESSING'] = 'P'
    log_df['RUN_DATE'] = run_date

    if rerun_flag:  # we do not want to add a new flow run line to the table if this is a rerun
        run_date = rerun_run_date
        log_df['RUN_DATE'] = run_date
        pass
    else:
        log_df.to_sql('dc_canvas_create_run_log', engine, schema=correct_schema, index=False,
                      if_exists='append')

    return input_parameters, run_date


def build_params_dict(canvas_id, files, engagement_id, table_schema, env):
    input_parameters = {
        'canvas_id': canvas_id,
        'engagement_id': engagement_id,
        'env': env,
        'files': files,
        'schema': table_schema,
    }
    print(json.dumps(input_parameters))
    return json.dumps(input_parameters)


@task
def constuct_message_file_key(canvas_id: str):
    return (
        f"canvas-processing-status/canvas-{canvas_id}-{datetime.now().isoformat()}.json"
    )


@task(log_stdout=True)
def clean_locations(full_canvas_out_pth):
    if len(full_canvas_out_pth.split('/')) > 4:  # at least at some depth to avoid horrific deleteion of lots fo data
        fls = wr.s3.list_objects(full_canvas_out_pth)
        if len(fls) > 0:
            print(f"deleting all files : {fls}")
            wr.s3.delete_objects(fls)

    return True


def explode_info(df, key_col, split_col, name_list):
    mce_l = []
    mce = df[[key_col, split_col]]
    for i, row in mce.iterrows():
        if row[split_col] is not None and len(row[split_col]) > 0:
            vals = row[split_col].replace("  ", " ").replace("  ", " ").replace(" ", ",").replace(",,", ",").split(',')
            for v in vals:
                if len(v) > 0:
                    # print(int(v))
                    mce_l.append([row[key_col], v])
    return pd.DataFrame(mce_l, columns=name_list)


# @task(tags=["snowflake_medium"])
# def create_rpt_flat_table(engagement_id, schema, env, canvas_id,correct_schema):
#     # this can be its own flow

#     engine = create_engine(sec.get_sf_pw( check_env('prod'), 'CPS_DSCI_ETL_WH', 'CPS_DSCI_ARCHIVE'))
#     contract_flatten_sql = f"""select  h.UID,c.uid as contract_uid, c.ID,c.CONTRACT_NUMBER, c.CONTRACT_TYPE, c.AM_SERVICE_TYPE
#     from  CPS_BIA_BR.DATA_CANVAS_CONTRACT_DATA_V c
#         join CPS_BIA_BR.DATA_CANVAS_ENGAGEMENT_HDR_V h on (h.id=c.id) where h.uid = {engagement_id}
#         and nvl(contract_del_flag,'N') != 'Y'  """

#     fdf = pd.read_sql(contract_flatten_sql, engine)
#     flat_contracts = explode_info(fdf, 'contract_uid', 'contract_number', ['contract_uid', 'contract_number'])

#     flat_contracts.drop_duplicates(subset=['contract_number'], keep='first', inplace=True)

#     today = date.today()

#     canvas_id = canvas_id.replace("-", "_").lower()
#     table_name = f'''rpt_flat_contracts_{engagement_id}_{canvas_id}_{today.strftime('%Y_%m_%d')}'''.upper()

#     flat_contracts.to_sql(table_name.lower(), con=engine, schema=correct_schema, if_exists='replace', index=False,
#                           chunksize=5000)

#     return table_name


@task(log_stdout=True, tags=["snowflake_xsmall"])
def delete_sf_table(db_table, sf_env, schema, warehouseXsmall, correct_schema):
    sf_env = check_env('prod')
    engine = create_engine(
        sec.get_sf_pw(sf_env, warehouseXsmall, 'CPS_DSCI_ARCHIVE')
    )

    con = engine.connect()
    print(f"""drop table if exists {correct_schema}.{db_table};""")
    table_dropped = con.execute(f"""drop table if exists {correct_schema}.{db_table};""")
    return True


def check_env(env):
    #   print(env)
    #   if env == "dev":
    #       cn = "dev_cps_dsci_etl_svc"
    #   elif env == "stage":
    #       cn = "stg_cps_dsci_etl_svc"
    #   elif env == "prod":
    #       cn = "prd_cps_dsci_etl_svc"
    return "prd_cps_dsci_etl_svc"


class name_space_enum(Enum):
    COL = 1
    REAL = 2
    DISPLAY = 3


def get_json_from_s3(bucket, key):
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, key)
    data = obj.get()['Body'].read().decode('utf-8')
    json_data = oyaml.safe_load(data)
    return json_data


# @task(log_stdout=True)
def get_pa_schema_for_cols(thin_cols, extra_col_def, name_set: name_space_enum, to_lower=False):
    pa_this_schema = []
    # extra_column_names=[]
    # for k in extra_col_def:
    #    extra_column_names.append(k[0])

    # all_cols = list(set(thin_cols).union(set(extra_column_names)))

    if name_set == name_space_enum.COL:
        pyArrow_Remap = get_json_from_s3('canvas-data-types', 'col_to_pyarrow_types.json')
    elif name_set == name_space_enum.REAL:
        pyArrow_Remap = get_json_from_s3('canvas-data-types', 'pyarrow_real_data_type_map.json')
    elif name_set == name_space_enum.DISPLAY:
        pyArrow_Remap = get_json_from_s3('canvas-data-types', 'display_to_pyarrow_types.json')

    for c in thin_cols:
        dt = pyArrow_Remap.get(c.lower(), '-')
        if dt == '-':
            print(f'{c}: missing')
            dt = 'pa.string()'
            for xc in extra_col_def:
                if xc[0] == c.lower():
                    print(f'overide default of {xc[0]}: to {xc[1]}')
                    dt = xc[1]
        if to_lower:
            pa_this_schema.append(pa.field(c.lower(), eval(dt)))
        else:
            pa_this_schema.append(pa.field(c, eval(dt)))

    #     # dont add 1 in we already have
    #     print(existing_def)
    #     for xc in extra_col_def:
    #         if xc[0] not in existing_def:
    #             print(xc)
    #             pa_this_schema.append(pa.field(xc[0], eval(xc[1])))

    # print(f"SCHEMA: {pa_this_schema}")
    return pa.schema(pa_this_schema)


def fix_numbers(s):
    s = pd.to_numeric(s.convert_dtypes(), errors='coerce')
    s = pd.to_numeric(s, errors='coerce').convert_dtypes()
    return s


def prep_data(df, ref: name_space_enum, fill_nulls=True):
    # need default values for col and

    print(f'{ref} columns used for map')

    if ref == name_space_enum.COL:
        default_values = get_json_from_s3('canvas-data-types', 'col_to_default_values.json')
        pandas_data_type_map = get_json_from_s3('canvas-data-types', 'pre_rename_sql_data_type_map.json')
    elif ref == name_space_enum.REAL:
        pandas_data_type_map = get_json_from_s3('canvas-data-types', 'pandas_data_type_map.json')
        default_values = get_json_from_s3('canvas-data-types', 'real_to_default_values.json')
    elif ref == name_space_enum.DISPLAY:
        default_values = get_json_from_s3('canvas-data-types', 'display_to_default_values.json')
        pandas_data_type_map = get_json_from_s3('canvas-data-types', 'display_to_pandas_types.json')

    for k in df.columns:

        if fill_nulls:
            default_v = default_values.get(k, 'xxxxx')
            # print(default_v)
            if default_v in ['xxxxx', "'null'"]:
                # print(f"def was '' for {k}")
                df[k] = df[k].replace(['nan', 'None', '<NA>', np.nan], '')
            else:
                df[k] = df[k].replace(['nan', 'None', '<NA>', np.nan], default_v)

        # print(k,pandas_data_type_map.get(k, 'GO DEFINE IT') )
        if pandas_data_type_map.get(k, 'xxxxx') in ["Int64", "float64", "int"]:  # "str" had this
            df[k] = fix_numbers(df[k])
        elif pandas_data_type_map.get(k, 'xxxxx') in ["datetime64[ns]"]:
            df[k] = pd.to_datetime(df[k], errors='coerce').dt.date
        elif pandas_data_type_map.get(k, 'xxxxx') in ["str"]:
            df[k] = df[k].astype("str")
        else:
            df[k] = df[k].astype("str")

    return df


def list_files_and_sizes(mypath, ext):
    file_nfo = []
    for filename in os.listdir(mypath):
        if filename.endswith(ext):
            file_nfo.append(filename)
    return file_nfo


def gen_temp_stage_name():
    letters = string.ascii_letters
    random_string = '{}'.format(''.join(random.choice(letters) for i in range(10)))
    return "culvert_stage_{}".format(random_string)


def write_parquet_with_pa(df, out_loc, names_to_use: name_space_enum, non_standard_cols=[], out_cols_as_lower=True,
                          fill_nulls=True):
    # make sure path is there else add them
    df.columns = fix_cols(df)
    df = prep_data(df, names_to_use, fill_nulls=False)  # display, col, real
    needed_schema = get_pa_schema_for_cols(df.columns.unique(), non_standard_cols, names_to_use, out_cols_as_lower)
    if df.shape[0] > 0:
        table = pa.Table.from_pandas(df, schema=needed_schema, preserve_index=False)
        with pq.ParquetWriter(out_loc, needed_schema, compression='snappy', allow_truncated_timestamps=True) as writer:
            writer.write_table(table)
    return True


def delete_json_from_s3(bucket, key):
    key = f'{key}.json'
    session = boto3.Session(
        aws_access_key_id=aws_sec.ACCESS_KEY, aws_secret_access_key=aws_sec.SECRET_KEY
    )

    s3 = session.resource("s3")
    obj = s3.Object(bucket, key)
    obj.delete()

    return True


@task(log_stdout=True)
def get_correct_schema(env):
    if env == 'prod':
        return 'CPS_DSCI_API'
    else:
        return 'CPS_DSCI_BR'


def get_sec_dir(pth):
    return os.path.join(os.getcwd(), pth)


@task(log_stdout=True)
def add_to_gu_log_table(sf_env, request_id, status, qry_type, dc_engagement_id, requestedBy):
    cn = check_env('prod')
    correct_schema = get_correct_schema.run(sf_env)

    engine = create_engine(
        sec.get_sf_pw(cn, "CPS_DSCI_ETL_EXT1_WH", correct_schema)
    )
    if isinstance(requestedBy, list):
        requestedBy = requestedBy[0]
    con = engine.connect()
    date_created = datetime.now().date().isoformat()
    if qry_type == 'insert':
        qry = f"""
        insert into {correct_schema}.dc_generic_upload(dc_engagement_id,
                                                        request_id,
                                                        file_location,
                                                        status,
                                                        output_file_path,
                                                        generic_template_name, 
                                                        CREATED_BY,
                                                        CREATE_DTM,
                                                        is_deleted
                                                        ) values ({dc_engagement_id},
                                                                    {request_id},
                                                                    's3://Logs for your Canvas Creation {request_id}',
                                                                    '{status}',
                                                                    's3://Click the magnifying glass to retreive your logs ->>>> ',
                                                                    'Click the magnifying glass...',
                                                                    '{requestedBy}',
                                                                    '{date_created}',
                                                                    'F'
                                                                    )
        """
    elif qry_type == 'update':
        qry = f"""
        UPDATE CPS_DB.{correct_schema}.dc_generic_upload set STATUS = '{status}'         
        where REQUEST_ID  = '{int(request_id)}' ;
        """

    con.execute(qry)


storage_obj = Docker(
    base_image="837578041534.dkr.ecr.us-east-1.amazonaws.com/bases/prefect:0.15.13-python3.9",
    python_dependencies=[
        "pandas==1.4.2",
        "awswrangler==2.12.1",
        "numpy==1.22.3",
        "boto3",
        "botocore",
        "aiohttp==3.8.4",
        "hvac==0.11.2",
        "snowflake-sqlalchemy==1.2.4",
        "s3fs==0.4",
        "SQLAlchemy==1.4.35",
        "fastparquet==0.7.2",
        "XlsxWriter==3.1.2",
        "oyaml==1.0",
        "networkx==2.8",
        "binpacking==1.5.2",
        "cloudpickle==2.0.0"

    ],

    registry_url="837578041534.dkr.ecr.us-east-1.amazonaws.com/dc/p1",
    path="parrallel_canvas_create.py",
    files={
        get_sec_dir('common/log_to_dc_job_messages.py'): "/common/log_to_dc_job_messages.py",
        get_sec_dir('common/trigger_prefect_flow.py'): "/common/trigger_prefect_flow.py",
        get_sec_dir('common/data_types.py'): "/common/data_types.py",
        get_sec_dir('common/new_bulkload.py'): "/common/new_bulkload.py",
        get_sec_dir('common/sec.py'): "/common/sec.py",
        get_sec_dir('common/aws_sec.py'): "/common/aws_sec.py",
        get_sec_dir('common/file_ops.py'): "/common/file_ops.py",
        get_sec_dir('common/sql_pool.py'): "/common/sql_pool.py",
        get_sec_dir('parrallel_canvas_create.py'): "parrallel_canvas_create.py",

    },
    env_vars={"PYTHONPATH": "${PYTHONPATH}:/"},
    stored_as_script=True
)
with Flow(
        "canvas-create-file-based",
        storage=storage_obj,
        run_config=KubernetesRun(memory_request=60000000000),
        #executor=LocalDaskExecutor(scheduler="processes", num_workers=(psutil.cpu_count(logical=True) - 1)),
        executor=LocalDaskExecutor(scheduler="processes", num_workers=8),
        # executor=LocalDaskExecutor(scheduler="processes", num_workers=20),
        result=S3Result(bucket="cam-prefect-results")
) as flow:
    canvas_request_date = Parameter("date", required=True)
    canvas_id = Parameter("canvas_id", required=True)
    # canvas_id = canvas_id.split('-')[-1]
    files = Parameter("files", required=True)
    dc_engagement_id = Parameter("engagement_id", required=True)
    env = Parameter("env", required=True)
    rerun_flag = Parameter("rerun_flag", default=False)
    rerun_run_date = Parameter("rerun_run_date", default='')


    config_col = 'config'
    child_c = 'instance_id'
    parent_c = 'parent_instance_id'
    snowflake_db = "CPS_DB"
    warehouse = "CPS_DSCI_ETL_EXT2_WH"
    warehouseMed = "cps_dsci_etl_wh"  # Medium
    warehouseXsmall = "CPS_DSCI_ETL_EXT1_WH"  # X-Small
    warehouseSmall = "CPS_DSCI_ETL_EXT2_WH"  # Small
    message_bucket = "data.canvas.messaging.cisco.com"
    bucket_name = "canvas-data-store-prod"

    correct_schema = get_correct_schema(env)

    # local_temp = create_working_space()  # move to
    cn, aws_canvas_out_pth, db_table, requested_by = prep_vars(dc_engagement_id, canvas_id, env, canvas_request_date,
                                                               correct_schema,
                                                               upstream_tasks=[correct_schema])
    logged_complete_to_gu = add_to_gu_log_table(env, canvas_id, "InProgress", 'insert', dc_engagement_id, requested_by,
                                                upstream_tasks=[cn, aws_canvas_out_pth, db_table, requested_by])
    logged, run_date = log_run_to_table(canvas_id, files, dc_engagement_id, correct_schema, env, cn, rerun_flag,
                                        rerun_run_date, upstream_tasks=[cn, aws_canvas_out_pth, db_table, requested_by])

    random_to_sample, to_update, file_membership = get_file_segments(files, env, canvas_id,
                                                                     upstream_tasks=[logged, run_date])

    segs = get_master_keys(to_update, warehouseSmall, env, correct_schema, 50000, canvas_id,
                           upstream_tasks=[random_to_sample, to_update, file_membership])  # 50k ~ 1.5-2GB per core

    superset_cols_all = get_master_cols(random_to_sample, env, canvas_id,
                                        upstream_tasks=[segs])  # all columns from sampled files

    processed_partial_graphs = write_graph.map(
        dfx=segs,
        child_c=unmapped(child_c),
        parent_c=unmapped(parent_c)
        , upstream_tasks=[superset_cols_all]
    )

    graph_df = combie_graph(processed_partial_graphs, child_c, env, canvas_id,
                            upstream_tasks=[processed_partial_graphs])

    contex_segments, total_lines_in_canvas = get_contextual_slices(segs, graph_df, config_col, env, canvas_id,
                                                                   upstream_tasks=[graph_df])

    local_tmp_dir = create_working_space(upstream_tasks=[contex_segments, total_lines_in_canvas])

    uss = update_superset_chunk.map(set_of_keys=contex_segments,
                                    set_of_columns=unmapped(superset_cols_all),
                                    lst_of_files=unmapped(to_update),
                                    canvas_id=unmapped(canvas_id),
                                    engagement_id=unmapped(dc_engagement_id),
                                    local_tmp_dir=unmapped(local_tmp_dir),
                                    file_membership=unmapped(file_membership),
                                    env=unmapped(env),
                                    correct_schema=unmapped(correct_schema),
                                    upstream_tasks=[local_tmp_dir]
                                    )

    bulk_loaded_table = bl.generic_bulk_load_snowflake(local_tmp_dir,
                                                       correct_schema,
                                                       db_table,
                                                       env,
                                                       warehouseMed,
                                                       create_table_from_file=True, truncate_table=True,
                                                       upstream_tasks=[uss])

    cleaned = clean_working_space(local_tmp_dir, env, canvas_id,
                                  upstream_tasks=[bulk_loaded_table])  # alanzen 11-29-2121

    meta_logged = write_metadata_table(
        aws_canvas_out_pth,
        total_lines_in_canvas,
        aws_canvas_out_pth,
        canvas_id,
        dc_engagement_id,
        cn,
        correct_schema,
        logged,
        run_date,
        bulk_loaded_table,
        upstream_tasks=[cleaned, uss],
    )

    triggered = trigger_cloud_flow_run(canvas_id, requested_by, env, correct_schema, dc_engagement_id,
                                       upstream_tasks=[meta_logged])

    logged_complete_to_gu = add_to_gu_log_table(env, canvas_id, "Complete", 'update', dc_engagement_id, requested_by,
                                                upstream_tasks=[triggered])

if __name__ == "__main__":
    flow.run(

        parameters=       {
          "canvas_id": 21742,
          "date": "2023-10-10T09:03:39.160695",
          "engagement_id": 2701,
          "env": "prod",
          "files": [],
          "rerun_flag": False,
          "rerun_run_date": "",

        }
    )

    # {
    #     "canvas_id": 20024,
    #     "date": "2023-08-01T16:10:33.723813",
    #     "engagement_id": 94,
    #     "env": "dev",
    #     "files": [ {
    #         "loc": "s3://canvas-data-store-prod/ACAT_PREPPED_FILES/2022_05_02/1649280657166/full/",
    #         "date": "2022-04-06",
    #         "name": "MARSH-MCLENNAN-INC"
    #       }, {
    #         "loc": "s3://canvas-data-store-prod/MCE_PREPPED_FILES/mce_1615217214843/2022_05_02/full/",
    #         "date": "2022-05-02",
    #         "name": "MMC_6_8_2022_2022-05-02"
    #       }],
    #     "rerun_flag": false,
    #     "rerun_run_date": "",
    #     "json_env": "prod",
    #     "destination_table": "DATA_CANVAS_DETAILS"
    # }