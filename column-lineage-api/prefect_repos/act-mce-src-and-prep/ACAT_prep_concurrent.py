import pandas as pd
import numpy as np
import os
import datetime as dt
from datetime import datetime
import math
from common import file_ops, core_cr_fn
from sqlalchemy import create_engine
from common import sec, data_types
import logging
import awswrangler as wr
from prefect.engine.results.s3_result import S3Result
from prefect.run_configs.kubernetes import KubernetesRun
from prefect.storage import Docker
from prefect.executors import DaskExecutor
from prefect.executors import LocalDaskExecutor
from prefect import task, unmapped, Flow, Parameter
import os, psutil
import boto3
import json
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 200)

snowflake_db = "CPS_DB"
dn_key_name = "prd_cps_dsci_etl_svc"
schema = "CPS_DSCI_ARCHIVE"
warehouseMed = "cps_dsci_etl_wh"  # Medium
warehouseXsmall = "CPS_DSCI_ETL_EXT1_WH"  # X-Small
warehouseSmall = "CPS_DSCI_ETL_EXT2_WH"  # Small

FORMAT = "[%(asctime)s, %(levelname)s] %(message)s"
logging.basicConfig(filename='logfile.log', level=logging.DEBUG, format=FORMAT)


def create_meta_json_from_df(df):
    "Takes in a df and returns a json with columns, rows and mem size"
    df_rows = df.shape[0]
    df_columns = df.shape[1]
    size = df.memory_usage(deep=True).sum()

    meta_json = {"rows": int(df_rows),
                 "columns": int(df_columns),
                 "mem_usage": int(size)
                 }
    return meta_json


def write_dict_to_json_file_in_s3(dictionary, bucket, key):
    """onverts a dict to a json file and writes to an s3 bucket/key location """
    s3 = boto3.resource('s3')
    s3object = s3.Object(bucket, key)
    s3object.put(
        Body=(bytes(json.dumps(dictionary).encode('UTF-8')))
    )


def split_dataframe(df, chunk_size=10000):
    chunks = list()
    num_chunks = math.ceil(len(df) / chunk_size)
    for i in range(num_chunks):
        chunks.append(df[i * chunk_size:(i + 1) * chunk_size])
    return chunks


def make_note(x):
    note = ''
    for index, value in x.items():
        note += f"""{index}: {value}   --> """
    return note


def return_last_element(row):
    return row[-1]


def get_instance_id(instance_numbers):
    engine = create_engine(sec.get_sf_pw(dn_key_name, warehouseXsmall, schema))
    if len(instance_numbers) == 1:
        df = pd.read_sql(
            f"""select INSTANCE_ID, INSTANCE_NUMBER from
"EDW_SERVICE_ETL_DB"."SS"."CSF_XXCCS_DS_INSTANCE_DETAIL" i  where INSTANCE_NUMBER in ({instance_numbers[0]})
""",
            engine,
        )
    else:
        df = pd.read_sql(
            f"""select INSTANCE_ID, INSTANCE_NUMBER from
    "EDW_SERVICE_ETL_DB"."SS"."CSF_XXCCS_DS_INSTANCE_DETAIL" i  where INSTANCE_NUMBER in {instance_numbers}
    """,
            engine,
        )

    return df


def split_list_under_size(lst, max_size=16383):
    if len(lst) > max_size:
        chunked_list = [lst[i:i + max_size] for i in range(0, len(lst), max_size)]
    else:
        chunked_list = [lst]
    return chunked_list


def get_enriched_data(in_list, max_len_in_list):
    chunked_list = split_list_under_size(in_list, max_len_in_list)
    complete_list_dfs = []
    for in_block in chunked_list:
        print("execute your sql in this loop with in block")
        complete_list_dfs.append(get_instance_id(tuple(in_block)))

    final_df = pd.concat(complete_list_dfs)

    return final_df


def fix_cols(df):
    cols = []
    for c in df.columns:
        cl = c.lower()
        cols.append(cl.strip().replace(' ', '_').replace('/', '_').replace('\\', '_'))
    return cols


column_for_instance = 'instance_number'


def active_signed(stscode_list):
    if len(stscode_list) == 2:
        if stscode_list[0] == 'ACTIVE' and stscode_list[1] == 'SIGNED':
            return 'Y'

    else:
        return 'N'


def flatten_list(this_list):
    if len(this_list) > 0:
        cl = [str(element) for element in this_list]
        return " --> ".join(cl)
    else:
        return ''


def null_or_normalize_dates(col):
    _v = dt.datetime.strptime('2100-12-31', "%Y-%m-%d")
    try:
        if col == 'None' or pd.isna(col) or col is None:
            pass
        elif type(col) is pd.Timestamp:
            _v = dt.datetime.strftime(col, "%Y-%m-%d")

        else:
            _v = dt.datetime.strptime(col, '%Y-%m-%d')
    except:
        pass
    finally:
        return _v


def zero_or_floats(col):
    _v = 0.0
    try:
        if col == 'None' or pd.isna(col) or col is None:
            pass
        else:
            _v = pd.to_numeric(col, errors='raise')
    except:
        pass
    finally:
        return _v


def null_or_normalize_strings(col):
    if col == 'None' or pd.isna(col):
        return None
    else:
        return str(col)


def fix_cols(df):
    cols = []
    for c in df.columns:
        cl = c.lower()
        cols.append(cl.strip().replace(' ', '_').replace('/', '_').replace('\\', '_'))
    return cols


def dask_columns_to_lower(df):
    cc = []
    for c in df.columns:
        cc.append(
            (
                c,
                c.lower()
                    .strip()
                    .replace(" ", "_")
                    .replace("/", "_")
                    .replace("\\", "_"),
            )
        )
    conversion_dict = dict(cc)
    return df.rename(columns=conversion_dict)


def fix_ints_def(df, v, def_value=0):
    print(v)
    arr = np.array(df[v], dtype=float)
    df[v] = arr
    df[v] = df[v].fillna(def_value).astype(np.double).round().astype(np.int64)


# MCE
def ez_instance_number(iid):
    if not pd.isna(iid):
        if int(iid) > 74981032:
            return iid
        else:
            return -1
    else:
        return 0


# MCE
def ez_instance_id(iid):
    if isinstance(iid, int) or isinstance(iid, float):
        if iid > 74981032:
            return int(iid)
        else:
            return -1
    else:
        return -2

def fix_numbers(s):
    s= pd.to_numeric(s.convert_dtypes(), errors='coerce')
    s= pd.to_numeric(s, errors='coerce').convert_dtypes()
    return s

def prep_data(df):
    col_list = df.columns
    for k in col_list:
        if k in data_types.data_dict:
            if data_types.data_dict[k] in ["Int64", "float64"]:  # "str" had this
                df[k] = fix_numbers(df[k])
            if data_types.data_dict[k] in ["datetime64[ns]"]:
                df[k] = pd.to_datetime(df[k], errors='coerce')
            if data_types.data_dict[k] in ["str"]:
                df[k] = df[k].astype("str")

        elif k.startswith('tag_'):
            df[k] = df[k].astype("str")
        else:
            df[k] = df[k].astype("str")

    return df


@task(log_stdout=True)
def get_and_split_into_multis_singles(this_file, singles_out_loc, multis_out_loc, run_id):
    request_id = run_id.split('_')[-2]
    engine = create_engine(sec.get_sf_pw(dn_key_name, warehouseXsmall, schema))
    con = engine.connect()
    update_acat_source_table = f""" update CPS_DSCI_ARCHIVE.ACAT_CANVAS_DATA_SOURCE_META set IS_PREPPED ='p' where REQUEST_ID = {request_id} """
    print(update_acat_source_table)
    con.execute(update_acat_source_table)


    print(f"system pid : {os.getpid()}")
    process = psutil.Process(os.getpid())
    print("get_and_split_into_multis_singles 1")
    print(int(process.memory_info().rss)/1024 ** 2)
    this_df_issues = wr.s3.read_parquet(path=this_file, columns=['instance_id', 'instance_number'])

    for col in this_df_issues.select_dtypes(include=["string"]):
        this_df_issues[col] = this_df_issues[col].astype('object')
        this_df_issues[col] = this_df_issues[col].replace(np.nan, None)

    cntt = (
        this_df_issues[["instance_number", "instance_id"]].groupby("instance_number").count()
    )

    issues = cntt[['instance_id']][cntt.instance_id > 1]

    issues['instance_id'] = pd.to_numeric(issues['instance_id'], downcast='integer', errors='coerce')

    this_df_singles = wr.s3.read_parquet(path=this_file)
    for col in this_df_singles.select_dtypes(include=["string"]):
        this_df_singles[col] = this_df_singles[col].astype('object')
        this_df_singles[col] = this_df_singles[col].replace(np.nan, None)

    if (
            not issues.empty
    ):  # check to see if we have any duplicates, if so group them, join back to the main df

        multis = issues.merge(this_df_singles, left_index=True, right_on='instance_number')
        multis = multis.rename(columns={"instance_id_y": "instance_id"})
        multis = multis.drop(['instance_id_x'], axis=1)
        multis['instance_id'] = multis['instance_id'].astype('int64')
        multis['instance_id'] = pd.to_numeric(multis['instance_id'], downcast='integer', errors='coerce')

        multis.to_parquet(
            os.path.join(multis_out_loc, "multi_rec_issues.parquet"),
            engine="pyarrow",
            compression="snappy",
            index=False,
            allow_truncated_timestamps=True,
            coerce_timestamps="ms",
        )

    shards = split_dataframe(this_df_singles, 100000)
    iter_v = 0
    print("get_and_split_into_multis_singles 2")
    print(int(process.memory_info().rss)/1024 ** 2)
    for d in shards:
        d.to_parquet(
            os.path.join(singles_out_loc, "segment_{}.parquet".format(iter_v)),
            engine="pyarrow",
            compression="snappy",
            index=False,
            allow_truncated_timestamps=True,
            coerce_timestamps="ms",
        )
        iter_v += 1


@task(log_stdout=True)
def get_multi_files(file_loc):
    print(file_loc)
    these_files = wr.s3.list_objects(file_loc)
    print(f"multi files :{these_files}")
    return these_files


@task(log_stdout=True)
def get_single_files(file_loc):
    print(file_loc)
    these_files = wr.s3.list_objects(file_loc)
    print(f"single files :{these_files}")
    return these_files


def do_prep_function(this_df):
    this_df.columns = fix_cols(this_df)
    this_df = this_df.drop(
        columns=['target_date_terminated', 'target_dnr_code', 'target_contract_number', 'target_service_line_name',
                 'target_install_site_id_flag', 'target_install_at_site_use_id', 'target_contract_bill_to_id',
                 'target_deal_id',
                 'user_atc_coverage_start_date', 'atc_coverage_end_date', 'invoice_start_date', 'list_price_protected'])

    print(f"""1 : {this_df.shape}""")
    fields_to_int = [
        'instance_number',
        'parent_instance_number',
        'instance_id', 'parent_instance_id']

    time_convert_cols = ['user_atc_coverage_start_date', 'start_date', 'ship_date', 'latest_cvg_date_terminated',
                         'last_update_date',
                         'last_date_of_support', 'instance_cvg_max_end_date', 'instance_creation_date', 'end_date',
                         'earliest_discovery_date',
                         'creation_date', 'atc_coverage_end_date', 'atc_coverage_start_date', 'invoice_start_date']

    float_convert_cols = ['service_list_price', 'product_list_price']

    net_time_convert_cols = list(set(this_df.columns).intersection(set(time_convert_cols)))
    net_fields_to_int = list(set(this_df.columns).intersection(set(fields_to_int)))
    net_float_convert_cols = list(set(this_df.columns).intersection(set(float_convert_cols)))
    net_string_convert_cols = list(
        set(this_df.columns).difference(set(net_time_convert_cols + net_fields_to_int + net_float_convert_cols)))

    # replacing NAN with none or the if statement in net_float_convert_cols wont work
    for f in net_float_convert_cols:
        this_df[f] = this_df[f].replace({np.nan: None})

    print("Fixing Floats")
    for f in net_float_convert_cols:
        this_df[f] = this_df.apply(lambda x: zero_or_floats(x[f]), axis=1)

    # trying this convert here to minimize code.
    print("Fixing Dates")
    for f in net_time_convert_cols:
        this_df[f] = this_df[f].apply(pd.to_datetime)

    print("Fixing ints")
    for f in net_fields_to_int:
        print(f)
        fix_ints_def(this_df, f)

    print("Fixing str")
    # replacing NAN with none or the if statement in null_or_normalize_strings wont work
    for column in net_string_convert_cols:
        print(f"""Fixing str part 1 ,col: {column}""")
        this_df[column] = this_df[column].replace(
            {np.nan: None}
        )

    print("Fixing str 2")
    for f in net_string_convert_cols:
        print(f"""Fixing str part 2 ,col: {f}""")
        this_df[f] = this_df.apply(lambda x: null_or_normalize_strings(x[f]), axis=1)

    print("Fixing Adresses...")
    this_df['street_address'] = this_df.apply(lambda x: core_cr_fn.remove_whitespace_no_stopwords(
        "{} {} {} {}".format(x['install_address1'],
                             x['install_address2'],
                             x['install_address3'],
                             x['install_address4'])), axis=1)

    this_df = this_df.drop(columns=['install_address1', 'install_address2', 'install_address3', 'install_address4'])

    this_df['instance_number'] = this_df['instance_number'].astype('int64')  # alanzen
    this_df['instance_id'] = this_df.apply(lambda x: ez_instance_id(x['instance_number']), axis=1)

    this_df['parent_instance_number'] = this_df['parent_instance_number'].astype('int64')  # alanzen
    this_df['parent_instance_id'] = this_df.apply(lambda x: ez_instance_id(x['parent_instance_number']), axis=1)

    for f in net_fields_to_int:
        fix_ints_def(this_df, f)

    instance_number_list = (this_df["instance_number"][this_df["instance_id"] == -1]).to_list()
    n = 16000

    instance_number_list_chunks = [instance_number_list[i:i + n] for i in range(0, len(instance_number_list), n)]
    this_df.set_index("instance_number", inplace=True, drop=False)

    print("about to fix instance_number_list")
    if instance_number_list:
        for instance_number_chunk in instance_number_list_chunks:
            print("Fixing instance_number_list")
            instance_num_id_df = get_enriched_data(instance_number_chunk, 16383)
            instance_num_id_df = prep_data(instance_num_id_df)
            instance_num_id_df.set_index("instance_number", inplace=True)
            this_df.update(instance_num_id_df)

    parent_instance_number_list = (this_df["parent_instance_number"][this_df["parent_instance_id"] == -1]).to_list()
    parent_instance_number_list_chunks = [parent_instance_number_list[i:i + n] for i in
                                          range(0, len(parent_instance_number_list), n)]
    if parent_instance_number_list:
        for parent_instance_number_list_chunk in parent_instance_number_list_chunks:
            print("Fixing parent_instance_number_list")
            parent_instance_num_id_df = get_enriched_data(parent_instance_number_list_chunk, 16383)
            parent_instance_num_id_df = parent_instance_num_id_df.rename(columns={"instance_id": "parent_instance_id"})
            parent_instance_num_id_df = prep_data(parent_instance_num_id_df)
            parent_instance_num_id_df.set_index("instance_number", inplace=True)
            this_df.update(parent_instance_num_id_df)

    this_df = this_df.reset_index(drop=True)

    return this_df


@task(log_stdout=True)
def do_prep_singles(this_parquet, out_path):
    this_df = pd.read_parquet(this_parquet, engine="pyarrow")
    for col in this_df.select_dtypes(include=["string"]):
        this_df[col] = this_df[col].astype('object')
        this_df[col] = this_df[col].replace(np.nan, None)

    this_df = do_prep_function(this_df)
    process = psutil.Process(os.getpid())
    print(f"do_prep_singles {this_parquet} ")
    print(int(process.memory_info().rss)/1024 ** 2)
    this_df.to_parquet(
        os.path.join(out_path, "{}".format(this_parquet.split(os.sep)[-1])),
        engine="pyarrow",
        compression="snappy",
        index=False,
        allow_truncated_timestamps=True,
        coerce_timestamps="ms",
    )

    return os.path.join(out_path, "{}".format(this_parquet.split(os.sep)[-1]))


@task(log_stdout=True)
def do_prep_multis(multi_file, multi_files_out_pth):
    this_df = pd.read_parquet(multi_file, engine="pyarrow")

    for col in this_df.select_dtypes(include=["string"]):
        this_df[col] = this_df[col].astype('object')
        this_df[col] = this_df[col].replace(np.nan, None)

    this_df = do_prep_function(this_df)
    process = psutil.Process(os.getpid())
    print(f"do_prep_multis {multi_file} ")
    print(int(process.memory_info().rss)/1024 ** 2)
    gb_res = this_df.sort_values(by=['instance_number', 'start_date', 'end_date'], ascending=True).groupby(
        'instance_number').agg(
        contract_number_list=('contract_number', 'unique'),
        mn_start_date=('start_date', 'min'),
        mx_end_date=('end_date', 'max'),
        sts_code_list=('sts_code', 'unique'),
        service_line_name_list=('service_line_name', 'unique'),
        covered_line_id_list=('covered_line_id', 'unique'),
        maintenance_so_number_list=('maintenance_so_number', 'unique'),
        contract_bill_to_site_use_id_list=('contract_bill_to_site_use_id', 'unique'),
        contract_bill_to_customer_name_list=('contract_bill_to_customer_name', 'unique'),
        contract_billto_gu_id_list=('contract_billto_gu_id', 'unique'),
        contract_billto_gu_name_list=('contract_billto_gu_name', 'unique'),
        contract_bid_parent_party_id_list=('contract_bid_parent_party_id', 'unique'),
        contract_bid_parent_party_name_list=('contract_bid_parent_party_name', 'unique'),
        line_count=('sts_code', 'count'),  # is this an ok column to do count on?
        acat_line_id_list=('acat_line_id', 'unique')

    )

    gb_res['modified_record'] = 'multi_line_fix'

    gb_res['is_active_signed'] = gb_res.apply(lambda x: active_signed(x['sts_code_list']), axis=1)

    gb_res['flat_contract_bill_to_customer_name'] = gb_res.apply(
        lambda x: flatten_list(x['contract_bill_to_customer_name_list']), axis=1)

    gb_res['flat_contract_billto_gu_name'] = gb_res.apply(lambda x: flatten_list(x['contract_billto_gu_name_list']),
                                                          axis=1)

    gb_res['flat_contract_bid_parent_party_name'] = gb_res.apply(
        lambda x: flatten_list(x['contract_bid_parent_party_name_list']), axis=1)

    gb_res['flat_service_line_name'] = gb_res.apply(lambda x: flatten_list(x['service_line_name_list']), axis=1)

    # list of columns that were grouped with unique in order to iter over them to build the note
    grouped_columns = ['contract_number_list',
                       'sts_code_list',
                       'service_line_name_list',
                       'covered_line_id_list',
                       'maintenance_so_number_list',
                       'contract_bill_to_site_use_id_list',
                       'contract_bill_to_customer_name_list',
                       'contract_billto_gu_id_list',
                       'contract_billto_gu_name_list',
                       'contract_bid_parent_party_id_list',
                       'contract_bid_parent_party_name_list',
                       'line_count',
                       'acat_line_id_list']

    gb_res['note'] = gb_res.apply(lambda x: make_note(x), axis=1)

    # list of columns that were grouped with unique in order to iter over them to build the note.
    # removed line count since its not actually a list
    grouped_columns = ['contract_number_list',
                       'sts_code_list',
                       'service_line_name_list',
                       'covered_line_id_list',
                       'maintenance_so_number_list',
                       'contract_bill_to_site_use_id_list',
                       'contract_bill_to_customer_name_list',
                       'contract_billto_gu_id_list',
                       'contract_billto_gu_name_list',
                       'contract_bid_parent_party_id_list',
                       'contract_bid_parent_party_name_list',
                       'acat_line_id_list']
    print(f"do_prep_multis {multi_file}  2")
    print(int(process.memory_info().rss)/1024 ** 2)
    # iterating over the df , col by col. grabing the last item of each list
    for col in gb_res[grouped_columns]:
        gb_res[f"""audit_{col}"""] = gb_res.apply(lambda x: flatten_list(x[col]), axis=1)

    gb_res = gb_res.drop(columns=grouped_columns)

    gb_res = gb_res.reset_index(drop=False)
    print(f"do_prep_multis {multi_file}  3")
    print(int(process.memory_info().rss)/1024 ** 2)
    gb_res.to_parquet(
        os.path.join(multi_files_out_pth, "{}".format(multi_file.split(os.sep)[-1])),
        engine="pyarrow",
        compression="snappy",
        index=False,
        allow_truncated_timestamps=True,
        coerce_timestamps="ms",
    )

    return True


@task(log_stdout=True, nout=4)
def concat_multis_singles(single_files_out_pth, multi_files_out_pth, full_canvas_out_pth, run_id, this_file, ):
    try:
        multi_files_df = wr.s3.read_parquet(path=multi_files_out_pth)
        print(f"Successfully imported multis parquet from : {multi_files_out_pth}")
    except:
        print(f"There are no multis available at : {multi_files_out_pth}")
        multi_files_df = pd.DataFrame(columns=['instance_number'])

    for col in multi_files_df.select_dtypes(include=["string"]):
        multi_files_df[col] = multi_files_df[col].astype('object')
        multi_files_df[col] = multi_files_df[col].replace(np.nan, None)

    single_files_df = wr.s3.read_parquet(path=single_files_out_pth)

    for col in single_files_df.select_dtypes(include=["string"]):
        single_files_df[col] = single_files_df[col].astype('object')
        single_files_df[col] = single_files_df[col].replace(np.nan, None)
    process = psutil.Process(os.getpid())
    print(f"concat_multis_singles 1")
    print(int(process.memory_info().rss)/1024 ** 2)
    # dropping dups from the original df
    single_files_df['instance_id'] = single_files_df['instance_id'].astype('int64')

    single_files_df = single_files_df.drop_duplicates(subset=["instance_id"], keep="last")

    # set index so we can join
    single_files_df = single_files_df.set_index("instance_number", drop=False)

    this_df = single_files_df.merge(multi_files_df, how="left", left_index=True, right_on="instance_number")
    print(f"concat_multis_singles 2")
    print(int(process.memory_info().rss)/1024 ** 2)
    try:
        acat_account_id = f"""{this_file.split("_", 20)[-3]}"""
        print(f"acat_account_id is is {acat_account_id}")
    except:
        acat_account_id = 0

    # below two lines are commented out for the tester file, the format of the name doesnt have a date to parse and write to sf
    date_chunk = f"""{this_file.split("_", 20)[-1]}"""  # split the file on _ and take the last element which looks like 2020-06-18.parquet
    try:
        date_sourced = str(date_chunk.split(".")[0])  # split on . to remove the parquet portion
    except:
        date_sourced = '2020-01-01'

    display_name = f"""acat_no_filters_{run_id}_{date_sourced}"""""

    this_df[f"""canvas_source_file_{display_name}"""] = 1

    this_df = this_df.set_index("instance_id", drop=False)

    this_df['instance_id'] = this_df['instance_id'].astype('int64')

    this_df['parent_instance_id'] = this_df['parent_instance_id'].astype('int64')

    this_df = this_df.drop(['instance_number_x', 'instance_number_y'], axis=1)
    num_of_instances = len(this_df)
    # print(this_df.dtypes)
    dc1 = split_dataframe(this_df, 100000)
    iter_v = 0
    for d in dc1:
        d.to_parquet(
            os.path.join(full_canvas_out_pth, "segment_{}.parquet".format(iter_v)),
            engine="pyarrow",
            compression="snappy",
            index=False,
            allow_truncated_timestamps=True,
            coerce_timestamps="ms",
        )
        iter_v += 1
    print(f"concat_multis_singles 3")
    print(int(process.memory_info().rss)/1024 ** 2)

    return acat_account_id, date_sourced, display_name, num_of_instances


@task(tags=["snowflake"])
def write_metadata_table(
        acat_customer_id,
        canvas_file_key,
        bucket,
        num_of_instances,
        run_id,
        canvas_output_location,
        date_sourced,
        display_name,
):
    """
        Notes for the metadata_table :

        create table canvas_data_sources
    (
        file_number int identity(null, 1),  -- for auditing etc
        remote_system_customer_identifier int, -- either smart account id for MCE or ACAT CUSTOMER ID..  UI  Engagement, Smart Account, Customer_id
        file_name varchar(5000),        -- the derived file name
        folder_path varchar(10000),   -- the real S3 bucket/folder
        file_source varchar(500),     -- MCE, ACAT
        file_type varchar(500),        -- Subtype from Source like MCE_without_EOL_and non serviceable
        num_records int,                -- number of records ( in this case instances )
        date_sourced date,            -- when the original source system took the snapshot
        last_processed_date date   -- we post process and enrich after sourcing sometimes going way back
    );

    """

    engine = create_engine(sec.get_sf_pw(dn_key_name, warehouseXsmall, schema))
    con = engine.connect()
    request_id = run_id.split('_')[-2]
    remote_system_customer_identifier = acat_customer_id
    file_name = "*.parquet"
    folder_path = f"""{canvas_output_location}"""
    file_type = "all"
    num_records = num_of_instances
    date_sourced = date_sourced
    last_processed_date = datetime.now()

    update_acat_source_table = f""" update CPS_DSCI_ARCHIVE.ACAT_CANVAS_DATA_SOURCE_META set IS_PREPPED ='T' where REQUEST_ID = {request_id} """
    print(update_acat_source_table)
    con.execute(update_acat_source_table)
    print(f"write_metadata_table 1")
    process = psutil.Process(os.getpid())
    print(int(process.memory_info().rss)/1024 ** 2)
    update_metadata_query = f"""
    INSERT INTO CPS_DB.CPS_BIA_BR.DATA_CANVAS_DATA_SOURCES (REMOTE_SYSTEM_CUSTOMER_IDENTIFIER, 
                                                                FILE_NAME, 
                                                                FOLDER_PATH, 
                                                                FILE_SOURCE, 
                                                                FILE_TYPE, 
                                                                NUM_RECORDS, 
                                                                DATE_SOURCED, 
                                                                LAST_PROCESSED_DATE,
                                                                REMOTE_SYSTEM,
                                                                DISPLAY_NAME,
                                                                REQUEST_ID)
    VALUES (
    '{remote_system_customer_identifier}',
    '{file_name}',
    '{folder_path}',
    'ACAT',
    '{file_type}',
    {num_records},
    '{date_sourced}',
    '{last_processed_date}',
    'acat_customer_id',
    '{display_name}',
    '{request_id}');
    """

    print(update_metadata_query)
    con.execute(update_metadata_query)


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
        "orderedset"
    ],
    registry_url="837578041534.dkr.ecr.us-east-1.amazonaws.com/flows",
    files={
        """/Users/ejurotic/PycharmProjects/act-mce-src-and-prep/common/core_cr_fn.py""": "/root/.prefect/flows/common/core_cr_fn.py",
        """/Users/ejurotic/PycharmProjects/canvas-create-flow/canvas-create-flow/common/new_bulkload.py""": "/root/.prefect/flows/common/new_bulkload.py",
        """/Users/ejurotic/PycharmProjects/act-mce-src-and-prep/common/sec.py""": "/root/.prefect/flows/common/sec.py",
        """/Users/ejurotic/PycharmProjects/act-mce-src-and-prep/common/data_types.py""": "/root/.prefect/flows/common/data_types.py"
    },
    env_vars={"PYTHONPATH": "${PYTHONPATH}:/root/.prefect/flows/"},
)

with Flow(
        "prep_data_acat",
        storage=storage_obj,
        run_config=KubernetesRun(),
        executor=LocalDaskExecutor(scheduler="processes", num_workers=25),
        result=S3Result(bucket="cam-prefect-results"),
) as canvas_prep_data_acat:
    input_params = Parameter("input_params")
    bucket_name = input_params["bucket_name"]
    date = input_params['date']
    run_id = input_params["run_id"]
    schema = input_params['schema']
    this_file = input_params['this_file']
    multi_files = input_params['multi_files']
    single_files = input_params['single_files']
    single_files_out_pth = input_params['single_files_out_pth']
    multi_files_out_pth = input_params['multi_files_out_pth']
    full_canvas_out_pth = input_params['full_canvas_out_pth']

    files_split = get_and_split_into_multis_singles(this_file, single_files, multi_files, run_id)

    list_of_single_files = get_single_files(single_files,
                                            upstream_tasks=[files_split])

    list_of_multi_files = get_multi_files(multi_files,
                                          upstream_tasks=[files_split])

    single_prepped_file = do_prep_singles.map(this_parquet=list_of_single_files,
                                              out_path=unmapped(single_files_out_pth),
                                              upstream_tasks=[list_of_single_files])
    did_prep_multis = do_prep_multis.map(multi_file=list_of_multi_files,
                                         multi_files_out_pth=unmapped(multi_files_out_pth),
                                         upstream_tasks=[list_of_multi_files])

    acat_customer_id, date_sourced, display_name, num_of_instances = concat_multis_singles(
        single_files_out_pth, multi_files_out_pth, full_canvas_out_pth, run_id, this_file,
        upstream_tasks=[single_prepped_file, did_prep_multis])
    write_metadata_table(
        acat_customer_id,
        full_canvas_out_pth,
        bucket_name,
        num_of_instances,
        run_id,
        full_canvas_out_pth,
        date_sourced,
        display_name,
        upstream_tasks=[num_of_instances],
    )

input_params =  {
    "date": "2021-12-08",
    "run_id": "ACAT_NO_FILTERS_NORTHROP-GRUMMAN_JEHEGEDU_232_1614002122080_2021-02-22",
    "schema": "CPS_DSCI",
    "this_file": "s3://canvas-data-store-dev/ACAT_FILES/ACAT_NO_FILTERS_NORTHROP-GRUMMAN_JEHEGEDU_232_1614002122080_2021-02-22.parquet",
    "bucket_name": "canvas-data-store-dev",
    "multi_files": "s3://canvas-data-store-dev/ACAT_FILES/ACAT_NO_FILTERS_NORTHROP-GRUMMAN_JEHEGEDU_232_1614002122080_2021-02-22/multis/",
    "single_files": "s3://canvas-data-store-dev/ACAT_FILES/ACAT_NO_FILTERS_NORTHROP-GRUMMAN_JEHEGEDU_232_1614002122080_2021-02-22/singles/",
    "full_canvas_out_pth": "s3://canvas-data-store-dev/canvas_dir/ACAT_NO_FILTERS_NORTHROP-GRUMMAN_JEHEGEDU_232_1614002122080_2021-02-22/full_canvas/",
    "multi_files_out_pth": "s3://canvas-data-store-dev/canvas_dir/ACAT_NO_FILTERS_NORTHROP-GRUMMAN_JEHEGEDU_232_1614002122080_2021-02-22/multis_prepped/",
    "single_files_out_pth": "s3://canvas-data-store-dev/canvas_dir/ACAT_NO_FILTERS_NORTHROP-GRUMMAN_JEHEGEDU_232_1614002122080_2021-02-22/singles_prepped/"
  }




# EXEC_ADDRESS = "tcp://172.18.138.27:12349"
# executor = DaskExecutor(address=EXEC_ADDRESS)
# executor = DaskExecutor(cluster_kwargs={
#     "n_workers": 30,
#     "host": "127.0.0.1",
#     "scheduler_port": 64823,
#     "dashboard_address": ":8789",
#     "memory_limit": "200G",
#     "threads_per_worker": 5,
# })
if __name__ == "__main__":
    canvas_prep_data_acat.run(
        parameters={'input_params': input_params}
    )

# ###################################################################
# # fake in_list to represent the code you have now
# in_list = []
# for r in range(0, 16384):  # modify to test
#     in_list.append(r)

# actual_data_you_want = get_enriched_data(in_list, 16383)
# ###################################################################