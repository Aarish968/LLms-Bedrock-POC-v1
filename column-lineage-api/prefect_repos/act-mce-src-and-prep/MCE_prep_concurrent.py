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
from prefect.engine.results.s3_result import S3Result
from prefect.run_configs.kubernetes import KubernetesRun
from prefect.storage import Docker
from prefect.executors import DaskExecutor
from prefect.executors import LocalDaskExecutor
from prefect import task, unmapped, Flow, Parameter,flatten
from common import data_types
import psutil
import json
import boto3
import hashlib
from typing import Tuple

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 200)

snowflake_db = "CPS_DB"
dn_key_name = "prd_cps_dsci_etl_svc"
schema = "CPS_DSCI_ARCHIVE"
warehouseMed = "cps_dsci_etl_wh"  # Medium
warehouseXsmall = "CPS_DSCI_ETL_EXT1_WH"  # X-Small
warehouseSmall = "CPS_DSCI_ETL_EXT2_WH"  # Small

primary_multi_sort_key = 'product_coverage_end_date'
secondary_multi_sort_key = "product_coverage_start_date"

FORMAT = "[%(asctime)s, %(levelname)s] %(message)s"
logging.basicConfig(filename='logfile.log', level=logging.DEBUG, format=FORMAT)


def mem_reference(ref_name):
    pidNum = os.getpid()
    process = psutil.Process(os.getpid())
    mb = int(process.memory_info().rss) / 1024 ** 2
    print(f"<MEM LOG>: {ref_name} = {mb} MB  pid: {pidNum}")  # mb


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


def get_instance_number(instance_ids):
    engine = create_engine(sec.get_sf_pw(dn_key_name, warehouseXsmall, schema))
    if len(instance_ids) == 1:
        df = pd.read_sql(
            f"""select INSTANCE_ID, INSTANCE_NUMBER from
"EDW_SERVICE_ETL_DB"."SS"."CSF_XXCCS_DS_INSTANCE_DETAIL" i  where INSTANCE_ID in ({instance_ids[0]})
""",
            engine,
        )
    else:
        df = pd.read_sql(
            f"""select INSTANCE_ID, INSTANCE_NUMBER from
    "EDW_SERVICE_ETL_DB"."SS"."CSF_XXCCS_DS_INSTANCE_DETAIL" i  where INSTANCE_ID in {instance_ids}
    """,
            engine,
        )

    return df


column_for_instance = 'instance_number'


def active_signed(stscode_list):
    if len(stscode_list) == 2:
        if stscode_list[0] == 'ACTIVE' and stscode_list[1] == 'SIGNED':
            return 'Y'

    else:
        return 'N'


# try itter instead
def flatten_list(this_list):
    can_use = True
    try:
        iter(this_list)
    except:
        can_use = False

    if can_use:
        c = ' --> '.join(map(str, this_list))
        return str(c)

    if isinstance(this_list, (int, float)):
        return str(this_list)

    if isinstance(this_list, (str, object)):
        return str(this_list)

    return ''


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
    if not pd.isna(iid):
        if int(iid) > 74981032:
            return iid
        else:
            return -1
    else:
        return 0


def fix_numbers(s):
    s = pd.to_numeric(s.convert_dtypes(), errors='coerce')
    s = pd.to_numeric(s, errors='coerce').convert_dtypes()
    return s


def prep_data(df):
    col_list = df.columns
    for k in col_list:
        if k in data_types.data_dict:
            if data_types.data_dict[k] in ["Int64", "float64"]:  # "str" had this
                df[k] = fix_numbers(df[k])
            if data_types.data_dict[k] in ["datetime64[ns]"]:
                df[k] = pd.to_datetime(df[k], errors='coerce')
            if data_types.data_dict[k] in ["object"]:
                df[k] = df[k].astype("str")
        elif k.startswith('tag_'):
            df[k] = df[k].astype("object")
        else:
            df[k] = df[k].astype("str")

    return df


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


@task(log_stdout=True)
def get_core_file_list(s3Path):
    fls = wr.s3.list_objects(s3Path)
    # print(f"total files : {len(fls)}")
    # print(f"ALL CORE FILES: {fls}")
    thinned = []
    for f in fls:
        # print(f)
        if f.endswith('.parquet'):
            thinned.append(f)
    # print(f"thinned total files : {len(thinned)}")
    print(f"thinned CORE FILES: {thinned}")

    return thinned


def do_prep_function(this_df):
    this_df = this_df.rename(columns={"engagement_id": "mce_engagement_id", "engagement_name": "mce_engagement_name"})
    this_df.columns = fix_cols(this_df)

    mem_reference('starting do_prep_function')

    if "parent_instance_id" in this_df.columns:
        this_df["parent_instance_number"] = this_df.apply(lambda x: ez_instance_number(x["parent_instance_id"]), axis=1)
        instance_id_list = (this_df["parent_instance_id"][this_df["parent_instance_number"] == -1]).to_list()

        # alanzen  Why chunked?
        n = 16000
        if len(instance_id_list) > 0:
            instance_id_list_chunks = [instance_id_list[i:i + n] for i in range(0, len(instance_id_list), n)]
            this_df.set_index("instance_number", inplace=True, drop=False)
            print("about to fix instance_id_list")
            if instance_id_list:
                for instance_id_chunk in instance_id_list_chunks:
                    print("Fixing instance_id_list")
                    instance_num_id_df = get_instance_number(tuple(instance_id_chunk))
                    instance_num_id_df = prep_data(instance_num_id_df)
                    instance_num_id_df.set_index("instance_id", inplace=True)
                    this_df.update(instance_num_id_df)

    #     if "parent_instance_number" in this_df.columns:
    #         parent_instance_id_list = (this_df["parent_instance_id"][this_df["instance_number"] == -1]).to_list()
    #         if len(parent_instance_id_list) > 0:
    #             parent_instance_id_list_chunks = [parent_instance_id_list[i:i + n] for i in
    #                                               range(0, len(parent_instance_id_list), n)]
    #             if parent_instance_id_list:
    #                 print("Fixing parent_instance_id_list")
    #                 for parent_instance_id_list_chunk in parent_instance_id_list_chunks:
    #                     parent_instance_num_id_df = get_instance_number(tuple(parent_instance_id_list_chunk))
    #                     parent_instance_num_id_df = prep_data(parent_instance_num_id_df)
    #                     parent_instance_num_id_df.set_index("instance_id", inplace=True)
    #                     this_df.update(parent_instance_num_id_df)

    this_df = this_df.reset_index(drop=True)

    mem_reference("end do_prep_function")

    return this_df


@task(log_stdout=True, nout=11, tags=["snowflake"])
def create_location_params(id, date, bucket_name, file_loc):
    # update IS_PREPPED so that other runs dont pick this run up
    engine = create_engine(sec.get_sf_pw(dn_key_name, warehouseXsmall, schema))
    con = engine.connect()
    update_old_p_to_fail = f"""update CPS_DSCI_ARCHIVE.MCE_ENGAGEMENT_TRACKING_META set is_prepped = 'FAIL' where is_prepped = 'P' and datediff( 'day',CURRENT_TIMESTAMP, MCE_ENGAGEMENT_TRACKING_META.PREPPED_DATE ) > 1"""
    update_mce_source_table = f""" update CPS_DSCI_ARCHIVE.MCE_ENGAGEMENT_TRACKING_META set IS_PREPPED ='P', PREPPED_DATE = CURRENT_TIMESTAMP where DATE_BUCKET = '{date}' and ENGAGEMENT_NUMBER = {id}"""
    print(update_mce_source_table)
    con.execute(update_old_p_to_fail)
    con.execute(update_mce_source_table)
    this_file = file_loc
    # if date.startswith('2022'):
    #     this_file = f"""s3://{bucket_name}/MCE_FILES/2022/{id}/{date}/{id}/"""
    # else:
    #     this_file = f"""s3://{bucket_name}/MCE_FILES/2021/{id}/{date}/{id}/"""
    #     this_file = "s3://canvas-data-store-dev/MCE_FILES/3640_mce_test/3640_mce_test.parquet"
    #     this_file = f"""s3://{bucket_name}/MCE_FILES/{id}"""
    canvas_output_location = f"""/canvas_dir/mce_canvas_{id}/{date}/"""
    canvas_parquet_folder = f"""mce_canvas_{id}_{date}"""

    date_sourced = date

    display_name = f"mce_no_filters_{id}_{date_sourced}"

    multi_files = f"""{this_file}multis/"""
    single_files = f"""{this_file}singles/"""
    full_canvas_out_pth = f"""s3://{bucket_name}{canvas_output_location}full_canvas/"""
    multi_files_out_pth = f"""s3://{bucket_name}{canvas_output_location}multis_prepped/"""
    single_files_out_pth = f"""s3://{bucket_name}{canvas_output_location}singles_prepped/"""

    filtered_parent_master_files = f"{this_file}data_"

    return this_file, canvas_output_location, canvas_parquet_folder, date_sourced, display_name, multi_files, single_files, full_canvas_out_pth, multi_files_out_pth, single_files_out_pth, filtered_parent_master_files


@task(log_stdout=True, nout=3)
def do_prep_multis(multi_file, full_canvas_out_pth):
    print(f"this is the multi location {multi_file}")
    has_file = True
    num_instances = 0
    ram_usage = get_used_ram()
    try:
        this_df = wr.s3.read_parquet(path=multi_file, path_suffix=".parquet")
    except:
        has_file = False
        print(f"No Files at {multi_file}")
        this_df = pd.DataFrame()

    if has_file and 'contract_start_date' in this_df.columns:
        mem_reference("do_prep_multis start")
        this_df = do_prep_function(this_df)
        this_df = prep_data(this_df)
        gb_res = (
            this_df.sort_values(
                by=["instance_id", primary_multi_sort_key, secondary_multi_sort_key], ascending=False,
            ).groupby("instance_id")
                .agg(
                product_coverage_line_number_list=("contract_number", "unique"),
                cpl_renewed_list=("cpl_renewed", "unique"),
                cpl_renewable_list=("cpl_renewable", "unique"),
                maintenance_so_number_list=("maintenance_so_number", "unique"),
                maintenance_po_number_list=("maintenance_po_number", "unique"),
                sa_creation_date=("sa_creation_date", "min"),
                sa_last_update_date=("sa_last_update_date", "max"),
                exs_number_flag_list=("exs_number_flag", "unique"),
                product_coverage_status_list=("product_coverage_status", "unique"),
                contract_number_list=("contract_number", "unique"),
                contract_start_date=("contract_start_date", "min"),
                contract_end_date=("contract_end_date", "max"),
                service_level_list=("service_level", "unique"),
                service_level_description_list=("service_level_description", "unique"),
                service_level_start_date_list=("service_level_start_date", "unique"),
                service_level_end_date_list=("service_level_end_date", "unique"),
                coverage_line_id_cpl_id_list=("coverage_line_id_cpl_id", "unique"),
                product_coverage_end_date=("product_coverage_end_date", "max"),
                coverage_details_months_list=("coverage_details_months", "unique"),
                coverage_ends_sortable_cal_q_list=(
                    "coverage_ends_sortable_cal_q",
                    "unique",
                ),
                coverage_ends_sortable_fq_list=("coverage_ends_sortable_fq", "unique"),
                coverage_starts_sortable_cal_q_list=(
                    "coverage_starts_sortable_cal_q",
                    "unique",
                ),
                coverage_starts_sortable_fq_list=(
                    "coverage_starts_sortable_fq",
                    "unique",
                ),
                contract_expired_category_list=("contract_expired_category", "unique"),
                line_count=("product_coverage_status", "count"),
                expiration_range_list=("expiration_range", "unique"),
            )
        )

    elif has_file:
        mem_reference("do_prep_multis start")
        this_df = do_prep_function(this_df)
        this_df = prep_data(this_df)
        gb_res = (
            this_df.sort_values(
                by=["instance_id", primary_multi_sort_key, secondary_multi_sort_key], ascending=False,
            ).groupby("instance_id")
                .agg(
                product_coverage_line_number_list=("contract_number", "unique"),
                cpl_renewed_list=("cpl_renewed", "unique"),
                cpl_renewable_list=("cpl_renewable", "unique"),
                maintenance_so_number_list=("maintenance_so_number", "unique"),
                maintenance_po_number_list=("maintenance_po_number", "unique"),
                sa_creation_date=("sa_creation_date", "min"),
                sa_last_update_date=("sa_last_update_date", "max"),
                exs_number_flag_list=("exs_number_flag", "unique"),
                product_coverage_status_list=("product_coverage_status", "unique"),
                contract_number_list=("contract_number", "unique"),

                contract_end_date=("contract_end_date", "max"),
                service_level_list=("service_level", "unique"),
                service_level_description_list=("service_level_description", "unique"),
                service_level_start_date_list=("service_level_start_date", "unique"),
                service_level_end_date_list=("service_level_end_date", "unique"),
                coverage_line_id_cpl_id_list=("coverage_line_id_cpl_id", "unique"),
                product_coverage_end_date=("product_coverage_end_date", "max"),
                coverage_details_months_list=("coverage_details_months", "unique"),
                coverage_ends_sortable_cal_q_list=(
                    "coverage_ends_sortable_cal_q",
                    "unique",
                ),
                coverage_ends_sortable_fq_list=("coverage_ends_sortable_fq", "unique"),
                coverage_starts_sortable_cal_q_list=(
                    "coverage_starts_sortable_cal_q",
                    "unique",
                ),
                coverage_starts_sortable_fq_list=(
                    "coverage_starts_sortable_fq",
                    "unique",
                ),
                contract_expired_category_list=("contract_expired_category", "unique"),
                line_count=("product_coverage_status", "count"),
                expiration_range_list=("expiration_range", "unique"),
            )
        )

        gb_res["modified_record"] = "multi_line_fix"
        gb_res["is_active_signed"] = gb_res.apply(
            lambda x: active_signed(x["product_coverage_status_list"]), axis=1
        )

        ######## find out logic behind why these were chosen  #############
        # gb_res['flat_contract_bill_to_customer_name'] = gb_res.apply(
        #     lambda x: flatten_list(x['contract_bill_to_customer_name_list']), axis=1)
        #
        # gb_res['flat_contract_billto_gu_name'] = gb_res.apply(lambda x: flatten_list(x['contract_billto_gu_name_list']),
        #                                                       axis=1)
        # gb_res['flat_contract_bid_parent_party_name'] = gb_res.apply(
        #     lambda x: flatten_list(x['contract_bid_parent_party_name_list']), axis=1)
        #
        # gb_res['flat_service_line_name'] = gb_res.apply(lambda x: flatten_list(x['service_line_name_list']), axis=1)
        # print(gb_res[gb_res["line_count"] > 2])
        # list of columns that were grouped with unique in order to iter over them to build the note
        grouped_columns = [
            "product_coverage_line_number_list",
            "cpl_renewed_list",
            "cpl_renewable_list",
            "maintenance_so_number_list",
            "maintenance_po_number_list",
            "exs_number_flag_list",
            "product_coverage_status_list",
            "contract_number_list",
            "service_level_list",
            "service_level_description_list",
            "service_level_start_date_list",
            "service_level_end_date_list",
            "coverage_line_id_cpl_id_list",
            "coverage_details_months_list",
            "coverage_ends_sortable_cal_q_list",
            "coverage_ends_sortable_fq_list",
            "coverage_starts_sortable_cal_q_list",
            "coverage_starts_sortable_fq_list",
            "contract_expired_category_list",
            "line_count",
            "expiration_range_list",
        ]

        gb_res["note"] = gb_res.apply(lambda x: make_note(x), axis=1)

        for col in gb_res[grouped_columns]:
            gb_res[f"""audit_{col}"""] = gb_res.apply(lambda x: flatten_list(x[col]), axis=1)

        gb_res = gb_res.drop(columns=grouped_columns)
        gb_res = gb_res.reset_index(drop=False)

        gb_res = prep_data(gb_res)

        mem_reference("do_prep_multis end")
        ##########################################gb_res
        # get supretst and write to file
        # return nac the list of columns

        multi_line_sort = ['instance_id', 'coverage_line_id_cpl_id', primary_multi_sort_key, secondary_multi_sort_key]
        this_df.sort_values(multi_line_sort, inplace=True)
        print(f" shape before drop dups {this_df.shape}")
        this_df = this_df.drop_duplicates(subset='instance_id', keep='last')
        print(f" shape after drop dups {this_df.shape}")

        this_df.set_index('instance_id', inplace=True)
        gb_res.set_index('instance_id', inplace=True)

        needed_cols = list(set(gb_res.columns).difference(set(this_df.columns)))
        for c in needed_cols:
            this_df[c] = np.nan

        this_df.update(gb_res)

        num_instances = this_df.shape[0]

        this_df.reset_index(drop=False, inplace=True)

        # added this bc it was null in final  alanzen 12-17-2021
        this_df["modified_record"] = "multi_line_fix"

        this_df = prep_data(this_df)
        # ram_usage
        ram_usage = get_used_ram()

        this_df.to_parquet(
            os.path.join(full_canvas_out_pth, "multi_flat.parquet"),
            engine="pyarrow",
            compression="snappy",
            index=False,
            allow_truncated_timestamps=True,
            coerce_timestamps="ms",
        )

        mem_reference("do_prep_multis end")

    else:
        this_df = pd.DataFrame()
        num_instances = 0
        ram_usage = get_used_ram()
    return this_df.columns, num_instances, ram_usage


@task(log_stdout=True)
def calc_issues(this_file):
    # returns a list of duplicated incidents ids
    this_df_issues = wr.s3.read_parquet(path=this_file, columns=['INSTANCE_ID', 'INSTANCE_NUMBER'],
                                        path_suffix=".parquet")
    this_df_issues.columns = fix_cols(this_df_issues)
    print(f"Dup Columns:{this_df_issues.columns.duplicated()}")
    this_df_issues = do_prep_function(this_df_issues)
    this_df_issues = prep_data(this_df_issues)

    print(f" total lines in core files: {this_df_issues.shape}")
    # print(this_df_issues.info())

    cntt = (
        this_df_issues[["instance_number", "instance_id"]].groupby("instance_id").count()
    )

    cntt.reset_index(drop=False, inplace=True)
    issues = cntt[['instance_id']][cntt.instance_number > 1]

    problems = list(issues.instance_id.values)
    print(f"num of multi instances {len(problems)}")
    return problems


def md5_str(v):
    return hashlib.md5(f"{v}".encode()).hexdigest()


def hash_cols(x):
    # gb_list = ["instance_id",'contract_number','service_level_start_date','service_level_end_date','coverage_line_id_cpl_id', "product_coverage_line_number"]
    return hashlib.md5(
        f"{x.instance_id}-{x.contract_number}-{x.service_level_start_date}-{x.service_level_end_date}-{x.coverage_line_id_cpl_id}-{x.product_coverage_line_number}".encode()).hexdigest()


# this_df_issues["keeper_hash"] = this_df_issues.apply(lambda x: hash_cols(x), axis=1)


@task(log_stdout=True)
def get_issue_records(this_file, out_path, multi_instances_ids, display_name):
    # create separate files with mullti probelems
    print(f"File for issue records : {this_file}")
    # print(multi_instances_ids)

    print(out_path)
    testing_slice = wr.s3.read_parquet(path=this_file, path_suffix=".parquet")
    testing_slice.columns = fix_cols(testing_slice)

    testing_slice = prep_data(testing_slice)

    print(f"testing_slice pre {testing_slice.shape}")
    # i want JUST the issue records for post processing
    testing_slice = testing_slice[testing_slice.instance_id.isin(multi_instances_ids)]
    print(f"testing_slice post {testing_slice.shape}")
    if testing_slice.shape[0] > 0:
        testing_slice[f"""canvas_source_file_{display_name}"""] = 1

        # fn = md5_str(this_file)
        fn = os.path.join(out_path, this_file.split(os.sep)[-1])

        # fnn = os.path.join(out_path, f"multi_rec_{fn}.parquet")

        print(f"File out issue records : {fn}")

        testing_slice.to_parquet(
            fn,
            engine="pyarrow",
            compression="snappy",
            index=False,
            allow_truncated_timestamps=True,
            coerce_timestamps="ms",
        )
    return True


@task(log_stdout=True)
def write_singles(f, column_list, multi_instances_ids, full_canvas_out_pth, display_name):
    print(f"Write Singles file:{f}")

    # print(multi_instances_ids)

    this_df = wr.s3.read_parquet(path=f, path_suffix=".parquet")
    this_df.columns = fix_cols(this_df)
    this_df = do_prep_function(this_df)
    this_df = prep_data(this_df)

    this_df[f"""canvas_source_file_{display_name}"""] = 1
    this_df["modified_record"] = "no"

    # print(this_df.dtypes)

    needed_cols = list(set(column_list).difference(set(this_df.columns)))

    for col in needed_cols:
        print(f"adding column {col}")
        this_df[col] = ''

    print(f"start singles cnt {this_df.shape}")
    # i want to kepp the NON issues in this DF
    this_df = this_df[~this_df.instance_id.isin(multi_instances_ids)]
    print(f"end singles cnt {this_df.shape}")
    # assures correct order
    # this_df=prep_data(this_df)
    num_instances = int(this_df.shape[0])
    print('num_instances')
    print(this_df.shape)
    fn = os.path.join(full_canvas_out_pth, f.split(os.sep)[-1])
    print(fn)

    # ram_usage
    ram_usage = get_used_ram()

    this_df[column_list].to_parquet(
        os.path.join(fn),
        engine="pyarrow",
        compression="snappy",
        index=False,
        allow_truncated_timestamps=True,
        coerce_timestamps="ms",
    )
    task_stats = [num_instances, ram_usage]
    return task_stats


@task(log_stdout=True)
def clean_locations(canvas_output_location, multi_files, single_files, multi_files_out_pth, single_files_out_pth):
    if len(multi_files.split('/')) > 4:  # at least at some depth to avoid horrific deleteion of lots fo data
        print(f"deleting all files : {multi_files}")
        wr.s3.delete_objects(multi_files)

    if len(canvas_output_location.split('/')) > 4:  # at least at some depth to avoid horrific deleteion of lots fo data
        print(f"deleting all files : {canvas_output_location}")
        wr.s3.delete_objects(canvas_output_location)

    if len(single_files.split('/')) > 4:  # at least at some depth to avoid horrific deleteion of lots fo data
        print(f"deleting all files : {single_files}")
        wr.s3.delete_objects(single_files)

    if len(multi_files_out_pth.split('/')) > 4:  # at least at some depth to avoid horrific deleteion of lots fo data
        print(f"deleting all files : {multi_files_out_pth}")
        wr.s3.delete_objects(multi_files_out_pth)

    if len(single_files_out_pth.split('/')) > 4:  # at least at some depth to avoid horrific deleteion of lots fo data
        print(f"deleting all files : {single_files_out_pth}")
        wr.s3.delete_objects(single_files_out_pth)

    return True

@task(log_stdout=True)
def get_num_instances(task_stats):
    file_counts = task_stats
    print('new file_counts')
    print(file_counts)
    total = 0
    for c in file_counts:
        try:
            total += c
            print('new total')
            print(total)
        except:
            pass
    print('total total')
    print(total)
    return total


@task(log_stdout=True, nout = 2)
def get_smart_acct(list_of_files, task_stats, multi_cols):
    file_counts  = task_stats
    print('file_counts')
    print(file_counts)
    this_df = wr.s3.read_parquet(path=list_of_files[0], path_suffix=".parquet")
    this_df.columns = fix_cols(this_df)
    try:
        smart_account_id = this_df["smart_account_id"][0]
    except:
        smart_account_id = 0

    print(f"smart account id{smart_account_id}")

    total = 0
    for c in file_counts:
        try:
            total += int(c[0])
            print('total')
            print(total)
        except:
            pass

    try:
        total += int(multi_cols)
    except:
        pass

    return smart_account_id, total


def get_used_ram():
    mem = psutil.virtual_memory()
    used_ram = mem.active
    return int(used_ram)


def write_dict_to_json_file_in_s3(dictionary, bucket, key):
    """onverts a dict to a json file and writes to an s3 bucket/key location """
    s3 = boto3.resource('s3')
    s3object = s3.Object(bucket, key)
    s3object.put(
        Body=(bytes(json.dumps(dictionary).encode('UTF-8')))
    )


@task(log_stdout=True)
def write_tuning_data(multi, singles, path, rows, canvas_parquet_folder, bucket_name):
    real_cpu_cnt = psutil.cpu_count(logical=False)
    try:
        if multi > 0:
            singles.append(multi)
    except:
        print("there are no  multis")
    print('write_tuning_data singles ')
    print(singles)
    all_singles_mem = []
    for i in singles:
        try:
            all_singles_mem.append(i[1])
        except:
            pass

    max_observed_ram = max(all_singles_mem)

    # write json with  max ram, file_path ( soruce location) number of cores, rows out
    tuning_dict = {"file_path": path,
                   "file_rows": rows,
                   "real_cpu_count": real_cpu_cnt,
                   "max_observed_ram": max_observed_ram,
                   }
    print("TUNNING DICT #############")
    print(tuning_dict)
    write_dict_to_json_file_in_s3(tuning_dict, bucket_name,
                                  f"""TUNINING_DATA/MCE_PREP/{canvas_parquet_folder}_meta.json""")

    return True


@task(tags=["snowflake"], log_stdout=True)
def write_metadata_table(
        smart_account_id,
        canvas_file_key,
        bucket,
        num_of_instances,
        engagement_id,
        canvas_output_location,
        date_sourced,
        display_name,
        full_canvas_out_pth,
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


    Sandeeps table

    INSERT INTO CPS_DB.CPS_BIA_BR.CES_CAM_CANVAS_DATA_SOURCES
    ( REMOTE_SYSTEM_CUSTOMER_IDENTIFIER, FILE_NAME, FOLDER_PATH, FILE_SOURCE, FILE_TYPE, NUM_RECORDS, DATE_SOURCED, LAST_PROCESSED_DATE, REMOTE_SYSTEM)
    VALUES ( 254613, 'segment_0.parquet', 's3://canvas-data-store-dev/canvas_dir/mce_canvas_1589810633571/', 'MCE', 'all', 6281, '2020-05-18', '2021-08-13', 'mce_smart_account')


    """
    engine = create_engine(sec.get_sf_pw(dn_key_name, warehouseXsmall, schema))
    con = engine.connect()

    remote_system_customer_identifier = smart_account_id
    file_name = "*.parquet"
    folder_path = f"""s3://{bucket}{canvas_output_location}"""
    file_type = "all"
    num_records = num_of_instances
    print('final num of instances written to SF')
    print(num_records)
    last_processed_date = datetime.now()



    update_mce_source_table = f""" update CPS_DSCI_ARCHIVE.MCE_ENGAGEMENT_TRACKING_META set IS_PREPPED ='T' where DATE_BUCKET = '{date_sourced}' and ENGAGEMENT_NUMBER = {engagement_id}"""
    print(update_mce_source_table)
    con.execute(update_mce_source_table)

    if "_" in date_sourced: # looking to see if date_sourced is the old format or new format like '2022-01-20_18_57_38'
        date_sourced = date_sourced.split("_")[0] # if its the new format , take only the date portion so we can write to a date type sf column
    else:
        date_sourced = date_sourced

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
    '{engagement_id}',
    '{file_name}',
    '{full_canvas_out_pth}',
    'MCE',
    '{file_type}',
    {num_records},
    '{date_sourced}',
    '{last_processed_date}',
    'mce_engagement_id',
    '{display_name}',
    '{engagement_id}');
    """

    print(update_metadata_query)
    con.execute(update_metadata_query)

    if smart_account_id != 0:
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
        '{smart_account_id}',
        '{file_name}',
        '{full_canvas_out_pth}',
        'MCE',
        '{file_type}',
        {num_records},
        '{date_sourced}',
        '{last_processed_date}',
        'mce_smart_account',
        '{display_name}',
        '{engagement_id}');
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
        """/Users/ejurotic/PycharmProjects/act-mce-src-and-prep/common/data_types.py""": "/root/.prefect/flows/common/data_types.py",
        """/Users/ejurotic/PycharmProjects/act-mce-src-and-prep/common/sec.py""": "/root/.prefect/flows/common/sec.py"
    },
    env_vars={"PYTHONPATH": "${PYTHONPATH}:/root/.prefect/flows/"},
)

with Flow(
        "prep_data_mce",
        storage=storage_obj,
        run_config=KubernetesRun(),
        executor=LocalDaskExecutor(scheduler="processes", num_workers=30),
        result=S3Result(bucket="cam-prefect-results"),
) as canvas_prep_data_mce:
    input_params = Parameter("input_params")
    bucket_name = input_params["bucket_name"]
    engagement_id = input_params["id"]
    date = input_params["date"]
    file_loc = input_params["file_loc"]
    schema = input_params["schema"]

    this_file, canvas_output_location, canvas_parquet_folder, date_sourced, display_name, multi_files, single_files, full_canvas_out_pth, multi_files_out_pth, single_files_out_pth, filtered_parent_master_files = create_location_params(
        engagement_id, date, bucket_name, file_loc)

    clean = clean_locations(full_canvas_out_pth, multi_files, single_files, multi_files_out_pth, single_files_out_pth)

    # need full context no way to parallel ~ 2 min for a big one
    # s3://canvas-data-store-dev/MCE_FILES/2021/1630561314397/2021-12-15/1630561314397/
    multi_instances_ids = calc_issues(this_file, upstream_tasks=[clean])

    fls = get_core_file_list(this_file, upstream_tasks=[clean])

    prepM = get_issue_records.map(
        this_file=fls,
        out_path=unmapped(multi_files),
        multi_instances_ids=unmapped(multi_instances_ids),
        display_name=unmapped(display_name)
    )

    list_of_columns, multi_cols, multi_mem_usage = do_prep_multis(multi_files, full_canvas_out_pth,
                                                                  upstream_tasks=[prepM])

    # master_files = get_multi_files(filtered_parent_master_files,upstream_tasks=[clean])

    # task_data is a list of file_counts, singles_mem_usage
    task_data = write_singles.map(f=fls,
                                                       column_list=unmapped(list_of_columns),
                                                       multi_instances_ids=unmapped(multi_instances_ids),
                                                       full_canvas_out_pth=unmapped(full_canvas_out_pth),
                                                       display_name=unmapped(display_name)
                                                       # task_args={"nout": 2}
                                                       )



    smart_account_id, num_of_instances = get_smart_acct(fls, task_data, multi_cols,
                                                        upstream_tasks=[task_data])

    write_metadata_table(
        smart_account_id,
        canvas_parquet_folder,
        bucket_name,
        num_of_instances,  # need
        engagement_id,
        canvas_output_location,
        date_sourced,
        display_name,
        full_canvas_out_pth,
        upstream_tasks=[num_of_instances, smart_account_id],
    )

    done = write_tuning_data(multi_mem_usage, task_data, this_file, num_of_instances, canvas_parquet_folder,
                             bucket_name)

# input_params = {
#     "id": "1610735941018",
#     "date": "2021-10-29",
#     "schema": "CPS_DSCI",
#     "bucket_name": "canvas-data-store-dev"
# }

input_params =  {
    "id": "1585756726997",
    "date": "2022-02-11_12_17_32",
    "file_loc": "",
    "schema": "CPS_DSCI",
    "rerun_flag": False,
    "bucket_name": "canvas-data-store-dev"
  }


if __name__ == "__main__":
    canvas_prep_data_mce.run(
        parameters={"input_params": input_params}
    )