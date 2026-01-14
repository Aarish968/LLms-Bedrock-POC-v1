import json
import pandas as pd
from prefect import Flow, Parameter, task
import prefect.executors
import time
import awswrangler as wr
from common import sec
from common import new_bulkload as bl
import tempfile
import math
import random
import os
from pathlib import Path
import shutil
from data_types import data_dict
import numpy as np
import io
import pandas as pd
import boto3
from sqlalchemy import create_engine
import xlsxwriter
from datetime import datetime

temp_base_location = '/tmp'

snowflake_db = "CPS_DB"
dn_key_name = "prd_cps_dsci_etl_svc"
schema = "CPS_DSCI_ARCHIVE"
warehouseMed = "cps_dsci_etl_wh"  # Medium
warehouseXsmall = "CPS_DSCI_ETL_EXT1_WH"  # X-Small
warehouseSmall = "CPS_DSCI_ETL_EXT2_WH"  # Small


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
        chunks.append(df[i * chunk_size:(i + 1) * chunk_size])
    return chunks


@task(log_stdout=True)
def split_patquets_local_tmp(this_df, split_size):
    file_num = 0
    f = create_working_space()
    chunk_size = 50000
    print(f"temp location {f}")
    chunks = split_dataframe(this_df, chunk_size)
    for ck in chunks:
        fn = os.path.join(f, 'chunk_{}.parquet'.format(file_num))
        ck.to_parquet(fn, engine='pyarrow', compression='snappy')
        file_num += 1
        print(fn)
    return f


@task()
def write_metadata_table(
        full_canvas_out_pth,
        num_of_instances,
        canvas_uid
):
    """

        create table CPS_DSCI_ARCHIVE.CREATED_CANVAS_META_DATA
        (
            JSON_LOC VARCHAR,
            DATE_CREATED TIMESTAMPNTZ,
            CANVAS_ID VARCHAR,
            CANVAS_LOC VARCHAR
        );



    """
    engine = create_engine(sec.get_sf_pw('prd_cps_dsci_etl_svc', warehouseXsmall, schema))
    con = engine.connect()

    json_loc = "s3://messaging.stage.cisco.com/start-canvas-creation/"

    date_created = datetime.now().date()

    update_metadata_query = f"""
    INSERT INTO CPS_DB.CPS_DSCI_ARCHIVE.CREATED_CANVAS_META_DATA (
                                                                JSON_LOC, 
                                                                DATE_CREATED, 
                                                                CANVAS_UID, 
                                                                CANVAS_LOC,
                                                                NUM_OF_INSTANCES
                                                                )
    VALUES (
    '{json_loc}',
    '{date_created}',
    '{canvas_uid}',
    '{full_canvas_out_pth}',
    '{num_of_instances}'
    );
    """

    print(update_metadata_query)
    con.execute(update_metadata_query)


def get_superset_of_columns(sorted_list_of_dfs: list, column_for_instance: str) -> set:
    """
    Get the superset of all columns from across all files.

    Args:
        - sorted_list_of_dfs (list): list of dfs sorted by date
        - column_for_instance (str): string of the column name for the instance id, is used as a pk
    """
    cols_per_df = []
    superset_of_cols = []

    for d in sorted_list_of_dfs:
        cols_per_df.append(list(set(d.columns)))
        for c in d.columns:
            superset_of_cols.append(c)

    superset_of_cols = list(set(superset_of_cols))

    return superset_of_cols


def get_superset_of_keys(sorted_list_of_dfs: list, column_for_instance: str) -> set:
    """
    Get superset of column_for_instance which is the pk in all files.

    Args:
        - sorted_list_of_dfs (list): list of dfs sorted by date
        - column_for_instance (str): string of the column name for the instance id, is used as a pk
    """
    keys_per_df = []
    superset_of_keys = []
    for df in sorted_list_of_dfs:
        sokeys = set(df[column_for_instance])
        keys_per_df.append(list(sokeys))
        superset_of_keys = [*superset_of_keys, *sokeys]
    keys_per_df
    superset_of_keys = set(superset_of_keys)

    return superset_of_keys


@task(log_stdout=True, nout=4)
def create_canvas_df(event: dict) -> pd.DataFrame:
    """
    Gets superset of keys and columns from multiple files and creates
    one superset df with the most current values.

    Args:
        event (dict): a json with a canvas id, and one or more 'files' with a name, loc and date value.

    """
    file_list = []
    canvas_id = params['canvas_id']
    dest_table_name = params['destination_table']
    schema = params['schema']
    engagement_id = params['engagement_id']
    for i in params["files"]:
        print(i)
        file_list.append(i)

    sorted_file_list = sorted(
        file_list, key=lambda k: k["date"]
    )  # sorts dict by date, to get them in chron order

    sorted_list_of_df = []

    column_for_instance = "instance_id"

    for file in sorted_file_list:
        file_df = wr.s3.read_parquet(path=file["loc"])
        for col in file_df.select_dtypes(include=["string"]):
            file_df[col] = file_df[col].astype('object')
            file_df[col] = file_df[col].replace(np.nan, None)
        col_list = file_df.columns
        for k in col_list:
            if k in data_dict:
                print(k, data_dict[k])
                print(file_df[k].dtype)
                if file_df[k].dtype == 'object' and data_dict[k] == 'Int64':
                    print("converting object to int")
                    file_df[k] = file_df[k].astype('str')
                    file_df[k] = file_df[k].astype('float')
                    file_df[k] = file_df[k].astype(data_dict[k])
                else:
                    file_df[k] = file_df[k].astype(data_dict[k])
        sorted_list_of_df.append(file_df)

    superset_of_cols = get_superset_of_columns(sorted_list_of_df, column_for_instance)
    superset_of_keys = get_superset_of_keys(sorted_list_of_df, column_for_instance)

    for i in sorted_list_of_df:
        i.set_index(column_for_instance, inplace=True)

    # remove key column and create empty dataframe with superset of columns and keys
    superset_of_cols.remove(column_for_instance)
    df_ = pd.DataFrame(index=list(superset_of_keys), columns=superset_of_cols)

    for j in sorted_list_of_df:
        df_.update(j)

    df_ = df_.reset_index(drop=False)
    df_ = df_.rename({"index": column_for_instance}, axis=1)

    print(df_.shape)
    df_['canvas_id'] = canvas_id
    df_['engagement_id'] = engagement_id
    df_['canvas_data'] = df_.apply(lambda x: x.to_json(date_format='iso', date_unit='s'), axis=1)

    date_created = datetime.now().date()
    canvas_uid = f"""{engagement_id}_{canvas_id}_{date_created}"""

    num_of_instances = len(df_)
    full_canvas_out_pth = f"""s3://canvas-data-store-dev/{canvas_uid}/"""

    dc1 = split_dataframe(df_, 100000)
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

    wr.s3.to_excel(df_.drop(['canvas_data'], axis=1),
                   f"""s3://canvas-data-store-dev/created_canvas/{canvas_uid}/canvas.xlsx""", index=False)

    return df_, num_of_instances, full_canvas_out_pth, canvas_uid


@task
def print_var(f):
    print(f)


# snowflake_db = "CPS_DB"
# dn_key_name = "prd_cps_dsci_etl_svc"
# schema = "CPS_DSCI_ARCHIVE"
# warehouseMed = "cps_dsci_etl_wh"  # Medium
# warehouseXsmall = "CPS_DSCI_ETL_EXT1_WH"  # X-Small
# warehouseSmall = "CPS_DSCI_ETL_EXT2_WH"  # Small


with Flow("canvas_create") as canvas_create:
    input_params = Parameter("input_params")

    engagement_id = input_params['engagement_id']
    canvas_id = input_params['canvas_id']
    snowflake_db = 'CPS_DB'
    warehouse = warehouseSmall
    cn = dn_key_name
    schema = input_params['schema']
    des_tbl = input_params['destination_table']
    canvas_df, num_of_instances, full_canvas_out_pth, canvas_uid = create_canvas_df(input_params)

    tmp_files = split_patquets_local_tmp(canvas_df, 50000)
    rr = bl.bulkload_existing_table(tmp_files, cn, schema, des_tbl, warehouse, ['canvas_data'], 4)
    clean_working_space(tmp_files, upstream_tasks=[rr])

    write_metadata_table(full_canvas_out_pth, num_of_instances, canvas_uid,
                         upstream_tasks=[rr])

canvas_create.visualize()



executor = prefect.executors.LocalExecutor()
# executor = prefect.executors.DaskExecutor(address="tcp://172.18.138.27:49095")

params = {
    "canvas_id": "ford_motors",
    "destination_table": "CANVAS_DATA_JSON",
    "schema": "CPS_DSCI_ARCHIVE",
    "date": "",
    "engagement_id": "testing_pf_1",
    "files": [
    {
    "name": "canvas_1623247379023",
    "loc": "s3://canvas-data-store-dev/canvas_dir/ACAT_NO_FILTERS_NNIT-AS_FABMARQU_230_1618935127398_2021-04-20/full_canvas/",
    "date": "2021-08-10"
    },
    {
    "name": "medium",
    "loc": "s3://canvas-data-store-dev/canvas_dir/ACAT_NO_FILTERS_NNIT-AS_FABMARQU_230_1606217346423_2020-11-24/full_canvas/",
    "date": "2021-08-09"
    }

    ]
    }

def main() -> None:
    canvas_create.run(executor=executor
                     , parameters={"input_params": params}
                     )

if __name__ == "__main__":
    main()