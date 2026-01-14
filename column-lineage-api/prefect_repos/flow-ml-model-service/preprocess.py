

import warnings

import boto3
import pandas as pd
warnings.filterwarnings('ignore')
from aws_sec import aws_sec
import psutil
from dask.distributed import Client
from prefect import Flow, task, resource_manager, Parameter
from prefect.engine.results.s3_result import S3Result
from prefect.executors.dask import LocalDaskExecutor
from prefect.run_configs.kubernetes import KubernetesRun
import random
import string
from datetime import date
import awswrangler as wr
import dask
import os
from sqlalchemy import create_engine
from common import sec
from common import core_cr_fn
import datetime as dt
from managed_query import get_managed_qry
import random as rnd
import json
from prefect.storage import Docker
# http://172.18.138.27:8087/notebooks/erp_cloud/alcon_quick.ipynb




@task(name= "Load data")
def load_data(parquet_loc):

    df = wr.s3.read_parquet(path=parquet_loc)

    return df


@task(name="Pre Processing")
def pre_processing(df: pd.DataFrame) -> pd.DataFrame:
    def c2flag(c):
        return 'con_{}'.format(c)

    def do_rnd():
        return rnd.random()

    def ldos(d, eff_date=dt.datetime.now()):
        if d < eff_date:
            return 'ldos'
        else:
            return 'active'

    def std_e(_pid):
        if _pid in std_exclusion:
            return 'out'
        else:
            return 'in'

    df['cn'] = df.apply(lambda x: c2flag(x.contract_number), axis=1)
    df.dropna(axis=0, how='all')



    std_exclusion = ['AIR100', 'AIR1000', 'AIR100U', 'AIR1040', 'AIR110A', 'AIR110U', 'AIR11A', 'AIR11U', 'AIR120A',
                     'AIR120R', 'AIR120U', 'AIR12A', 'AIR12U', 'AIR130A', 'AIR130U', 'AIR13A', 'AIR140A', 'AIR14U',
                     'AIR150U', 'AIR1540', 'AIR1560', 'AIR1570', 'AIR15U', 'AIR1700', 'AIR1800', 'AIR1810', 'AIR1815',
                     'AIR1830', 'AIR1850', 'AIR2000', 'AIR2700', 'AIR2800', 'AIR3000', 'AIR340', 'AIR350', 'AIR3500',
                     'AIR35SE', 'AIR35SI', 'AIR3700', 'AIR3800', 'AIR4800', 'AIR500A', 'AIR500U', 'AIRANT', 'AIRAP',
                     'AIRBNDL', 'AIRCA', 'AIRCELL', 'AIRCI', 'AIRCMN', 'AIRIA', 'AIRINFA', 'AIRINFE', 'AIRINFU',
                     'AIRIW', 'AIRMGMA', 'AIRMGMU', 'AIRMHW', 'AIRMHW2', 'AIRMHW3', 'AIRMHW4', 'AIRMOD', 'AIRMSTH',
                     'AIRNCS', 'AIROLD', 'AIRPWR', 'AIRSNSR', 'AIRWAN', 'C9130AX', 'C9115AX', 'C9117AX', 'C9120AX',
                     'IPPHONE', 'PHON3PC', 'PHONCOL', 'PHONE', 'PHONVID', 'PHONVOC', 'SBPHONE', 'WPHONE', 'CNSWTCH',
                     'CNWRL', 'CNCOMM', 'CNSEC', 'CNEMM', 'CNVSN', 'GRHW', 'CNAPM', 'GSHW', 'GRLIC']



    df['std_exclusion_flg'] = df.apply(lambda x: std_e(x['pid']), axis=1)

    df['last_date_of_support_flg'] = df.apply(lambda x: ldos(x['last_date_of_support']), axis=1)

    df['customer_name'] = df['customer_name'].str.replace('\d+', '')
    df['customer_name_mod'] = df.apply(
        lambda x: core_cr_fn.remove_whitespace("{}".format(x['customer_name'])), axis=1)
    df['state_mod'] = df.apply(lambda x: core_cr_fn.remove_whitespace("{}".format(x['state'])), axis=1)

    df['city_mod'] = df.apply(lambda x: core_cr_fn.remove_whitespace("{}".format(x['city'])), axis=1)

    df['postal_code_mod'] = df.apply(
        lambda x: core_cr_fn.remove_whitespace("{}".format(x['postal_code'])), axis=1)

    df['address4_mod'] = df.apply(lambda x: core_cr_fn.remove_whitespace("{}".format(x['address4'])),
                                                  axis=1)
    df = df.drop(['state', 'city', 'postal_code', 'address4', 'customer_name', 'last_date_of_support'],
                                 axis=1)

    cols_to_exclude = ['ship_date_days_prior', 'erp_list_price', 'rnd']

    df['gen_location'] = df.apply(
        lambda x: "{}:{}:{}".format(x['country_code_iso'], x['state_mod'], x['postal_code_mod']), axis=1)

    df['instance_id'] = df['instance_id'].astype(int)



    df = df.drop(
                ['ship_date', 'warranty_end_date', 'last_date_of_renewal', 'last_date_of_service_attach'], axis=1)


    for col in df.columns:
        if col not in cols_to_exclude:
            df[col] = df[col].astype('category')

    return df





@task(name="Save Prepped df", nout = 2)
def save_prepped_df(prepped_df, eng_id):
    def gen_min_id():
        letters = string.ascii_letters
        random_string = '{}'.format(''.join(random.choice(letters) for i in range(10)))
        return random_string
    today = date.today()
    aws_canvas_out_pth = f"s3://ds-data-store-prod/prepped_data/{eng_id}/{today.strftime('%Y_%m_%d')}"
    file_id = f"{eng_id}_model_{gen_min_id()}"
    aws_path = os.path.join(aws_canvas_out_pth, f"{file_id}.parquet")
    wr.s3.to_parquet(prepped_df, path=aws_path, compression='snappy', index=False)

    return aws_path,file_id


@task(name="Writing Json to s3 for to trigger training")
def write_message_for_training(eng_id, aws_path,file_id):
    def write_dict_to_json_file_in_s3(dictionary, bucket, key):
        """converts a dict to a json file and writes to an s3 bucket/key location """
        session = boto3.Session(
            aws_access_key_id=aws_sec.ACCESS_KEY,
            aws_secret_access_key=aws_sec.SECRET_KEY
        )

        s3 = session.resource('s3')
        s3object = s3.Object(bucket, key)
        s3object.put(
            Body=(bytes(json.dumps(dictionary).encode('UTF-8')))
        )
        return True

    tuning_dict = {"file_path": aws_path,
                   "eng_id": eng_id,
                   "date": str(date.today()),
                   }
    bucket_name = "ds-data-store-prod"
    key = f"""trigger_training/{file_id}.json"""
    write_dict_to_json_file_in_s3(tuning_dict, bucket_name, key)
    return True


storage_obj = Docker(
    base_image="prefecthq/prefect:0.15.3-python3.8",
    python_dependencies=[
        "pandas==1.1.3",
        "awswrangler==2.10.0",
        "numpy==1.19.2",
        "boto3==1.18.16",
        "aiohttp",
        "hvac",
        "snowflake-sqlalchemy==1.2.4",
        "cloudpickle==2.1.0",
        "s3fs==0.4",
        "hvac>0.11.0",
        "SQLAlchemy>1.3.0",
        "awswrangler>2.10.0",
        "fastparquet>0.7.1",
        "oyaml",
        "orderedset"

    ],
    registry_url="837578041534.dkr.ecr.us-east-1.amazonaws.com/flows",
    files={
        """/Users/ejurotic/PycharmProjects/mlModelService/common/core_cr_fn.py""": "/root/.prefect/flows/common/core_cr_fn.py",
        """/Users/ejurotic/PycharmProjects/mlModelService/common/sec.py""": "/root/.prefect/flows/common/sec.py",
        """/Users/ejurotic/PycharmProjects/mlModelService/aws_sec/aws_sec.py""": "/root/.prefect/flows/aws_sec/aws_sec.py",
        """/Users/ejurotic/PycharmProjects/mlModelService/managed_query.py""":"/root/.prefect/flows/managed_query.py",
    },
    env_vars={"PYTHONPATH": "${PYTHONPATH}:/root/.prefect/flows/"},
)




with Flow("ml-model-preprocess",
        storage=storage_obj,
        run_config=KubernetesRun(memory_request=60000000000),
        # executor=LocalDaskExecutor(scheduler="processes", num_workers=psutil.cpu_count(logical=True)),
        executor=LocalDaskExecutor(scheduler="processes", num_workers=2),
        result=S3Result(bucket="cam-prefect-results")
) as flow:
    eng_id = Parameter("eng_id", required=True)
    file_path = Parameter("file_path", required=True)
    df = load_data(file_path)
    prepped_df = pre_processing(df)
    aws_path,file_id = save_prepped_df(prepped_df,eng_id)
    write_message_for_training(eng_id, aws_path,file_id)

if __name__ == "__main__":
    flow.run(
        parameters=   {
      "eng_id": 578,
    "file_path": 's3://ds-data-store-prod/sourced_data/578/2022_08_08/578_model_EshHzKQNmP.parquet'
    }
    )



