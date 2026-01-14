
import pickle

import logging

import numpy as np

import warnings
from prefect.storage import Docker
import boto3
import pandas as pd
warnings.filterwarnings('ignore')

from prefect import Flow, task, resource_manager, Parameter
from prefect.engine.results.s3_result import S3Result
from prefect.executors.dask import LocalDaskExecutor
from prefect.run_configs.kubernetes import KubernetesRun

import awswrangler as wr

from sqlalchemy import create_engine
from common import sec

import json
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")

@task()
def load_data(parquet_loc):

    df = wr.s3.read_parquet(path=parquet_loc)

    return df
@task()
def pre_processing(df: pd.DataFrame) -> pd.DataFrame:
    # should this all be in the preprocess flow?

    df = df[df.last_date_of_support_flg == 'active']
    cols_to_exclude = ['ship_date_days_prior', 'erp_list_price', 'rnd']
    for col in df.columns:
        if col not in cols_to_exclude:
            df[col] = df[col].astype('category')
    return df

@task()
def persist_result( result):
    # processed_df['predicted'] = str(result)
    print(result.head())
    print(result.info)
    engine = create_engine(sec.get_sf_pw('prd_cps_dsci_etl_svc', 'cps_dsci_etl_wh', 'CPS_DSCI_ARCHIVE'))
    result.to_sql('PREDICT_MANAGED'.lower(), schema='CPS_DSCI_ARCHIVE', con=engine, index=False,
                                            if_exists='append',chunksize=16000 )
    return True

@task()
def get_model_pickle(model_file_key):
    # model_file_key key =  """trained_models/578_model_NNuYlptWkx.sav"""
    key = model_file_key
    model_bucket = 'ds-data-store-prod'

    logger.info('reading hash.pic from s3')
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=model_bucket, Key=key)
    data = obj['Body'].read()
    loaded_model = pickle.loads(data)
    logger.info('read in hash.pic from s3')
    return loaded_model

def get_payload_in_file(bucket_name: str, object_key: str):
    json_object = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    payload = json_object["Body"].read()
    return json.loads(payload)

@task()
def get_preds(loaded_model,processed_df, eng_id):
    print(loaded_model.classes_)

    def which_class_is_positive(mod, class_value):
        itr = 0
        for c in mod.classes_:
            if class_value == c:
                print(c, itr)
                return itr
            itr += 1
        return

    prob_column = 'managed'

    # this should be persisted somewhere so they alwasy match between training and preds
    categorical_columns = [  # 'std_exclusion_flg',
        # 'address4_mod',
        'business_unit',
        # 'catalog_product_type',
        # 'city_mod',
        # 'contract_org_id',
        'country_code_iso',
        # 'customer_id',
        'customer_name_mod',
        # 'deal_id',
        'gen_location',
        # 'gu_id',
        # 'install_site_id',
        # 'is_servicable',
        # 'is_spm_coverable',
        # 'item_type',
        # 'last_date_of_support_flg',
        # 'mapped_to_service_flag',
        # 'po_number',
        # 'postal_code_mod',
        'product_family',
        # 'serviceable_product_flag',
        # 'site_business_entity',
        # 'so_number',
        # 'state_mod'
    ]

    # categorical_columns = ['std_exclusion_flg', 'business_unit',
    #  'catalog_product_type', 'city_mod', 'country_code_iso',  'customer_name_mod',
    #  'gen_location',
    #  'gu_id',
    #  'is_spm_coverable',
    #  'postal_code_mod',
    #  'product_family',
    #  'serviceable_product_flag',
    #  'site_business_entity',
    #  'state_mod']


    # 'install_site_id', 'customer_id' 'address4_mod',
    # categorical_columns = [#'po_number', 'so_number','deal_id',
    #        'country_code_iso',
    #        'site_business_entity', 'gu_id','product_family',
    #        'business_unit', 'catalog_product_type', 'item_type',
    #        'customer_name_mod', 'state_mod', 'city_mod', 'postal_code_mod',
    #         'gen_location']

    numerical_columns = ['erp_list_price', 'ship_date_days_prior', 'instance_id']

    Xt = processed_df[categorical_columns + numerical_columns]

    preds = loaded_model.predict_proba(Xt)
    Xt['prediction_{}_prob'.format(prob_column)] = np.array(preds[:, which_class_is_positive(loaded_model, prob_column)])

    reasonably_confident_predictions = Xt[(Xt.prediction_managed_prob >= .8)]

    pd.options.display.float_format = '{:.0f}'.format

    # prepending instance_id with engagement_id to keep the predicitons within the scope of an engagement
    def create_eng_in_id_col(x):
        return str(f"""{eng_id}_{str(x)}""")

    reasonably_confident_predictions['eng_instance_id'] = ''

    reasonably_confident_predictions['eng_instance_id'] = reasonably_confident_predictions.apply(
        lambda x: create_eng_in_id_col(x['instance_id']), axis=1)

    return reasonably_confident_predictions





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
        "orderedset",
        "sklearn"

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


with Flow("ml-model-predict",
        storage=storage_obj,
        run_config=KubernetesRun(memory_request=60000000000),
        # executor=LocalDaskExecutor(scheduler="processes", num_workers=psutil.cpu_count(logical=True)),
        executor=LocalDaskExecutor(scheduler="processes", num_workers=2),
        result=S3Result(bucket="cam-prefect-results")
) as flow:
    file_loc = Parameter("file_loc", required=True)
    model_file_key = Parameter("model_file_key", required=True)
    eng_id = Parameter("eng_id", required=True)
    new_data = load_data(file_loc)
    processed_df = pre_processing(new_data)
    loaded_model = get_model_pickle(model_file_key) # """trained_models/578_model_NNuYlptWkx.sav"""
    result = get_preds(loaded_model,processed_df, eng_id)
    persist_result( result)

if __name__ == "__main__":
    flow.run(
        parameters=   {
      "eng_id": 578,
            "model_file_key": "trained_models/578_model_AHrTuOJHWE.sav",
            "file_loc": "s3://ds-data-store-prod/prepped_data/578/2022_08_08/578_model_AHrTuOJHWE.parquet"
    }
    )
