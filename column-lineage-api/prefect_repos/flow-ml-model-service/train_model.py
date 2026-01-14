

import warnings

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.model_selection import cross_val_score, KFold, train_test_split
from prefect.storage import Docker
warnings.filterwarnings('ignore')
from prefect.executors import LocalDaskExecutor
import dask.dataframe as dd
from dask.distributed import Client
from prefect import Flow, task, resource_manager,Parameter
import prefect
from prefect.engine.results.s3_result import S3Result
from prefect.executors.dask import LocalDaskExecutor
from prefect.run_configs.kubernetes import KubernetesRun
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score
from sklearn.metrics import confusion_matrix
from sklearn.model_selection import GridSearchCV
from xgboost import XGBClassifier
import joblib
import pickle
import dask
import os
import awswrangler as wr
from aws_sec import aws_sec
import boto3
from sklearn.ensemble import RandomForestClassifier
# http://172.18.138.27:8087/notebooks/erp_cloud/alcon_quick.ipynb
import pandas as pd



@task(name="Load Data")
def load_data(parquet_loc):
    session = boto3.Session(
        aws_access_key_id=aws_sec.ACCESS_KEY,
        aws_secret_access_key=aws_sec.SECRET_KEY
    )

    df = wr.s3.read_parquet(path=parquet_loc, boto3_session=session)

    return df


@task(name="Pre Processing")
def pre_processing(df: pd.DataFrame) -> pd.DataFrame:
    # should this all be in the preprocess flow?

    df = df[df.last_date_of_support_flg == 'active']
    cols_to_exclude = ['ship_date_days_prior', 'erp_list_price', 'rnd']
    for col in df.columns:
        if col not in cols_to_exclude:
            df[col] = df[col].astype('category')
    return df


@task(name="Train Model")
def train_model(df: pd.DataFrame):
    target_col = ['cam_managed']

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

    numerical_columns = ['erp_list_price', 'ship_date_days_prior','instance_id']

    X = df[categorical_columns + numerical_columns]
    y = df[target_col]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, random_state=42, test_size=0.2)

    categorical_encoder = OneHotEncoder(handle_unknown='ignore')
    numerical_pipe = Pipeline([
        ('imputer', SimpleImputer(strategy='mean'))
    ])

    preprocessing = ColumnTransformer(
        [('cat', categorical_encoder, categorical_columns),
         ('num', numerical_pipe, numerical_columns)])

    rf = Pipeline([
        ('preprocess', preprocessing),
        ('classifier', RandomForestClassifier(random_state=42, n_jobs=-1))
    ])





    rf.fit(X_train, y_train)

    logger = prefect.context.get("logger")
    logger.info("full_pipeline train accuracy: %0.3f" % rf.score(X_train, y_train))
    logger.info("full_pipeline test accuracy: %0.3f" % rf.score(X_test, y_test))

    return rf


@task(name="Save Model")
def save_model(full_pipeline,file_id):
    def write_object_to_s3(obj, bucket, key):
        session = boto3.Session(
            aws_access_key_id=aws_sec.ACCESS_KEY,
            aws_secret_access_key=aws_sec.SECRET_KEY
        )

        s3 = session.resource('s3')
        s3object = s3.Object(bucket, key)
        s3object.put(
            Body=(obj)
        )
        return True





    pickle_obj = pickle.dumps(full_pipeline)
    bucket_name = "ds-data-store-prod"
    key = f"""trained_models/{file_id}.sav"""
    write_object_to_s3(pickle_obj, bucket_name, key)


    return


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


with Flow("ml-model-train",
            storage=storage_obj,
            run_config=KubernetesRun(memory_request=60000000000),
            # executor=LocalDaskExecutor(scheduler="processes", num_workers=psutil.cpu_count(logical=True)),
            executor=LocalDaskExecutor(scheduler="processes", num_workers=2),
            result=S3Result(bucket="cam-prefect-results")
) as flow:
    parquet_loc = Parameter("parquet_loc", required=True)
    file_id = Parameter("file_id", required=True)
    df = load_data(parquet_loc)
    df = pre_processing(df)
    model = train_model(df)
    saved = save_model(model,file_id)

if __name__ == "__main__":
    flow.run(
        parameters=   {
      "parquet_loc": "s3://ds-data-store-prod/prepped_data/578/2022_08_08/578_model_AHrTuOJHWE.parquet",
      "file_id" : "578_model_AHrTuOJHWE"
    }
    )

# can then use this to read any model that we have saved.

# from xgboost import XGBClassifier
# import pandas as pd
# import pickle
#
# filename = 'finalized_alcon_model.sav'
# new_data = pd.read_csv('alcon-2021-02-19T17-47-45.362Z.csv')
# loaded_model = pickle.load(open(filename, 'rb'))
# result = loaded_model.predict(new_data[:1])
# result