

import warnings

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.model_selection import cross_val_score, KFold

warnings.filterwarnings('ignore')
from prefect.executors import LocalDaskExecutor
import dask.dataframe as dd
from dask_ml.model_selection import train_test_split
from dask.distributed import Client
from prefect import Flow, task, resource_manager
import prefect
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score
from sklearn.metrics import confusion_matrix
from sklearn.model_selection import GridSearchCV
from xgboost import XGBClassifier
import joblib
import pickle
import dask
import os


# http://172.18.138.27:8087/notebooks/erp_cloud/alcon_quick.ipynb


@resource_manager
class DaskCluster:
    """Create a temporary dask cluster.

    Args:
        - cluster_kwargs (dict): options to forward to the backing Dask cluster manager
    """

    def __init__(self, cluster_kwargs=None):
        self.cluster_kwargs = cluster_kwargs or {}

    def setup(self):
        """Create a temporary dask cluster, returning the `Client`"""
        # Here we use a LocalCluster, but you could just as well use any other dask cluster manager.
        from dask.distributed import LocalCluster
        dask.config.set({'scheduler.work-stealing': False})
        dask.config.set({'distributed.comm.timeouts.connect': '60s'})
        dask.config.set({'distributed.scheduler.work-stealing': False})
        workers = os.cpu_count() - 1

        cluster = LocalCluster(n_workers=workers, host='127.0.0.1', scheduler_port=12345, dashboard_address=':8787',
                               memory_limit='1G', threads_per_worker=1)

        # Another option would be to use dask-kubernetes.
        # See https://kubernetes.dask.org/en/latest/kubecluster.html for more info.
        #
        # from dask_kubernetes import KubeCluster
        # cluster = KubeCluster('worker-spec.yml')  # you'll likely want some config specific to your deployment here
        # cluster.adapt()

        return Client(cluster)

    def cleanup(self, client):
        """Shutdown the temporary dask cluster"""
        cluster = client.cluster

        # Ignore any errors shutting down the client, they're innocuous
        try:
            client.close()
        except Exception:
            pass

        # Shutdown the cluster. An error here will be marked by prefect and
        # show up in the UI (if using Cloud/server), but won't fail the flow.
        # It will let you observe the failure, so if it's something you want to
        # investigate you can.
        cluster.close()



@task(name="Load Data")
def load_data():
    ## Dropping all columns that have nulls and also wont be used in the final model.
    df = dd.read_csv(
        "alcon-2021-02-19T17-47-45.362Z.csv",
        dtype={'deal_id': 'object',
               'so_number': 'object'},
        storage_options={"anon": True}
    )

    return df


@task(name="Pre Processing")
def pre_processing(df: dd.DataFrame) -> dd.DataFrame:
    ## Dropping all columns that have nulls and also wont be used in the final model.
    df = df.drop(['bill_to_party_id', 'deal_id', 'so_number', 'mapped_to_service_flag'], axis=1)
    df = df.compute()  # This brings the DataFrame from Dask to Pandas
    return df


@task(name="Train Model")
def train_model(df: dd.DataFrame):
    target_col = ['cam_managed']

    categorical_columns = [
        'country_code_iso',
        'product_family',
        'business_unit',
        'catalog_product_type',
        'item_type',
        'customer_name_mod',
        'state_mod',
        'city_mod',
        # 'postal_code_mod',
        # 'address4_mod',
        'gen_location']

    numerical_columns = ['erp_list_price', 'ship_date_days_prior']

    X = df[categorical_columns + numerical_columns]
    y = df[target_col]

    # must transform the target column to a an array of 1s and 0s
    label_encoder = LabelEncoder()
    y = label_encoder.fit_transform(y)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, random_state=42, test_size=0.8)  # stratify not working for some reason

    categorical_encoder = OneHotEncoder(handle_unknown='ignore')
    numerical_pipe = Pipeline([
        ('imputer', SimpleImputer(strategy='mean'))
    ])

    preprocessing = ColumnTransformer(
        [('cat', categorical_encoder, categorical_columns),
         ('num', numerical_pipe, numerical_columns)])

    full_pipeline = Pipeline([
        ('preprocess', preprocessing),
        ('classifier', XGBClassifier(booster='gbtree',
                                     objective='multi:softprob',
                                     learning_rate=0.1,
                                     n_estimators=100,
                                     random_state=2,
                                     n_jobs=-1,
                                     missing=-999.0,
                                     num_class=2))])

    client = Client('127.0.0.1:64820')

    with joblib.parallel_backend('dask', scatter=[X_train, y_train]):
        full_pipeline.fit(X_train, y_train)

    logger = prefect.context.get("logger")
    logger.info("full_pipeline train accuracy: %0.3f" % full_pipeline.score(X_train, y_train))
    logger.info("full_pipeline test accuracy: %0.3f" % full_pipeline.score(X_test, y_test))

    return full_pipeline


@task(name="Save Model")
def save_model(full_pipeline):
    filename = 'finalized_alcon_model.sav'
    pickle.dump(full_pipeline, open(filename, 'wb'))
    # joblib.dumps(full_pipeline, "/tmp/model.pkl") #other pckl option
    return


with Flow("ml-flow") as flow:
    df = load_data()
    df = pre_processing(df)
    with DaskCluster() as client:
        model = train_model(df)
    saved = save_model(model)

flow.run()
flow.exectuor = LocalDaskExecutor()



# can then use this to read any model that we have saved.

from xgboost import XGBClassifier
import pandas as pd
import pickle

filename = 'finalized_alcon_model.sav'
new_data = pd.read_csv('alcon-2021-02-19T17-47-45.362Z.csv')
loaded_model = pickle.load(open(filename, 'rb'))
result = loaded_model.predict(new_data[:1])
result