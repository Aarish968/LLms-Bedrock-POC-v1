
import pandas as pd
import pickle
import boto3
import logging
from SnowFlakeRepository import SnowFlake
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")


def get_file(bucket: str, key: str) -> pd.DataFrame:
    return pd.read_excel(f"s3://{bucket}/{key}", dtype={'deal_id': 'object',
                                                        'so_number': 'object'})

def pre_processing(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(['bill_to_party_id', 'deal_id', 'so_number', 'mapped_to_service_flag','cam_managed'], axis=1)
    return df

def persist_result(sf_cloud, processed_df, result):
    processed_df['predicted'] = str(result)
    sf = SnowFlake(sf_cloud)
    model_perdictions_table = f"""t_model_perdictions"""
    sf.df_to_sql(model_perdictions_table, processed_df, schema="CPS_DSCI_ARCHIVE")

def get_model_pickle():
    # get current model pickle
    key = 'finalized_alcon_model.sav'
    model_bucket = 'cam.dev.models'

    logger.info('reading hash.pic from s3')
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=model_bucket, Key=key)
    data = obj['Body'].read()
    loaded_model = pickle.loads(data)
    logger.info('read in hash.pic from s3')
    return loaded_model

def run(event, context):
    # Get excel that triggered the even that kicked off this lambda
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    file_key = event["Records"][0]["s3"]["object"]["key"]
    sf_cloud = False

    logger.info(f"Processing {file_key} from {bucket_name}")
    new_data = get_file(bucket_name, file_key)

    processed_df = pre_processing(new_data)

    loaded_model = get_model_pickle()

    result = loaded_model.predict(processed_df[:1])

    persist_result(sf_cloud, processed_df, result)

    if result[0] == 0:
        logger.info(f"{file_key} from {bucket_name} is predicted to be Managed")
    else:
        logger.info(f"{file_key} from {bucket_name} is predicted to be Not-Managed")
    return result





if __name__ == "__main__":
    event = {
      "Records": [
        {
          "s3": {
            "bucket": {
              "name": "cam.dev.model.inputs",
              "arn": "arn:aws:s3:::cam.dev.model.inputs"
            },
            "object": {
              "key": "alcon/test_3.xlsx"
            }
          }
        }
      ]
    }

    context = ''

    run(event, context)