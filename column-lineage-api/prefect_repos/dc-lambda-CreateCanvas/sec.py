import os
import hvac
import json
from urllib.parse import quote_plus as urlquote
from sqlalchemy.dialects import registry
import socket
registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')
import boto3
s3_client = boto3.client("s3")
ssm_client = boto3.client("ssm", "us-east-1")

def decrypt_parameter(parameter_name: str):
    parameter = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
    return parameter["Parameter"]["Value"]

PREFECT_AUTH_TOKEN = decrypt_parameter(f"/cam/{os.getenv('STAGE')}/prefect/token")

sf_key_info = {

    'prd_cps_dsci_etl_svc':
        {
            'url': 'https://east.keeper.cisco.com',
            'namespace': 'cloudDB',
            'token': decrypt_parameter(f"/cam/{os.getenv('STAGE')}/snowflake/token"),
            'secret': 'secret/snowflake/prd/cps_dsci_etl_svc/password',
            'snowflake_db_engine_str': 'snowflake://cps_dsci_etl_svc:{pw}@cisco.us-east-1/CPS_DB/{schema}?warehouse={wh}&role=cps_dsci_etl_role',
            'cloud_snowflake_db_engine_str': 'snowflake://cps_dsci_etl_svc:{pw}@cisco.us-east-1.privatelink/CPS_DB/{schema}?warehouse={wh}&role=cps_dsci_etl_role'
        },
    'dev_cps_dsci_etl_svc':
        {
            'url': 'https://east.keeper.cisco.com',
            'namespace': 'cloudDB',
            'token': decrypt_parameter(f"/cam/{os.getenv('STAGE')}/snowflake/token"),
            'secret': 'secret/snowflake/dev/cps_dsci_etl_svc/password',
            'snowflake_db_engine_str': 'snowflake://cps_dsci_etl_svc:{pw}@ciscodev.us-east-1/CPS_DB/{schema}?warehouse={wh}',
            'cloud_snowflake_db_engine_str': 'snowflake://cps_dsci_etl_svc:{pw}@ciscodev.us-east-1.privatelink/CPS_DB/{schema}?warehouse={wh}'

        },
    'stg_cps_dsci_etl_svc':
        {
            'url': 'https://east.keeper.cisco.com',
            'namespace': 'cloudDB',
            'token': decrypt_parameter(f"/cam/{os.getenv('STAGE')}/snowflake/token"),
            'secret': 'secret/snowflake/stg/cps_dsci_etl_svc/password',
            'snowflake_db_engine_str': 'snowflake://cps_dsci_etl_svc:{pw}@ciscostage.us-east-1.privatelink/CPS_DB/{schema}?warehouse={wh}'
        },
    'dev_cps_bia_etl_svc':
        {
            'url': 'https://east.keeper.cisco.com',
            'namespace': 'cloudDB',
            'token': decrypt_parameter(f"/cam/{os.getenv('STAGE')}/bia_snowflake/token"),
            'secret': 'secret/snowflake/dev/cps_bia_etl_svc/password',
            'snowflake_db_engine_str': 'snowflake://cps_bia_etl_svc:{pw}@ciscodev.us-east-1/CPS_DB/{schema}?warehouse={wh}&role=cps_bia_etl_role',
            'cloud_snowflake_db_engine_str': 'snowflake://cps_bia_etl_svc:{pw}@ciscodev.us-east-1.privatelink/CPS_DB/{schema}?warehouse={wh}&role=cps_bia_etl_role'
        }

}


def get_sf_pw(conn_name, wh, schema):
    cn = sf_key_info[conn_name.lower()]
    client = hvac.Client(
        url=cn['url'],
        namespace=cn['namespace'],
        token=cn['token']
    )
    jsonval = client.read(cn['secret'])
    value = json.dumps(jsonval["data"])
    pw = urlquote(json.loads(value)['passwd'])
    if socket.gethostname() in ('cps-node-009' , 'ALANZEN-P865P', 'EJUROTIC-M-45YS'):
        con_string = cn['snowflake_db_engine_str'].format(pw=pw, schema=schema, wh=wh)
    else:
        con_string = cn['cloud_snowflake_db_engine_str'].format(pw=pw, schema=schema, wh=wh)
    return con_string



