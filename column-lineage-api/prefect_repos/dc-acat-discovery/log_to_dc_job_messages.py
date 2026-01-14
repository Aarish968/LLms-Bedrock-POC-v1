import json
import requests
import boto3
from common import sec
from sqlalchemy import create_engine
from common import config
from prefect import  task
from prefect.triggers import all_successful, all_failed, all_finished,any_failed
temp_base_location = '/mnt/newmt/ERP/home/alanzen/bulk_tmp'
import prefect

def get_correct_schema(env):
    if env == 'prod':
        return 'CPS_DSCI_API'
    else:
        return 'CPS_DSCI_BR'




def check_env(env):
    print(env)
    if env == "dev":
        cn = "dev_cps_dsci_etl_svc"
    elif env == "stage":
        cn = "stg_cps_dsci_etl_svc"
    elif env == "prod":
        cn = "prd_cps_dsci_etl_svc"
    return cn


@task(log_stdout=True)
def final_flow_state_message(sf_env,notification_id, requested_by,flow_params):
    excel_loc = flow_params.excel_output_uri


    print(f"final flow state : requested_by {requested_by} , notification_id {notification_id}")


    try:
        if notification_id != 0:
            params_list = 'result'

            r = call_notifications_api(
                sf_env,
                notification_id,
                requested_by,
                params_list,
                notification_type='update_state')

    except Exception as e:
        print(r)
        print(e)


    try:
        if notification_id != 0:
            params_list = {
                "type": "message",
                    "data": {
                    "excel_location": excel_loc }
            }

            r = call_notifications_api(
                sf_env,
                notification_id,
                requested_by,
                params_list,
                notification_type='append')

    except Exception as e:
        print(r)
        print(e)

    return True


@task(log_stdout=True,trigger=any_failed)
def final_failed_flow_state_message(sf_env,notification_id, requested_by,):

    print(f"final flow state FAILED : requested_by {requested_by} , notification_id {notification_id}")

    try:
        if notification_id != 0:
            params_list = 'error'

            r = call_notifications_api(
                sf_env,
                notification_id,
                requested_by,
                params_list,
                notification_type='update_state')

    except Exception as e:
        print(r)
        print(e)

    return True

# def log_to_dc_job_messages(sf_env,request_id, log_message):
#     cn = check_env('prod')
#     correct_schema = get_correct_schema(sf_env)
#
#     engine = create_engine(
#         sec.get_sf_pw(cn, "CPS_DSCI_ETL_EXT1_WH", correct_schema)
#     )
#
#     con = engine.connect()
#
#
#     bia_qry = f"""
#     insert into {correct_schema}.dc_job_messages(request_id,logged_message) values ({request_id},'{log_message}')
#     """
#
#     try:
#         con.execute(bia_qry)
#     except Exception as e:
#         print(e)
#         print(
#             f"Failed while attempting to log message to : {correct_schema}.dc_job_messages"
#         )
#
#     return True


def demo_cognito_api_auth(env,service_name,region_name):
    secret_id = f"{env}/Cognito"


    session = boto3.session.Session()
    client_ssm = session.client(
        service_name=service_name,
        region_name=region_name,
    )
    cognito_secret_raw = json.loads(
        client_ssm.get_secret_value(SecretId=secret_id)["SecretString"]
    )


    cognito_client = session.client(
        service_name="cognito-idp",
        region_name=region_name,
    )




    response_raw = cognito_client.admin_initiate_auth(
        UserPoolId=cognito_secret_raw['UserPoolId'],
        ClientId=cognito_secret_raw['ClientId'],
        AuthFlow=cognito_secret_raw['AuthFlow'],
        AuthParameters={
            "USERNAME": cognito_secret_raw['USERNAME'],
            "PASSWORD": cognito_secret_raw['PASSWORD'],
        },
    )


    AuthenticationResult = response_raw['AuthenticationResult']
    Access_Token = AuthenticationResult['AccessToken']

    return Access_Token


def call_notifications_api(env, notification_id, logged_user, params_list, notification_type):
    auth_token = demo_cognito_api_auth(env, region_name="us-east-1", service_name="secretsmanager")
    res_log = []

    logged_user_request_param = logged_user.replace('@', '%40')

    dev_endpoint = "https://devdatacanvaswf.cisco.com"

    prod_endpoint = "https://datacanvaswf.cisco.com"

    if env == 'prod':
        endpoint = prod_endpoint
    elif env == 'dev':
        endpoint = dev_endpoint

    # if notification_type == 'new':
    #     full_request_uri = f'{endpoint}/api/v2/workflows/notifications?logged_user={logged_user_request_param}'
    #     headers = {'Authorization': f'Bearer {auth_token}', 'Content-Type': 'application/json'}
    #     r = requests.post(full_request_uri,
    #                       headers=headers, verify=False, json=params_list)

    if notification_type == 'append':
        full_request_uri = f'{endpoint}/api/v2/workflows/notifications/message/{notification_id}'
        headers = {'Authorization': f'Bearer {auth_token}', 'Content-Type': 'application/json'}
        r = requests.patch(full_request_uri,
                           headers=headers, verify=False, json=params_list)

    if notification_type == 'update_state':
        full_request_uri = f'{endpoint}/api/v2/workflows/notifications/{notification_id}/{params_list}?logged_user={logged_user_request_param}'
        headers = {'Authorization': f'Bearer {auth_token}', 'Content-Type': 'application/json'}
        r = requests.post(full_request_uri,
                          headers=headers, verify=False, json=params_list)



    request_status = f"Status Code: {r.status_code}, Response: {r.json()}"
    print(request_status)
    res = r.json()
    res_log.append(res)
    print(res_log)

    return request_status


def log_to_dc_job_messages(sf_env, request_id, log_message, requested_by, notification_id):
    #
    # requested_by = config.requested_by
    # notification_id = config.notification_id


    # requested_by = prefect.context.requested_by
    # notification_id = prefect.context.notification_id

    cn = check_env('prod')
    correct_schema = get_correct_schema(sf_env)

    engine = create_engine(
        sec.get_sf_pw(cn, 'CPS_DSCI_ETL_EXT1_WH', correct_schema)
    )

    con = engine.connect()

    bia_qry = f"""
    insert into {correct_schema}.dc_job_messages(request_id,logged_message) values ({request_id},'{log_message}')
    """

    try:
        con.execute(bia_qry)
    except Exception as e:
        print(e)
        print(
            f"Failed while attempting to log message to : {correct_schema}.dc_job_messages"
        )

    try:
        if notification_id != 0:
            params_list = [
                {
                    "type": 'message',
                    "data": log_message,
                }
            ]

            r = call_notifications_api(
                sf_env,
                notification_id,
                requested_by,
                params_list,
                notification_type='append')

    except Exception as e:
        print(r)
        print(e)

    return True