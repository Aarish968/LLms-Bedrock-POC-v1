from prefect import Flow
import requests
import boto3
import os
from dotenv import load_dotenv
import ssl

load_dotenv(".env")

client = boto3.client('cognito-idp')

username = os.environ.get('USERNAME')
password = os.environ.get('PASSWORD')
user_pool_id = os.environ.get('USER_POOL_ID')
client_id = os.environ.get('CLIENT_ID')

with Flow('test-dc-backend') as flow:
    resp = client.admin_initiate_auth(
        UserPoolId=user_pool_id,
        ClientId=client_id,
        AuthFlow='ADMIN_NO_SRP_AUTH',
        AuthParameters={
            "USERNAME": username,
            "PASSWORD": password
        }
    )
    print("Log in success")
    url = "https://devdatacanvaswf.cisco.com/api/tasks"
    bearer_token = 'Bearer ' + resp['AuthenticationResult']['IdToken']
    headers = {"Authorization": bearer_token}
    data = {
        "user_id": "rfreedy@cisco.com",
    }
    response = requests.get(
        url,
        data=data,
        headers=headers,
        verify=False
    )
    print(response.raise_for_status())