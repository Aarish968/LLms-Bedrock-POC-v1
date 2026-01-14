import boto3
import os
from dotenv import load_dotenv

load_dotenv(".env")

client = boto3.client('cognito-idp')
username = os.environ.get('USERNAME')
temp_password = os.environ.get('TEMP_PASSWORD')
new_password = os.environ.get('PASSWORD')
user_pool_id = os.environ.get('USER_POOL_ID')
client_id = os.environ.get('CLIENT_ID')
user_email = os.environ.get('USER_EMAIL')

def main():
    resp = client.admin_initiate_auth(
        UserPoolId=user_pool_id,
        ClientId=client_id,
        AuthFlow='ADMIN_NO_SRP_AUTH',
        AuthParameters={
            "USERNAME": username,
            "PASSWORD": temp_password
        }
    )
    print(resp)
    cr = {
        'NEW_PASSWORD': new_password,
        'USERNAME': username,
        'userAttributes.name': user_email
    }
    new_pass_resp = client.admin_respond_to_auth_challenge(
        ChallengeName='NEW_PASSWORD_REQUIRED',
        UserPoolId=user_pool_id,
        ClientId=client_id,
        ChallengeResponses=cr,
        Session=resp['Session'],
    )
    print(new_pass_resp)
if __name__ == '__main__':
    main()