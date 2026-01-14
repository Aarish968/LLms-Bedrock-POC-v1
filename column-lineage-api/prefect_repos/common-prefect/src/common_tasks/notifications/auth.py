import json
from typing import Any, Literal, Optional

from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs


class GetAccessToken(Task):
    
    """
    Task to retrieve an access token from AWS Cognito.
    It will inject the 'token' tag into the tags of the task for later
    consumption by ApiNotificationHandler.
    """
    
    def __init__(self, region_name: str = "us-east-1"):
        super().__init__()
        self.region_name = region_name
        self.tags.add("token")
        
    @defaults_from_attrs("region_name")
    def run(self, settings: Any, region_name: Optional[str] = None) -> str:
        """
        Settings should be an object with the following attributes:
        - env: Literal['dev', 'prod']
        
        """
        token = get_access_token(settings.env, region_name)
        return token



def get_access_token(
    env: Literal['dev', 'prod'], region_name: str = "us-east-1"
    ) -> str:
    """
    This function will retrieve an access token from AWS Cognito.
    """
    import boto3
    
    if env is None or env not in {'dev', 'prod'}:
        raise Exception("env must be either 'dev' or 'prod'")
    
    secret_name = f"{env}/Cognito"
    
    session = boto3.session.Session(region_name=region_name)
    
    client_ssm = session.client(service_name='secretsmanager')
    client_cognito = session.client(
        service_name='cognito-idp', region_name=region_name
    )
    
    response = client_ssm.get_secret_value(SecretId=secret_name)
    secret = json.loads(response['SecretString'])
    
    auth = client_cognito.admin_initiate_auth(
        UserPoolId=secret['UserPoolId'],
        ClientId=secret['ClientId'],
        AuthFlow=secret['AuthFlow'],
        AuthParameters={
            'USERNAME': secret['USERNAME'],
            'PASSWORD': secret['PASSWORD']
        }
    )
    return auth['AuthenticationResult']['AccessToken']
