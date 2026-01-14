import json
from typing import Literal, Optional, TypedDict

from prefect import Task
from prefect.engine.signals import FAIL
from prefect.utilities.tasks import defaults_from_attrs


class CognitoSecret(TypedDict):
    UserPoolId: str
    ClientId: str
    AuthFlow: str
    USERNAME: str
    PASSWORD: str


class CognitoLoginTask(Task):
    """
    This task extends SSMSecret that retrieves a secret from AWS Secrets Manager.
    From there, it will use then use the secret to retrieve an access token from AWS Cognito.
    
    The secret_name is derived from the env parameter. It follows a simple pattern of:
    '{env}/Cognito'
    
    Examples
    --------
    ```python
    from prefect import Flow, Parameter
    from common_tasks.aws_tasks import CognitoLoginTask
    
    login_task = CognitoLoginTask()
    
    with Flow("Some Flow") as flow:
        env = Parameter("env")
        auth_token = login_task(env=env)
    
    """
    
    def __init__(
        self, region_name: Optional[str] = 'us-east-1',
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        env: Optional[Literal['dev', 'prod']] = None,
    ):
        super().__init__(tags=["token"])
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.env = env
    
    @defaults_from_attrs(
        "region_name", "aws_access_key_id", "aws_secret_access_key", "env"
        )
    def run(
        self,
        region_name: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        env: Optional[Literal['dev', 'prod']] = None,
    ):
        
        """
        Retrieves SSM Secret and Initiates Cognito Login.
        
        Parameters can be passed to override the initial parameters.
        
        If en
        
        
        Parameters
        ----------
        region_name
        aws_access_key_id
        aws_secret_access_key
        env

        Returns
        -------
        AccessToken
        """
        
        import boto3
        
        if env is None or env not in {'dev', 'prod'}:
            raise FAIL(f"env is required and must be either 'dev' or 'prod'. Got {env}")
        
        secret_name = f"{env}/Cognito"
        
        session = boto3.session.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        
        client_ssm = session.client(service_name='secretsmanager')
        
        
        try:
            response = client_ssm.get_secret_value(SecretId=secret_name)
            secret: CognitoSecret = json.loads(response['SecretString'])
        except Exception as e:
            raise FAIL(f"Error retrieving secret: {e}") from e
        
        client_cognito = session.client(
            service_name='cognito-idp', region_name=region_name
        )
        try:
            auth = client_cognito.admin_initiate_auth(
                UserPoolId=secret['UserPoolId'],
                ClientId=secret['ClientId'],
                AuthFlow=secret['AuthFlow'],
                AuthParameters={
                    'USERNAME': secret['USERNAME'],
                    'PASSWORD': secret['PASSWORD']
                }
            )
        except Exception as e:
            raise FAIL(f"Error initiating auth: {e}") from e
        
        result = auth.get('AuthenticationResult')
        if result is None:
            raise FAIL(
                "Authentication response did not contain an AuthenticationResult."
                )
        
        token = result.get('AccessToken')
        if token is None:
            raise FAIL("Authentication response did not contain an AccessToken.")
        
        return token
