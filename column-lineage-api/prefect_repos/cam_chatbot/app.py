import json
import boto3
import requests
from botocore.exceptions import ClientError

print('Loading function')


def get_secret():
    secret_name = "WebExChatbot"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = json.loads(response['SecretString'])

    return secret['chatbot']

secret = get_secret()
headers = {'Authorization': f'Bearer {secret}',
           'Content-Type': 'application/json'}


def write_to_s3(message, data_id):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('cam-chatbot')
    print(f"Writing to S3: {message}")
    with open('/tmp/' + data_id + '.json', 'w') as data:
        data.write(message)
    bucket.upload_file('/tmp/' + data_id + '.json', data_id)


def get_attachment(data_id):
    url = f"https://webexapis.com/v1/attachment/actions/{data_id}"
    response = requests.get(url, headers=headers)
    r = json.dumps(response.json(), indent=4)
    return r

def get_room_messages(roomId):
    url = f"https://webexapis.com/v1/messages?roomId={roomId}&max=1"
    response = requests.get(url, headers=headers)
    r = json.dumps(response.json(), indent=4)
    return r

def lambda_handler(event, context):
    body = json.loads(event['body'])
    print(json.dumps(body, indent=4))
    roomId = body['data']['roomId']
    data_id = body['data']['id']

    message = get_attachment(data_id)
    room_replies = get_room_messages(roomId)

    print (message)
    print (room_replies)
    write_to_s3(message, data_id)
    write_to_s3(room_replies, roomId)

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(body, indent=4)
    }