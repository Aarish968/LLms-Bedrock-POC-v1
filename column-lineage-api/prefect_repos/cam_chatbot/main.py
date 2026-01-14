import requests
import json
import os


SEC = os.environ.get('SEC')
ATTACH_ACTIONS_WEBHOOKID = "Y2lzY29zcGFyazovL3VzL1dFQkhPT0svZjhlNzUyZjMtMjY1MC00ZTk1LWExYjMtZTE2ZWJkZWRmN2Zj"
MESSAGE_CREATION_WEBHOOKID = "Y2lzY29zcGFyazovL3VzL1dFQkhPT0svNDZhODlkMTctM2UyZC00NTc1LWE0ZDItMGJiZmMwY2JmYWRk"

headers = {'Authorization': f'Bearer {SEC}',
           'Content-Type': 'application/json'}


def get_request(url):
    """
    Formats the incoming url, attaching headers, and formatting the json response.
    Returns a json serialized response.
    """
    r = requests.get(
        url,
        headers=headers)
    response = json.dumps(r.json(), indent=4)
    print(f"{response}")

    return response


def post_request(url, msg):
    """
    Formats the incoming url, and the submitted message.
    Posts the message to the appropriate endpoint, and returns a json serialized response.
    """
    print (url)
    print (msg)

    r = requests.post(
        url,
        data=msg,
        headers=headers)

    response = json.dumps(r.json(), indent=4)
    print(f"{response}")

    return response


def put_request(url, msg):
    """
    Function for a put requests to json serialize the return and attaches headers.
    This is specific to updating webhooks that already exist.
    """
    r = requests.put(
        url,
        data=msg,
        headers=headers)

    response = json.dumps(r.json(), indent=4)
    print(f"{response}")

    return response


def create_webhook():
    """
    This creates a WebEx Webbhook at a static API endpoint.
    Requires:
    -------
    name: An abritratry WebHook Name
    targetUrl: The target location the WebHook should submit a post request to
    resource: What object should the webhook fire on
    event: Under what listener conditions should the webhook fire on at the resource
    """
    url = 'https://webexapis.com/v1/webhooks'
    m = {
        "name": "My Data Canvas Action Webhook",
        "targetUrl": "https://2caiwm3tfc.execute-api.us-east-1.amazonaws.com/api/",
        "resource": "attachmentActions",
        "event": "created"
    }

    msg = json.dumps(m)
    return post_request(url, msg)


def update_webhook(targetUrl, webHookId=ATTACH_ACTIONS_WEBHOOKID):
    """
    Updates the targetUrl for an existing WebHook.
    Requires:
    ---------
    ATTACH_ACTIONS_WEBHOOKID is a global for the already existing webhook; there is a MESSAGE and ATTACHMENT webhook
    name: The updated name you wish to assign the webhook
    targetUrl: The new webhook post location if the lambda changes
    """
    url = f'https://webexapis.com/v1/webhooks/{webHookId}'
    m = {
        "name": "My Data Canvas Action Webhook",
        "targetUrl": targetUrl,
    }

    body = json.dumps(m)
    return put_request(url, body)


def check_webhook_details(webHookId):
    """
    Takes the passed webHookId and simply returns a json response for the configuration.
    """
    url = f'https://webexapis.com/v1/webhooks/{webHookId}'
    return get_request(url)

def adaptive_card_working():
    """
    Creates a static Adaptive Card Definition to embed in the message payload.
    https://developer.webex.com/docs/api/guides/cards
    Currently uses Adaptive Card Spec v1.2, which changes the template and schema.
    """
    m = [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "type": "AdaptiveCard",
                "version": "1.2",
                "body": [
                    {
                        "type": "ColumnSet",
                        "columns": [
                            {
                                "type": "Column",
                                "width": "auto",
                                "items": [
                                    {
                                        "type": "TextBlock",
                                        "text": "[Do You Want to Build a Snowman?]()",
                                        "horizontalAlignment": "Left",
                                        "size": "Medium"
                                    }
                                ],
                                "verticalContentAlignment": "Center",
                                "horizontalAlignment": "Left",
                                "spacing": "Small"
                            }
                        ]
                    },
                    {
                        "type": "ActionSet",
                        "horizontalAlignment": "Center",
                        "spacing": "None",
                        "actions": [
                            {
                                "type": "Action.Submit",
                                "title": "Yes",
                                "data": {
                                    "response": "yes"
                                }
                            },
                            {
                                "type": "Action.Submit",
                                "title": "No",
                                "data": {
                                    "response": "no"
                                }
                            }
                        ]
                    }
                ]
            }
    }]

    return m

def post_msg (message, user):
    """
    Takes a message to send, and a user.
    By default, if attachments is included with an adaptive card, the message
    will be overwritten and not included. 

    Requires:
    ---------
    user: An e-mail address with an active webex account
    text: A string to send to the user as a message
    attachments: If enabled, sends a responsive adaptive card to the user.
    """

    m = {
        "toPersonEmail": user,
        "text": message,
        "attachments": adaptive_card_working()
    }

    msg = json.dumps(m)
    print (msg)
    r = post_request('https://webexapis.com/v1/messages', msg)


def check_attachment(attachment_id):
    """
    Retrieves the encrypted responses for a given message and attachment.
    When a response is submitted, or an adaptive card selected, the webex
    platform encrypts the response by default.

    Decryption then requires a separate authenticated request, and a
    supplied attachment_id from the original message to retrieve.

    Requires:
    --------
    attachment_id: This is dynamically assigned by post_msg.

    Returns a json string of the actual user response in the message.
    """
    url = f'https://webexapis.com/v1/attachment/actions/{attachment_id}'
    print(f"Trying {url}")
    response = requests.get(url, headers=headers)
    print(json.dumps(response.json(), indent=4))
    return response

if __name__ == "__main__":

    #Adaptive card attachments take precedence over string messages.
    post_msg("Hello", "alanzen@cisco.com")

    # Default messages don't always have attachment_id's.
    # If it exists check it, otherwise done - for manual debugging
    attachment_id = ''
    if attachment_id:
        check_attachment(attachment_id)

    ##Webhook only needs to be created ONCE for the application
    #create_webhook()

    ##Updating the webhook, requires a minimum the url + name)
    #update_webhook('https://4ontt6zcccs2sbwg6f4dodcooa0qfvxs.lambda-url.us-east-1.on.aws/')

    ##Checking for webhook details requires the ID of the existing webhook. Listing all webhooks does not allow filters.
    #check_webhook_details(ATTACH_ACTIONS_WEBHOOKID)
