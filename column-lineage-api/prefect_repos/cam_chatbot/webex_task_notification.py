import prefect
import requests
import json
from prefect.client import Secret
from prefect import task, Flow


#TODO Pass in State Message into Adaptive Card
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


def post_request(url, msg):
    """
    Formats the incoming url, and the submitted message.
    Posts the message to the appropriate endpoint, and returns a json serialized response.
    """

    SEC = Secret('WEBEX_CHAT_KEY').get()

    headers = {'Authorization': f'Bearer {SEC}',
           'Content-Type': 'application/json'}

    r = requests.post(
        url,
        data=msg,
        headers=headers)

    response = json.dumps(r.json(), indent=4)

    return response


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


def my_state_handler(obj, old_state, new_state):
    
    msg = "\nCalling my custom state handler on {0}:\n{1} to {2}\n"
    print(msg.format(obj, old_state, new_state))
    post_msg("Hello", "chriboy2@cisco.com")

    return new_state

@task
def plus_one(x):
    return x + 1

with Flow(name="state-handler-demo",
               state_handlers=[my_state_handler]) as flow:
    y = plus_one(2)
    print (type(y))
    

if __name__ == "__main__":
    flow.run()
