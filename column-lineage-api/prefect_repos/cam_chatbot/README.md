# CAM Chatbot

## Purpose 
Describes the setup and deployment configuration for the CAM Chatbot.  
Additionally, discusses the functionality and operational considerations.
  
## Overview   
The CAM WebEx Chat Bot consists of three core functions.

1. Prefect State Notification Handler  
2. WebEx WebHooks and Action Cards  
3. AWS S3 and Lambda  

When a Prefect flow completes, the state notification handler fires a Python program, that sends the results and an action card via WebEx to the configured user. The user can select "Yes" or "No", regarding the success of the Prefect flow (subject to change). When selected, this triggers a POST request to the configured AWS Lambda with the response payload.  This initial response payload only includes the Message ID, and does not include the actual response. Lambda extracts the messageId from this response, and requests back from WebEx the selected response.  
When this response is received, the full json message response is saved as a .json file into the S3 bucket.
The full end-to-end flow of events appears as:  
  
  Prefect Flow (triggers) -> Notification Handler (sends WebEx message to user) -> WebEx Action Card (triggers) -> Lambda Event -> Requests Message Details from WebEx -> S3

## Prerequisites  
 - Service Account for Terraform  
 - Terraform Installed  
 - CAM Chatbot repository cloned  
  
## Execution  
Requires a configured service account with appropriate IAM roles and permissions across the environment.  
This should be configured as a private credentials file such as `providers.tfvars` or exported up as environment variables via `export AWS_ACCESS_KEY_ID="anaccesskey"`.  
The `terraform` commands should be run from the directory this README exists in - file paths for Lambda are relative to the base path for terraform, and will fail to properly package and deploy if you are in any other directory.

1. `terraform init` - Register the necessary providers to instantiate the infrastructure.  
2. `terraform plan -out chabot.out` - Terraform plan the necessary steps to provision the infrastructure to an out file.
3. `terraform apply "chatbot.out"` - Apply the necessary changes to provision.
  
## Deployed Infrastructure

The Terraform in chatbot.tf deployed the following items into AWS:  
 - An IAM role `chat-callback-dev`, required to invoke Lambda functions
 - An IAM role policy that permits resource access to S3, Secrets Manager, and Cloud Watch.
 - An S3 bucket for storage, `cam-chatbot`.
 - A Lambda function named `chat-callback-dev`.
  
## Registering WebHooks

Once the Lambda has been provisioned, the Lambda Invocation URL is required to configure the WebHook on WebEx.  
This has already been configured initially, but if the Lambda is removed and re-provisioned, the WebEx Webhook will need to be updated.
To create the WebHook initially, is a simple POST operation with the following payload, where the targetURL is the Lambda invocation URL:

```
    url = 'https://webexapis.com/v1/webhooks'
    m = {
        "name": "My Data Canvas Action Webhook",
        "targetUrl": "https://random_lambda.execute-api.us-east-1.amazonaws.com/api/",
        "resource": "attachmentActions",
        "event": "created"
    }
```

If the Lambda is modified, and a new URL is provisioned in any way, the existing WebHook url can be updated via the following PUT.  
Note that an existing webHookId is required, which is returned from the initial creation.

```
    url = f'https://webexapis.com/v1/webhooks/{webHookId}'
    m = {
        "name": "My Data Canvas Action Webhook",
        "targetUrl": targetUrl,
    }
```



