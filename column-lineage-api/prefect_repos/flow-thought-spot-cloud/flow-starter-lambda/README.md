# Flow Starter Lambda

## Purpose

Triggers Prefect flows to create and load CAM canvases into ElasticSearch for use within the CAM application.

## Functionality

Triggered on upload of a JSON file to the `messaging.${stage}.cisco.com` S3 bucket in the **start-canvas-creation** folder. The expect shape of the JSON in the uploaded file is as follows:

```json
{
  "canvas_id": string,
  "schema": string,
  "date": string,
  "operation_id": string,
  "files": [
    {
      "name": string,
      "loc": string,
      "date": string
    }
  ]
}
```

## Deployment

To deploy an updated version of this Lambda function simply run:

```
serverless deploy -s <stage_name>
```

`stage_name` can be either dev, stage, or prod
