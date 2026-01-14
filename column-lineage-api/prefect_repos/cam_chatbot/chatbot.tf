terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
    }
  }
}


resource "aws_iam_role" "chat_callback" {
  path                 = "/"
  name                 = "chat-callback-dev"
  max_session_duration = 3600
  assume_role_policy   = <<EOF
{
        "Version":"2012-10-17",
        "Statement":[
            {
                "Sid":"",
                "Effect":"Allow",
                "Principal": {
                    "Service": "lambda.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
}
EOF
}


resource "aws_iam_role_policy" "chat_callback" {
  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "logs:CreateLogStream",
                "secretsmanager:GetSecretValue",
                "logs:CreateLogGroup",
                "logs:PutLogEvents"
            ],
            "Resource": [
                "arn:aws:secretsmanager:us-east-1:837578041534:secret:WebExChatbot-ZkpwdR",
                "arn:*:logs:*:*:*",
                "arn:aws:s3:::cam-chatbot/*"
            ]
        }
    ]
}
EOF
  role   = aws_iam_role.chat_callback.name
}

resource "aws_s3_bucket" "S3Bucket" {
  bucket = "cam-chatbot"
}


data "archive_file" "init" {
  type        = "zip"
  source_file = "./app.py"
  output_path = "./app.zip"
}

resource "aws_lambda_function" "cam_chatbot" {
  description = ""
  function_name = aws_iam_role.chat_callback.name
  handler       = "app.app"
  architectures = [
    "x86_64"
  ]

  filename    = data.archive_file.init.output_path
  memory_size = 128
  role        = "${aws_iam_role.chat_callback.arn}"
  runtime     = "python3.8"
  timeout     = 60
  tracing_config {
    mode = "PassThrough"
  }
}

resource "aws_lambda_permission" "chat_callback" {
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.cam_chatbot.arn
  principal     = "apigateway.amazonaws.com"
  source_arn    = "arn:aws:execute-api:us-east-1:837578041534:2caiwm3tfc/*"
}



