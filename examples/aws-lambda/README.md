# AWS Lambda + ignis example

Automatically analyze Spark event logs with ignis the moment they land in S3,
and post findings to Slack.

## How it works

```
Spark job finishes
      │
      ▼
Event log written to S3
      │
      ▼ S3 ObjectCreated notification
AWS Lambda (handler.py)
      │
      ├── parse_event_log(s3://bucket/key)
      ├── run all ignis rules
      └── findings? → ignis notify slack
```

The Lambda is triggered by S3 event notifications on `ObjectCreated`. It parses
the new log, runs all six ignis rules, and posts a Slack message if any findings
exceed the configured minimum severity.

## Deployment

### 1. Package the function

```bash
pip install -r requirements.txt -t package/
cp handler.py package/
cd package && zip -r ../ignis-lambda.zip . && cd ..
```

### 2. Create the Lambda

```bash
aws lambda create-function \
  --function-name ignis-spark-analyzer \
  --runtime python3.12 \
  --role arn:aws:iam::YOUR_ACCOUNT:role/ignis-lambda-role \
  --handler handler.handler \
  --zip-file fileb://ignis-lambda.zip \
  --timeout 120 \
  --memory-size 256 \
  --environment "Variables={IGNIS_SLACK_WEBHOOK=https://hooks.slack.com/services/...}"
```

### 3. Grant the Lambda read access to your S3 bucket

Attach an IAM policy to the Lambda execution role:

```json
{
  "Effect": "Allow",
  "Action": ["s3:GetObject"],
  "Resource": "arn:aws:s3:::my-spark-logs/*"
}
```

### 4. Add an S3 event notification

```bash
aws s3api put-bucket-notification-configuration \
  --bucket my-spark-logs \
  --notification-configuration '{
    "LambdaFunctionConfigurations": [{
      "LambdaFunctionArn": "arn:aws:lambda:us-east-1:YOUR_ACCOUNT:function:ignis-spark-analyzer",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {
        "Key": {
          "FilterRules": [{"Name": "prefix", "Value": "events/"}]
        }
      }
    }]
  }'
```

Also grant S3 permission to invoke the Lambda:

```bash
aws lambda add-permission \
  --function-name ignis-spark-analyzer \
  --statement-id s3-invoke \
  --action lambda:InvokeFunction \
  --principal s3.amazonaws.com \
  --source-arn arn:aws:s3:::my-spark-logs
```

## Environment variables

| Variable | Required | Description |
|---|---|---|
| `IGNIS_SLACK_WEBHOOK` | No | Slack incoming webhook URL. If not set, findings are logged but not sent. |
| `IGNIS_MIN_SEVERITY` | No | Minimum severity to notify on: `WARNING` (default) or `INFO`. |

## IAM role

The Lambda execution role needs:
- `s3:GetObject` on your event log bucket
- `AWSLambdaBasicExecutionRole` (for CloudWatch Logs)

## Configuring Spark to write event logs to S3

Add to your `spark-defaults.conf` or pass as `--conf` flags:

```
spark.eventLog.enabled  true
spark.eventLog.dir      s3://my-spark-logs/events/
```

Databricks writes event logs automatically — point the S3 notification at your
Databricks log path prefix.
