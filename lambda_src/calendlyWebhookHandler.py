import json
import boto3
import os
import hashlib
import datetime
import traceback

s3 = boto3.client("s3")
sns = boto3.client("sns")

BUCKET = os.environ.get("BUCKET_NAME", "calendly-marketing-pipeline-data")
TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")

def lambda_handler(event, context):
    try:
        # Log incoming event
        print("Received event:", json.dumps(event))

        # Extract body (API Gateway sends JSON string under 'body')
        body_raw = event.get("body", "{}")
        body_json = json.loads(body_raw) if isinstance(body_raw, str) else body_raw

        # Timestamp and unique hash key for idempotency
        ts = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
        date_partition = ts.split("T")[0]
        hash_key = hashlib.md5(json.dumps(body_json).encode()).hexdigest()

        s3_key = f"bronze/calendly/webhooks/dt={date_partition}/{hash_key}.json"

        s3.put_object(
            Bucket=BUCKET,
            Key=s3_key,
            Body=json.dumps(body_json, indent=2),
            ContentType="application/json",
            ServerSideEncryption="AES256"
        )

        print(f"✅ Stored webhook event in S3: {s3_key}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Webhook stored successfully",
                "s3_key": s3_key
            })
        }

    except Exception as e:
        error_trace = traceback.format_exc()
        print(f"❌ Error: {e}\n{error_trace}")
        if TOPIC_ARN:
            sns.publish(
                TopicArn=TOPIC_ARN,
                Subject="Calendly Webhook Handler Failure",
                Message=f"Error: {str(e)}\n\nTraceback:\n{error_trace}"
            )
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
