import json
import boto3
import os
import time
import concurrent.futures
from botocore.config import Config
from boto3.s3.transfer import TransferConfig

# Optimized S3 client with retry settings
s3 = boto3.client(
    "s3",
    config=Config(retries={"max_attempts": 10, "mode": "adaptive"})  # Increases retry attempts
)

# Multi-part copy settings for large files
TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=5 * 1024 * 1024,  # 5MB threshold for multipart
    max_concurrency=10,  # Parallel uploads
)

# Target bucket and path
TARGET_BUCKET = os.getenv("TARGET_BUCKET")
TARGET_PREFIX = os.getenv("TARGET_PREFIX", "processed/")

def move_file(source_bucket, object_key, max_retries=5):
    """Moves a file from source_bucket to TARGET_BUCKET with retry logic."""
    destination_key = f"{TARGET_PREFIX}{object_key.split('/')[-1]}"

    for attempt in range(max_retries):
        try:
            # Copy file with multipart support
            s3.copy(
                {"Bucket": source_bucket, "Key": object_key},
                TARGET_BUCKET,
                destination_key,
                Config=TRANSFER_CONFIG
            )

            # Delete original file after successful copy
            s3.delete_object(Bucket=source_bucket, Key=object_key)

            print(f"✅ Moved: {source_bucket}/{object_key} → {TARGET_BUCKET}/{destination_key}")
            return {"status": "success", "file": object_key}

        except Exception as e:
            print(f"⚠️ Attempt {attempt + 1}/{max_retries} failed for {object_key}: {str(e)}")
            time.sleep(2 ** attempt)  # Exponential backoff

    print(f"❌ Failed to move {object_key} after {max_retries} attempts")
    return {"status": "error", "file": object_key}

def lambda_handler(event, context):
    """Processes multiple SQS messages in parallel, with retries for failed moves."""
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for record in event["Records"]:
                message_body = json.loads(record["body"])
                s3_event = message_body.get("Records", [])[0]
                source_bucket = s3_event["s3"]["bucket"]["name"]
                object_key = s3_event["s3"]["object"]["key"]

                futures.append(executor.submit(move_file, source_bucket, object_key))

            results = [future.result() for future in concurrent.futures.as_completed(futures)]

        return {"statusCode": 200, "body": results}

    except Exception as e:
        print(f"🚨 Lambda error: {str(e)}")
        return {"statusCode": 500, "body": f"Error: {str(e)}"}
