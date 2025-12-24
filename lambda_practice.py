import json
import boto3
import os
import re
from datetime import datetime, timezone
from botocore.exceptions import ClientError

s3 = boto3.client("s3")
sns = boto3.client("sns")
sfn = boto3.client("stepfunctions")

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
STEP_FUNCTION_ARN = os.environ["STEP_FUNCTION_ARN"]


def normalize(text):
    """Lowercase and strip for comparison"""
    return text.strip().lower() if text else ""


def sanitize(text):
    """Clean for Step Functions names"""
    text = text.replace(' ', '-').replace('/', '-').replace('.', '-')
    return re.sub(r'[^a-zA-Z0-9_-]', '', text)[:50]


def get_config(bucket):
    """Find and load first .json file in config/ folder"""
    response = s3.list_objects_v2(Bucket=bucket, Prefix="config/")
    
    for obj in response.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".json") and not key.endswith("/"):
            print(f"Loading config: {key}")
            config_obj = s3.get_object(Bucket=bucket, Key=key)
            return json.loads(config_obj["Body"].read())
    
    raise Exception("No config file found in config/")


def lambda_handler(event, context):
    """Process S3 file uploads"""
    
    # Get file details
    bucket = event["detail"]["bucket"]["name"]
    key = event["detail"]["object"]["key"]
    filename = key.split("/")[-1]
    
    print(f"Processing: {filename}")
    
    # Load config (any .json file in config/)
    config = get_config(bucket)
    
    # Find matching job
    normalized_key = normalize(key)
    job = next((j for j in config if normalize(j.get("source_file_name", "")) == normalized_key), None)
    
    # No match - move to new_files
    if not job:
        dest = f"data/new_files/{filename}"
        s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': key}, Key=dest)
        s3.delete_object(Bucket=bucket, Key=key)
        print(f"Moved to: {dest}")
        
        try:
            sns.publish(TopicArn=SNS_TOPIC_ARN, Subject="New File", Message=f"Moved: {filename} → {dest}")
        except Exception:
            pass
        
        return {"status": "unmapped", "moved_to": dest}
    
    # Inactive - notify
    if not job.get("is_active"):
        try:
            sns.publish(TopicArn=SNS_TOPIC_ARN, Subject="Inactive Job", Message=f"File: {filename}\nJob: {job['job_id']}")
        except Exception:
            pass
        
        return {"status": "inactive", "job_id": job.get("job_id")}
    
    # Active - trigger Step Functions
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
    execution_name = f"run-{sanitize(filename)}-{timestamp}"
    
    try:
        sfn.start_execution(
            stateMachineArn=STEP_FUNCTION_ARN,
            name=execution_name,
            input=json.dumps({
                "glue_jobs": [{
                    "job_id": job["job_id"],
                    "job_name": job["job_name"],
                    "source_file_name": filename,
                    "target_table": job["target_table"],
                    "upsert_keys": job["upsert_keys"]
                }]
            })
        )
        print(f"✅ Triggered: {execution_name}")
        return {"status": "triggered", "execution": execution_name}
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'ExecutionAlreadyExists':
            print(f"Already running: {execution_name}")
            return {"status": "already_running"}
        raise
