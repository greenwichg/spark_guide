import json
import boto3
import os
import re
from datetime import datetime, timezone
from botocore.exceptions import ClientError

s3 = boto3.client("s3")
sns = boto3.client("sns")
sfn = boto3.client("stepfunctions")


def normalize(name):
    """Convert filename to lowercase for comparison"""
    if not name:
        return ""
    return name.strip().lower()


def sanitize_execution_name(name):
    """Clean filename for use in Step Functions execution name"""
    sanitized = name.replace(' ', '-').replace('.', '-').replace('/', '-')
    sanitized = re.sub(r'[^a-zA-Z0-9_-]', '', sanitized)
    return sanitized[:50]


def build_glue_job(job, file_name=None):
    """Prepare job configuration for Step Functions"""
    source_file = file_name or job.get("source_file_name")
    
    return {
        "job_id": job["job_id"],
        "job_name": job["job_name"],
        "source_file_name": source_file,
        "target_table": job["target_table"],
        "upsert_keys": job["upsert_keys"]
    }


def move_to_new_file(bucket, source_key):
    """Move unmapped/new file from data/in/ to data/new_files/"""
    try:
        file_name = source_key.split("/")[-1]
        destination_key = f"data/new_files/{file_name}"
        
        print(f"Moving unmapped file from {source_key} to {destination_key}")
        
        s3.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': source_key},
            Key=destination_key
        )
        
        s3.delete_object(Bucket=bucket, Key=source_key)
        
        print(f"Successfully moved unmapped file to {destination_key}")
        return destination_key
        
    except ClientError as e:
        print(f"Failed to move file to data/new_files: {e}")
        return None


def load_config_file(bucket, config_path="config/config.json"):
    """Load single config JSON file from S3"""
    try:
        print(f"Loading config file: {config_path}")
        
        config_obj = s3.get_object(Bucket=bucket, Key=config_path)
        config_data = json.loads(config_obj["Body"].read().decode("utf-8"))
        
        print(f"Loaded {len(config_data)} jobs from {config_path}")
        return config_data
        
    except ClientError as e:
        print(f"Failed to load config file: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"Invalid JSON in config file: {e}")
        return []


def send_notification(topic_arn, subject, message):
    """Send email notification via SNS"""
    try:
        sns.publish(TopicArn=topic_arn, Subject=subject, Message=message)
        print(f"Notification sent: {subject}")
        return True
    except ClientError as e:
        print(f"Failed to send notification: {e}")
        return False


def lambda_handler(event, context):
    """Process S3 file upload events"""
    
    print("Lambda triggered")
    print(json.dumps(event))

    if event.get("source") != "aws.s3":
        print("Ignoring non-S3 event")
        return {"message": "Not an S3 event"}

    try:
        detail = event.get("detail", {})
        bucket = detail["bucket"]["name"]
        key = detail["object"]["key"]
    except KeyError as e:
        print(f"Invalid event structure: {e}")
        return {"message": "Invalid event"}

    print(f"Processing: {key} from {bucket}")

    if not key.startswith("data/in/") or not key.endswith(".csv"):
        print("File not in data/in/ or not CSV")
        return {"message": "File ignored"}

    try:
        SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
        STEP_FUNCTION_ARN = os.environ["STEP_FUNCTION_ARN"]
    except KeyError as e:
        print(f"Missing environment variable: {e}")
        raise

    # Load config file
    config_data = load_config_file(bucket)

    # Normalize the uploaded file key for comparison
    normalized_key = normalize(key)
    print(f"Normalized file key: {normalized_key}")

    # Find matching job using normalized comparison
    matching_job = None
    for item in config_data:
        source_file_name = item.get("source_file_name")
        
        if not source_file_name:
            print(f"Job {item.get('job_id', 'unknown')} missing source_file_name")
            continue
        
        # Normalize config path for comparison
        normalized_config = normalize(source_file_name)
        
        print(f"Comparing: '{normalized_key}' == '{normalized_config}' (job: {item.get('job_id')})")
        
        # Case-insensitive comparison
        if normalized_config == normalized_key:
            matching_job = item
            print(f"✅ Match found! Job ID: {item.get('job_id', 'unknown')}")
            break

    # Handle unmapped file
    if not matching_job:
        print("No matching job found - moving to data/new_files/")
        
        new_location = move_to_new_file(bucket, key)
        
        send_notification(
            SNS_TOPIC_ARN,
            "New File Uploaded - Moved to data/new_files/",
            f"A new file (not in config) was uploaded.\n"
            f"File has been moved to data/new_files/ folder.\n\n"
            f"Original Location: {key}\n"
            f"New Location: {new_location}\n"
            f"Bucket: {bucket}\n\n"
            f"Action needed:\n"
            f"1. Review the file in data/new_files/ folder\n"
            f"2. Add configuration to config.json\n"
            f"3. Move file to data/in/ to process"
        )
        
        return {
            "message": "New file - moved to data/new_files/",
            "original_location": key,
            "new_location": new_location,
            "reason": "unmapped"
        }

    # Handle inactive job
    if not matching_job.get("is_active", False):
        print("Job is inactive")
        
        send_notification(
            SNS_TOPIC_ARN,
            "File Upload Skipped - Inactive Job",
            f"File uploaded but job is currently inactive.\n"
            f"File remains in data/in/ folder.\n\n"
            f"Location: {key}\n"
            f"Job ID: {matching_job.get('job_id', 'unknown')}\n"
            f"Job Name: {matching_job.get('job_name', 'unknown')}\n\n"
            f"Action needed:\n"
            f"- Set is_active to true in the config file"
        )
        
        return {
            "message": "Job inactive - file remains in data/in/",
            "location": key,
            "job_id": matching_job.get("job_id", "unknown")
        }

    # Trigger Step Functions for active job
    print("Job is active - triggering Step Functions")
    
    file_name = key.split("/")[-1]
    safe_name = sanitize_execution_name(key)
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')
    execution_name = f"run-{safe_name}-{timestamp}"

    print(f"Starting execution: {execution_name}")

    try:
        sfn.start_execution(
            stateMachineArn=STEP_FUNCTION_ARN,
            name=execution_name,
            input=json.dumps({
                "glue_jobs": [build_glue_job(matching_job, file_name)]
            })
        )
        
        print("Step Function started successfully")
        
        return {
            "message": "Job triggered",
            "file": file_name,
            "job_id": matching_job.get("job_id", "unknown"),
            "execution": execution_name
        }
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'ExecutionAlreadyExists':
            print("Execution already exists")
            return {"message": "Already running"}
        else:
            print(f"Failed to start Step Function: {e}")
            raise
