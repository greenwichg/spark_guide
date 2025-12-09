import json
import boto3
import os
import time
from datetime import datetime, timezone, timedelta

s3 = boto3.client('s3')
sns = boto3.client('sns')
sfn = boto3.client('stepfunctions')

def lambda_handler(event, context):
    config_bucket = os.environ['CONFIG_BUCKET']
    config_key = os.environ['CONFIG_KEY']
    source_bucket = os.environ['SOURCE_BUCKET']
    source_prefix = os.environ['SOURCE_PREFIX']
    sns_topic_arn = os.environ['SNS_TOPIC_ARN']
    step_function_arn = os.environ['STEP_FUNCTION_ARN']
  
    # Fixed test name - ONLY ONE can run
    execution_name = "test-run-001"
  
    # Note the approximate start time for later filtering
    start_time = datetime.now(timezone.utc) - timedelta(seconds=10)  # Buffer for invocation lag
  
    # Check if execution already exists
    try:
        exec_arn = f"{step_function_arn.replace(':stateMachine:', ':execution:')}:{execution_name}"
        sfn.describe_execution(executionArn=exec_arn)
        print(f"Execution {execution_name} already exists, skipping...")
        return {'message': 'Execution already exists, skipping'}
    except sfn.exceptions.ExecutionDoesNotExist:
        pass
    except Exception as e:
        print(f"Describe error: {e}")
  
    # Wait 90 seconds to batch potential concurrent triggers
    time.sleep(90)
  
    # Double-check after waiting
    try:
        exec_arn = f"{step_function_arn.replace(':stateMachine:', ':execution:')}:{execution_name}"
        sfn.describe_execution(executionArn=exec_arn)
        print(f"Execution {execution_name} started by another Lambda, skipping...")
        return {'message': 'Execution already exists, skipping'}
    except sfn.exceptions.ExecutionDoesNotExist:
        pass
    except Exception as e:
        print(f"Describe error: {e}")
  
    # Read config
    config_data = s3.get_object(Bucket=config_bucket, Key=config_key)
    content = config_data['Body'].read().decode('utf-8')
  
    config_jobs = []
    content = content.strip()
    if content.startswith('['):
        config_jobs = json.loads(content)
    else:
        decoder = json.JSONDecoder()
        while content:
            obj, idx = decoder.raw_decode(content)
            config_jobs.append(obj)
            content = content[idx:].strip()
  
    # Get S3 files
    s3_files = set()
    response = s3.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)
    for obj in response.get('Contents', []):
        file_name = obj['Key'].split('/')[-1]
        if file_name and file_name.endswith('.csv'):
            s3_files.add(file_name)
  
    if not s3_files:
        print("No CSV files found, exiting...")
        return {'message': 'No files found'}
  
    config_files = {job['source_file_path'].split('/')[-1] for job in config_jobs}
    new_files = s3_files - config_files
  
    print(f"Detected new_files: {list(new_files)}")  # Debug
  
    # Marker-based claiming (still useful for per-file dedup if needed)
    tracking_prefix = 'tracked/'
    claimed_new_files = []  # Optional: Track local claims if needed
  
    for new_file in new_files:
        marker_key = f"{tracking_prefix}{new_file}"
        try:
            s3.put_object(
                Bucket=config_bucket,
                Key=marker_key,
                Body='',
                IfNoneMatch='*'
            )
            claimed_new_files.append(new_file)
            print(f"Successfully claimed marker for: {new_file}")
        except s3.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'PreconditionFailed':
                print(f"Marker for {new_file} already exists, skipping claim")
            else:
                print(f"S3 put_object error for {new_file}: {e}")
                raise
  
    glue_params = []
    skipped_jobs = []
  
    for job in config_jobs:
        file_name = job['source_file_path'].split('/')[-1]
        if file_name in s3_files:
            if job.get('is_active', False):
                glue_params.append({
                    'job_id': job.get('job_id', ''),
                    'job_name': job.get('job_name', ''),
                    'source_file_path': job.get('source_file_path', ''),
                    'target_table': job.get('target_table', ''),
                    'upsert_keys': job.get('upsert_keys', [])
                })
            else:
                skipped_jobs.append(job.get('job_name', ''))
  
    if glue_params:
        try:
            sfn.start_execution(
                stateMachineArn=step_function_arn,
                name=execution_name,
                input=json.dumps({'glue_jobs': glue_params})
            )
            
            # Successful SFN start: Now list recent tracked files and send single SNS
            time_window_minutes = 5  # Adjust based on your batch window (e.g., >90s sleep)
            recent_cutoff = datetime.now(timezone.utc) - timedelta(minutes=time_window_minutes)
            
            # List all markers in tracked/
            tracked_response = s3.list_objects_v2(Bucket=config_bucket, Prefix=tracking_prefix)
            tracked_files = []
            keys_to_delete = []  # For optional cleanup
            
            for obj in tracked_response.get('Contents', []):
                key = obj['Key']
                last_modified = obj['LastModified']
                
                # Filter recent ones
                if last_modified >= recent_cutoff:
                    file_name = key[len(tracking_prefix):]  # Strip prefix to get original file name
                    if file_name:  # Avoid empty keys
                        tracked_files.append(file_name)
                    keys_to_delete.append(key)  # Collect for deletion if desired
            
            tracked_files = sorted(set(tracked_files))  # Dedup and sort for clean output
            
            print(f"Recent tracked files: {tracked_files}")  # Debug
            
            if tracked_files:
                try:
                    sns.publish(
                        TopicArn=sns_topic_arn,
                        Subject='New Files Found in S3',
                        Message=f'New files detected: {tracked_files}'
                    )
                    print(f"SNS notification sent for: {tracked_files}")
                except Exception as e:
                    print(f"SNS publish error: {e}")
            
            # Optional: Delete the recent markers after sending (uncomment if needed)
            # for key in keys_to_delete:
            #     s3.delete_object(Bucket=config_bucket, Key=key)
            #     print(f"Deleted marker: {key}")
            
            return {
                'message': 'Step Function triggered',
                'execution_name': execution_name,
                'active_jobs': len(glue_params),
                'new_files': list(new_files) if new_files else None
            }
        except sfn.exceptions.ExecutionAlreadyExists:
            print(f"Execution {execution_name} already exists, skipping...")
            return {'message': 'Execution already exists, skipping'}
  
    return {'message': 'No matching files', 'new_files': list(new_files) if new_files else None}
