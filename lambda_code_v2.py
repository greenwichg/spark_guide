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
    
    # CHECK 1: Skip if Step Function ran in last 30 minutes
    try:
        thirty_min_ago = datetime.now(timezone.utc) - timedelta(minutes=30)
        executions = sfn.list_executions(
            stateMachineArn=step_function_arn,
            maxResults=5
        )
        
        for exe in executions.get('executions', []):
            start_time = exe['startDate']
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=timezone.utc)
            
            if start_time > thirty_min_ago:
                print(f"Recent execution: {exe['status']}, skipping...")
                return {'message': 'Recent execution exists, skipping'}
    except Exception as e:
        print(f"Check error: {e}")
    
    # CHECK 2: Skip if Step Function currently running
    try:
        running = sfn.list_executions(
            stateMachineArn=step_function_arn,
            statusFilter='RUNNING',
            maxResults=1
        )
        if running['executions']:
            print("Step Function running, skipping...")
            return {'message': 'Step Function running, skipping'}
    except Exception as e:
        print(f"Running check error: {e}")
    
    # Small delay to let other files land (60 seconds)
    time.sleep(60)
    
    # CHECK 3: Re-verify no Step Function started during wait
    try:
        one_min_ago = datetime.now(timezone.utc) - timedelta(minutes=1)
        executions = sfn.list_executions(
            stateMachineArn=step_function_arn,
            maxResults=3
        )
        
        for exe in executions.get('executions', []):
            start_time = exe['startDate']
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=timezone.utc)
            
            if start_time > one_min_ago:
                print("Step Function started by another Lambda, skipping...")
                return {'message': 'Step Function just started, skipping'}
    except Exception as e:
        print(f"Recheck error: {e}")
    
    # Process files
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
    
    s3_files = set()
    response = s3.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)
    for obj in response.get('Contents', []):
        file_name = obj['Key'].split('/')[-1]
        if file_name and file_name.endswith('.csv'):
            s3_files.add(file_name)
    
    config_files = {job['source_file_path'].split('/')[-1] for job in config_jobs}
    new_files = s3_files - config_files
    
    if new_files:
        sns.publish(
            TopicArn=sns_topic_arn,
            Subject='New Files Found in S3',
            Message=f'New files detected: {list(new_files)}'
        )
        return {'message': 'SNS sent', 'new_files': list(new_files)}
    
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
        sfn.start_execution(
            stateMachineArn=step_function_arn,
            input=json.dumps({'glue_jobs': glue_params})
        )
        return {
            'message': 'Step Function triggered',
            'active_jobs': len(glue_params),
            'skipped_jobs': skipped_jobs
        }
    
    return {'message': 'No action needed', 'skipped_jobs': skipped_jobs}
