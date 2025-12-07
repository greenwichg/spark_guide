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
    
    # Generate execution name based on current hour (only 1 per hour allowed)
    now = datetime.now(timezone.utc)
    execution_name = f"exec-{now.strftime('%Y-%m-%d-%H')}"
    
    # Wait 60 seconds to let all files upload
    time.sleep(60)
    
    # Try to start Step Function with unique name
    # If another Lambda already started it, this will fail
    
    # First, read config and get files
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
        try:
            sfn.start_execution(
                stateMachineArn=step_function_arn,
                name=execution_name,  # Unique name per hour
                input=json.dumps({'glue_jobs': glue_params})
            )
            return {
                'message': 'Step Function triggered',
                'execution_name': execution_name,
                'active_jobs': len(glue_params),
                'skipped_jobs': skipped_jobs
            }
        except sfn.exceptions.ExecutionAlreadyExists:
            print(f"Execution {execution_name} already exists, skipping...")
            return {'message': 'Execution already exists, skipping'}
    
    return {'message': 'No action needed', 'skipped_jobs': skipped_jobs}
