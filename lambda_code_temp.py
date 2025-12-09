import json
import boto3
import os

s3 = boto3.client('s3')
sns = boto3.client('sns')
sfn = boto3.client('stepfunctions')

def lambda_handler(event, context):
    config_bucket = os.environ['CONFIG_BUCKET']
    config_key = os.environ['CONFIG_KEY']
    sns_topic_arn = os.environ['SNS_TOPIC_ARN']
    step_function_arn = os.environ['STEP_FUNCTION_ARN']

    # Extract bucket and key from S3 event
    if 'Records' in event and len(event['Records']) > 0:
        s3_record = event['Records'][0].get('s3', {})
        bucket = s3_record.get('bucket', {}).get('name')
        key = s3_record.get('object', {}).get('key')
        if not bucket or not key:
            print("Invalid S3 event structure; skipping.")
            return {'message': 'Invalid event'}
       
        # Extract file name from key
        file_name = key.split('/')[-1]
        if not file_name.endswith('.csv'):
            print(f"File {file_name} is not a CSV; skipping.")
            return {'message': 'Non-CSV file'}
       
        print(f"Processing file: {file_name}")
    else:
        print("No valid S3 records in event; skipping.")
        return {'message': 'No S3 records'}

    # Read config.json
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

    # Get config files
    config_files = {job['source_file_path'].split('/')[-1] for job in config_jobs}

    # Fixed test name - ONLY ONE can run
    execution_name = "test-run-001"

    if file_name in config_files:
        # If file is in config, trigger Step Function
        glue_params = []
        for job in config_jobs:
            job_file_name = job['source_file_path'].split('/')[-1]
            if job_file_name == file_name:
                if job.get('is_active', False):
                    glue_params.append({
                        'job_id': job.get('job_id', ''),
                        'job_name': job.get('job_name', ''),
                        'source_file_path': job.get('source_file_path', ''),
                        'target_table': job.get('target_table', ''),
                        'upsert_keys': job.get('upsert_keys', [])
                    })
                break  # Assume one match per file

        if glue_params:
            try:
                sfn.start_execution(
                    stateMachineArn=step_function_arn,
                    name=execution_name,
                    input=json.dumps({'glue_jobs': glue_params})
                )
                print(f"Step Function triggered for file: {file_name}")
                return {
                    'message': 'Step Function triggered',
                    'execution_name': execution_name,
                    'active_jobs': len(glue_params)
                }
            except sfn.exceptions.ExecutionAlreadyExists:
                print(f"Execution {execution_name} already exists, skipping...")
                return {'message': 'Execution already exists, skipping'}
        else:
            print(f"No active job in config for file {file_name}; no action.")
            return {'message': 'No active job, no action'}
    else:
        # If not in config, send SNS
        sns.publish(
            TopicArn=sns_topic_arn,
            Subject='New File Found in S3',
            Message=f'New file detected: {file_name}'
        )
        print(f"SNS notification sent for new file: {file_name}")
        return {'message': 'SNS sent for new file'}
