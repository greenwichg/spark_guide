import json
import boto3
import os
from datetime import datetime, timezone

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
    
    lock_key = 'locks/lambda.lock'
    
    # Step 1: Try to acquire lock using S3 conditional write
    try:
        # Check if lock exists and is recent (less than 15 minutes old)
        try:
            lock_obj = s3.head_object(Bucket=config_bucket, Key=lock_key)
            lock_age = (datetime.now(timezone.utc) - lock_obj['LastModified']).total_seconds()
            
            if lock_age < 900:  # 15 minutes
                print(f"Lock exists ({lock_age:.0f}s old), skipping...")
                return {'message': 'Another Lambda processing, skipping'}
            else:
                print(f"Lock expired ({lock_age:.0f}s), proceeding...")
        except s3.exceptions.ClientError as e:
            if e.response['Error']['Code'] != '404':
                raise
            print("No lock found, proceeding...")
        
        # Create lock IMMEDIATELY
        s3.put_object(
            Bucket=config_bucket, 
            Key=lock_key, 
            Body=json.dumps({'timestamp': datetime.now(timezone.utc).isoformat()})
        )
        print("Lock created")
        
    except Exception as e:
        print(f"Lock error: {e}")
        return {'message': 'Lock error, skipping'}
    
    try:
        # Step 2: Double-check no Step Function running
        try:
            running = sfn.list_executions(
                stateMachineArn=step_function_arn,
                statusFilter='RUNNING',
                maxResults=1
            )
            if running['executions']:
                print("Step Function already running, releasing lock...")
                s3.delete_object(Bucket=config_bucket, Key=lock_key)
                return {'message': 'Step Function already running, skipping'}
        except Exception as e:
            print(f"Could not check executions: {e}")
        
        # Step 3: Read config.json
        config_data = s3.get_object(Bucket=config_bucket, Key=config_key)
        content = config_data['Body'].read().decode('utf-8')
        
        # Parse config
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
        
        # Step 4: Get S3 CSV files
        s3_files = set()
        response = s3.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)
        for obj in response.get('Contents', []):
            file_name = obj['Key'].split('/')[-1]
            if file_name and file_name.endswith('.csv'):
                s3_files.add(file_name)
        
        # Get config file names
        config_files = {job['source_file_path'].split('/')[-1] for job in config_jobs}
        
        # Check for new files
        new_files = s3_files - config_files
        
        if new_files:
            s3.delete_object(Bucket=config_bucket, Key=lock_key)
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject='New Files Found in S3',
                Message=f'New files detected: {list(new_files)}'
            )
            return {'message': 'SNS sent', 'new_files': list(new_files)}
        
        # Step 5: Process matching files
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
            # Keep lock - expires in 15 minutes
            return {
                'message': 'Step Function triggered',
                'active_jobs': len(glue_params),
                'skipped_jobs': skipped_jobs
            }
        
        s3.delete_object(Bucket=config_bucket, Key=lock_key)
        return {'message': 'No action needed', 'skipped_jobs': skipped_jobs}
        
    except Exception as e:
        # Release lock on error
        try:
            s3.delete_object(Bucket=config_bucket, Key=lock_key)
        except:
            pass
        raise e
