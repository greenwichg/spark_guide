import json
import boto3
import os

s3 = boto3.client('s3')
sns = boto3.client('sns')

def lambda_handler(event, context):
    config_bucket = os.environ['CONFIG_BUCKET']
    config_key = os.environ['CONFIG_KEY']
    sns_topic_arn = os.environ['SNS_TOPIC_ARN']
  
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
  
    # Check if file_name is not in config
    if file_name not in config_files:
        try:
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject='New File Found in S3',
                Message=f'New file detected: {file_name}'
            )
            print(f"SNS notification sent for new file: {file_name}")
            return {'message': 'SNS sent for new file'}
        except Exception as e:
            print(f"SNS publish error: {e}")
            return {'message': 'SNS error'}
    else:
        print(f"File {file_name} already in config; no SNS sent.")
        return {'message': 'File in config, no action'}
