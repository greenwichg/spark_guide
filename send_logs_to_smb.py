import boto3
import paramiko
import json
from datetime import datetime
import os

def lambda_handler(event, context):
    """
    Triggered by S3 event when log file is created
    Transfers log file to SFTP with automatic retry via Lambda
    """
    
    S3_BUCKET = 'nyl-invqai-dev-anaplan'
    SECRET_NAME = 'prod/sftp/log-transfer'
    
    s3_client = boto3.client('s3')
    secrets_client = boto3.client('secretsmanager')
    
    try:
        # Parse S3 event
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        log_file_key = record['s3']['object']['key']
        
        filename = log_file_key.split('/')[-1]
        
        print(f"Processing: {log_file_key}")
        
        # Skip non-log files
        if not log_file_key.startswith('logs/') or not filename.endswith('.txt'):
            print(f"Skipping non-log file: {log_file_key}")
            return {'statusCode': 200, 'body': 'Skipped'}
        
        # Download from S3
        log_obj = s3_client.get_object(Bucket=bucket, Key=log_file_key)
        log_content = log_obj['Body'].read()
        
        print(f"Downloaded {filename}: {len(log_content)} bytes")
        
        # Get SFTP credentials
        secret = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        creds = json.loads(secret['SecretString'])
        
        # Check if credentials are still placeholder
        if creds['sftp_host'] == 'PENDING_FROM_SURESH':
            print("⚠ SFTP credentials not yet provided by business team")
            print("Skipping transfer - will retry when credentials are available")
            # Return success to avoid retries until credentials are ready
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Credentials pending - skipped transfer',
                    'file': filename
                })
            }
        
        # Connect to SFTP
        print(f"Connecting to SFTP: {creds['sftp_host']}")
        
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        ssh.connect(
            hostname=creds['sftp_host'],
            port=int(creds.get('sftp_port', 22)),
            username=creds['username'],
            password=creds['password'],
            timeout=30,
            banner_timeout=30
        )
        
        sftp = ssh.open_sftp()
        
        # Create date-based directory structure
        path_parts = log_file_key.split('/')
        if len(path_parts) >= 4:
            year, month, day = path_parts[1], path_parts[2], path_parts[3]
            date_folder = f"{year}{month}{day}"
            remote_dir = f"{creds['remote_dir']}/{date_folder}"
            
            # Create directory if needed
            try:
                sftp.stat(remote_dir)
            except IOError:
                try:
                    sftp.mkdir(remote_dir)
                    print(f"Created directory: {remote_dir}")
                except IOError:
                    # Concurrent Lambda might have created it
                    pass
        else:
            remote_dir = creds['remote_dir']
        
        # Upload to SFTP
        remote_path = f"{remote_dir}/{filename}"
        
        with sftp.file(remote_path, 'wb') as remote_file:
            remote_file.write(log_content)
        
        sftp.close()
        ssh.close()
        
        print(f"✓ Transferred: {filename} to {remote_path}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Success',
                'file': filename,
                'size': len(log_content),
                'remote_path': remote_path
            })
        }
        
    except secrets_client.exceptions.ResourceNotFoundException:
        # Secret doesn't exist yet - skip for now
        print("⚠ Secret not created yet - skipping transfer")
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Secret not found - skipped'})
        }
        
    except Exception as e:
        # Log the error
        print(f"✗ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        
        # Re-raise to trigger Lambda's automatic retry
        # Lambda will retry 2 times automatically
        raise
