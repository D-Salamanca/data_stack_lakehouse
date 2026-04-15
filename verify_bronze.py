import boto3
from botocore.client import Config

s3 = boto3.client(
    's3',
    endpoint_url='http://proyecto2-minio:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin123',
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

response = s3.list_objects_v2(Bucket='bronze')
print('\n' + '='*60)
print('FILES IN BRONZE BUCKET')
print('='*60)
if 'Contents' in response:
    files = sorted(response['Contents'], key=lambda x: x['Key'])
    for obj in files:
        print(f'  {obj["Key"]}')
    print(f'\nTotal files: {len(files)}')
else:
    print('  (empty)')
print('='*60 + '\n')
