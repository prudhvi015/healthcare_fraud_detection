import boto3
from dotenv import load_dotenv
import os

# Load credentials from .env file
load_dotenv()

# Connect to S3
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

# List buckets
response = s3.list_buckets()
print("✅ AWS Connection Successful!")
print("\nYour S3 Buckets:")
for bucket in response['Buckets']:
    print(f"  - {bucket['Name']}")

# List folders in your bucket
bucket_name = os.getenv('S3_BUCKET_NAME')
response = s3.list_objects_v2(Bucket=bucket_name, Delimiter='/')
print(f"\nFolders in {bucket_name}:")
for prefix in response.get('CommonPrefixes', []):
    print(f"  - {prefix['Prefix']}")
