import boto3
from dotenv import load_dotenv
import os

# Load credentials from .env file
load_dotenv()

# Why we use environment variables:
# Never hardcode credentials in scripts
# .env keeps them secure and separate from code
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION')
bucket_name = os.getenv('S3_BUCKET_NAME')

# Connect to S3
# boto3 is the official AWS Python library
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=aws_region
)

# Files to upload
# We define source (local path) and destination (S3 path)
files_to_upload = [
    {
        'local': 'data/raw/Medicare_Part_D_Prescribers_by_Provider_and_Drug_2023.csv',
        'S3': 'prescribers/Medicare_Part_D_Prescribers_by_Provider_and_Drug_2023.csv',
        'description': 'CMS Medicare 2023 data'
    },
    {
        'local': 'data/raw/LEIE_fraud_labels_2026.csv',
        'S3': 'reference/LEIE_fraud_labels_2026.csv',
        'description': 'LEIE fraud labels'
    }
]

# Upload each file
for file in files_to_upload:
    print(f"Uploading {file['description']}...")
    print(f"  From: {file['local']}")
    print(f"  To:   s3://{bucket_name}/{file['S3']}")

    # Upload with progress tracking
    file_size = os.path.getsize(file['local'])

    class ProgressTracker:
        def __init__(self, total):
            self.uploaded = 0
            self.total = total

        def __call__(self, bytes_uploaded):
            self.uploaded += bytes_uploaded
            percentage = (self.uploaded / self.total) * 100
            print(f"  Progress: {percentage:.1f}%", end='\r')

    tracker = ProgressTracker(file_size)

    s3.upload_file(
        file['local'],
        bucket_name,
        file['S3'],
        Callback=tracker
    )
    print(f"\n  ✅ {file['description']} uploaded successfully!")

print("\n🎉 All files uploaded to S3!")
print(f"Bucket: s3://{bucket_name}/")
