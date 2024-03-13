from pathlib import Path

import boto3
import os

from dotenv import load_dotenv

load_dotenv()


def upload_files(directory, bucket_name, folder):
    """
    Upload files in a directory to a folder in an S3 bucket.

    :param directory: The directory containing the files to upload.
    :param bucket_name: The name of the bucket to upload to.
    :param folder: The folder in the bucket where files should be uploaded.
    """
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

    # Create an S3 client
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    # Walk through the directory
    for root, dirs, files in os.walk(directory):
        for filename in files:
            # Construct the full local path
            local_path = os.path.join(root, filename)

            # Construct the full S3 path
            relative_path = os.path.relpath(local_path, directory)
            s3_path = folder +"/" + relative_path

            print(f"Uploading {local_path} to {s3_path} in bucket {bucket_name}")

            # Perform the upload
            s3.upload_file(local_path, bucket_name, s3_path)

# Replace 'your-directory' with the path to the directory containing your files
# Replace 'your-bucket-name' with your S3 bucket name
# Replace 'your-folder-name' with your desired folder name in the S3 bucket
file_folder = Path(__file__).resolve().parents[0].joinpath("downloaded_resources")
upload_files(str(file_folder), 'cydalics-test-bucket', 'test')
print("Finished")