import base64
import hashlib
from pathlib import Path

from boto3 import client
from botocore.exceptions import BotoCoreError, ClientError

class ObjectStorageException(Exception):
    '''
    Object Storage exceptions
    '''

def upload_file(bucket_name: str, file_path: Path, object_name: str = None) -> bool:
    '''
    Upload a file to s3 using boto
    '''
    file_path = Path(file_path) # Double check its a path
    s3_client = client('s3')
    object_name = object_name or str(file_path)
    if not file_path.exists() or not file_path.is_file():
        raise ObjectStorageException(f'Invalid file path {str(file_path)}')

    data = file_path.read_bytes()
    md5_digest = hashlib.md5(data).digest()
    md5_base64 = base64.b64encode(md5_digest).decode('utf-8')
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_name,
            Body=data,
            ContentMD5=md5_base64,
        )
        return True
    except (BotoCoreError, ClientError) as e:
        raise ObjectStorageException('Error uploading file') from e

def get_file(bucket_name: str, object_name: str, file_path: Path) -> bool:
    '''
    Download client to path
    '''
    file_path = Path(file_path) # Double check its a path
    s3_client = client('s3')
    try:
        # Download the object
        response = s3_client.get_object(
            Bucket=bucket_name,
            Key=object_name,
        )
        # Read the body stream
        data = response['Body'].read()
    except (BotoCoreError, ClientError) as e:
        raise ObjectStorageException('Error downloading file') from e

    # Write to destination file
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(data)

    return True

def delete_file(bucket_name: str, object_name: str) -> bool:
    '''
    Delete files in object storage
    '''
    s3_client = client('s3')
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=object_name)
        return True
    except (BotoCoreError, ClientError) as e:
        raise ObjectStorageException('Error deleting file') from e
