import base64
import hashlib
import logging
import re
from pathlib import Path

from boto3 import client
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)

# Hex characters of the SHA-256 kept in a key. 128 bits is far beyond what catching corruption needs and
# keeps keys short.
DIGEST_LENGTH = 32
DIGEST_CHUNK_BYTES = 1024 * 1024
# `<stem>.sha256-<digest><suffix>`; the marker keeps a key from before digests from ever matching.
_KEY_DIGEST = re.compile(rf'\.sha256-(?P<digest>[0-9a-f]{{{DIGEST_LENGTH}}})(?:\.[^./]*)?$')

class ObjectStorageException(Exception):
    '''
    Object Storage exceptions
    '''

def content_digest(data: bytes) -> str:
    '''
    The digest of some bytes, in the form key_with_digest embeds.
    '''
    return hashlib.sha256(data).hexdigest()[:DIGEST_LENGTH]

def file_digest(file_path: Path) -> str:
    '''
    The digest of a file's content, read in chunks so a large file is never held whole.
    '''
    sha = hashlib.sha256()
    with Path(file_path).open('rb') as handle:
        for chunk in iter(lambda: handle.read(DIGEST_CHUNK_BYTES), b''):
            sha.update(chunk)
    return sha.hexdigest()[:DIGEST_LENGTH]

def key_with_digest(key: str, digest: str) -> str:
    '''
    Embed a content digest in an object key: `queue/a.pcm` becomes `queue/a.sha256-<digest>.pcm`.

    The key then names the exact bytes it should hold. Every upload_file and get_file of that key
    checks them, whichever pod does it, without the digest having to travel anywhere else (the broker
    registry, the cache catalog and the HTTP seam only ever carry the key).
    '''
    suffix = Path(key).suffix
    stem = key[:-len(suffix)] if suffix else key
    return f'{stem}.sha256-{digest}{suffix}'

def key_digest(key: str) -> str | None:
    '''
    The digest embedded in an object key, or None for a key without one (every key from before digests).
    '''
    match = _KEY_DIGEST.search(str(key))
    return match.group('digest') if match else None

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
    expected = key_digest(object_name)
    if expected is not None and content_digest(data) != expected:
        raise ObjectStorageException(f'{file_path} does not match the digest in its key {object_name}')
    # bandit B324: MD5 is mandated by the S3 Content-MD5 protocol header, not used for security
    md5_digest = hashlib.md5(data, usedforsecurity=False).digest()
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
        # bandit B324: comparing against S3 ETag (MD5 is dictated by S3 protocol), not used for security
        computed_md5 = hashlib.md5(data, usedforsecurity=False).hexdigest()
        etag = response.get('ETag', '').strip('"')
        # ETag from multi-part upload has '-N' suffix — skip check if so
        if etag and '-' not in etag and etag != computed_md5:
            logger.warning('S3 checksum mismatch: etag=%s computed=%s key=%s', etag, computed_md5, object_name)
    except (BotoCoreError, ClientError) as e:
        raise ObjectStorageException('Error downloading file') from e

    # A key that names its content is checked here, before anything is written: unlike the ETag check
    # above, which only warns, a mismatch means these are not the bytes that were uploaded.
    expected = key_digest(object_name)
    if expected is not None and content_digest(data) != expected:
        raise ObjectStorageException(f'Checksum mismatch downloading {object_name}')

    # Write to destination file
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(data)

    return True

def list_objects(bucket_name: str, prefix: str) -> list[dict]:
    '''
    List objects in an S3 bucket with the given prefix, sorted by last_modified descending.
    Returns [{'key': ..., 'last_modified': ...}] or [] if no objects found.
    '''
    s3_client = client('s3')
    results = []
    kwargs = {'Bucket': bucket_name, 'Prefix': prefix}
    try:
        while True:
            response = s3_client.list_objects_v2(**kwargs)
            for obj in response.get('Contents', []):
                results.append({'key': obj['Key'], 'last_modified': obj['LastModified']})
            if response.get('NextContinuationToken'):
                kwargs['ContinuationToken'] = response['NextContinuationToken']
            else:
                break
    except (BotoCoreError, ClientError) as e:
        raise ObjectStorageException('Error listing objects') from e
    results.sort(key=lambda x: x['last_modified'], reverse=True)
    return results

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
