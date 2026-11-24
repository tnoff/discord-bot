'''
A small S3 client: the four calls this project makes (put, get, list, delete), signed with AWS
Signature Version 4 and sent with requests.

Written in place of boto3 because the backend is an S3-compatible store (Oracle Object Storage's
S3 API), not AWS: boto3 tracks AWS's defaults, and when those change (the CRC32 checksums of
boto3 1.36) other S3 implementations break, while its near-daily releases made every bump a
discord_core release for seven pods. The S3 API these calls use has not changed in years.

Configured from the same environment variables boto3 read, so a deployment needs no change:

    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY   static keys (on Oracle, a Customer Secret Key)
    AWS_SESSION_TOKEN                           optional, for temporary credentials
    AWS_SHARED_CREDENTIALS_FILE / AWS_PROFILE   optional; where to read the keys when the two
                                                variables above are unset (~/.aws/credentials,
                                                profile `default`)
    AWS_REGION / AWS_DEFAULT_REGION             the signing region (Oracle: e.g. us-ashburn-1)
    AWS_ENDPOINT_URL_S3 / AWS_ENDPOINT_URL      the endpoint; AWS's regional one when unset
    AWS_MAX_ATTEMPTS                            optional; attempts per request, the first included
    S3_CONNECT_TIMEOUT / S3_READ_TIMEOUT        optional; seconds
    S3_RETRY_BACKOFF                            optional; seconds before the first retry

Requests always use path-style addressing (`<endpoint>/<bucket>/<key>`), which Oracle requires
and AWS still serves. Unlike boto3 there is no instance-role lookup, and only the credentials file
is read, not ~/.aws/config: region and endpoint come from the variables above.

The functions stay synchronous, as they were with boto3; async callers run them in a thread.
'''
import base64
import configparser
import hashlib
import hmac
import logging
import os
import re
# The only XML parsed is replies from our own object store.
import xml.etree.ElementTree as ET  # nosec B405
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, urlsplit

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# Hex characters of the SHA-256 kept in a key. 128 bits is far beyond what catching corruption needs and
# keeps keys short.
DIGEST_LENGTH = 32
DIGEST_CHUNK_BYTES = 1024 * 1024
# `<stem>.sha256-<digest><suffix>`; the marker keeps a key from before digests from ever matching.
_KEY_DIGEST = re.compile(rf'\.sha256-(?P<digest>[0-9a-f]{{{DIGEST_LENGTH}}})(?:\.[^./]*)?$')

DEFAULT_REGION = 'us-east-1'
# Defaults for the tunables below; each can be overridden from the environment (see config_from_env).
# Seconds. Objects are whole songs held in memory, so the read timeout is generous.
DEFAULT_CONNECT_TIMEOUT = 10.0
DEFAULT_READ_TIMEOUT = 300.0
# Attempts per request, the first included, as AWS_MAX_ATTEMPTS counts them: 5 is boto3's default.
# Retries cover connection errors, throttling and 5xx. The signature stays valid across them: S3
# allows 15 minutes of clock skew.
DEFAULT_MAX_ATTEMPTS = 5
# Seconds; urllib3 doubles it each retry.
DEFAULT_RETRY_BACKOFF = 0.5
# Not configurable: these are the statuses S3 documents as worth retrying.
RETRY_STATUSES = (429, 500, 502, 503, 504)

SIGNING_ALGORITHM = 'AWS4-HMAC-SHA256'
EMPTY_PAYLOAD_HASH = hashlib.sha256(b'').hexdigest()
# RFC 3986 unreserved characters, which SigV4 leaves unencoded.
_UNRESERVED = '-_.~'

class ObjectStorageException(Exception):
    '''
    Object Storage exceptions
    '''

class ObjectDigestMismatch(ObjectStorageException):
    '''
    A fetched object's bytes do not match the digest in its key.

    Its own type because a caller can act on it: a cached object that fails this check will fail it
    on every later fetch, so it is worth reporting to whoever can evict it. A fetch can also be cut
    short in transit, so whoever evicts should verify with a read of its own first.
    '''

class S3RequestError(ObjectStorageException):
    '''
    A request that failed: the store answered with an error, or could not be reached at all.
    '''
    def __init__(self, message: str, status: int | None = None, code: str | None = None):
        super().__init__(message)
        self.status = status
        self.code = code

# ---------------------------------------------------------------------------
# Content digests in object keys
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Configuration and signing
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class S3Config:
    '''
    Where requests go, what they are signed with, and how long and how often they are tried.
    '''
    endpoint: str
    region: str
    access_key_id: str
    secret_access_key: str
    session_token: str | None = None
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT
    read_timeout: float = DEFAULT_READ_TIMEOUT
    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    retry_backoff: float = DEFAULT_RETRY_BACKOFF

def _env_number(env, name: str, default, cast, minimum):
    raw = env.get(name)
    if raw is None or raw == '':
        return default
    try:
        value = cast(raw)
    except ValueError as e:
        raise ObjectStorageException(f'{name} must be a number, got {raw!r}') from e
    if value < minimum:
        raise ObjectStorageException(f'{name} must be at least {minimum}, got {raw!r}')
    return value

def _shared_credentials(env) -> tuple[str, str, str | None] | None:
    '''
    Keys from the shared credentials file, the way boto3 read it: the file named by
    AWS_SHARED_CREDENTIALS_FILE (else ~/.aws/credentials), section AWS_PROFILE (else `default`).
    The deployed pods get theirs here, from a mounted file. None when the file, the section or
    either key is missing or unreadable.
    '''
    path = Path(env.get('AWS_SHARED_CREDENTIALS_FILE') or '~/.aws/credentials').expanduser()
    parser = configparser.RawConfigParser()
    try:
        parser.read(path, encoding='utf-8')
    except (configparser.Error, UnicodeDecodeError) as e:
        logger.warning('Could not parse the credentials file %s: %s', path, e)
        return None
    section = env.get('AWS_PROFILE') or 'default'
    if not parser.has_section(section):
        return None
    access_key_id = parser.get(section, 'aws_access_key_id', fallback=None)
    secret_access_key = parser.get(section, 'aws_secret_access_key', fallback=None)
    if not access_key_id or not secret_access_key:
        return None
    return access_key_id.strip(), secret_access_key.strip(), parser.get(section, 'aws_session_token', fallback=None)

def config_from_env(environ: dict | None = None) -> S3Config:
    '''
    Read the S3 configuration from the environment, under the variable names boto3 used.

    The tunables are optional, each falling back to its DEFAULT_* constant:

        S3_CONNECT_TIMEOUT / S3_READ_TIMEOUT   seconds
        AWS_MAX_ATTEMPTS                       attempts per request, the first included (1 = no retries)
        S3_RETRY_BACKOFF                       seconds before the first retry, doubling after
    '''
    env = os.environ if environ is None else environ
    access_key_id = env.get('AWS_ACCESS_KEY_ID')
    secret_access_key = env.get('AWS_SECRET_ACCESS_KEY')
    session_token = env.get('AWS_SESSION_TOKEN')
    if not access_key_id or not secret_access_key:
        shared = _shared_credentials(env)
        if shared is None:
            raise ObjectStorageException('AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set, '
                                         'or AWS_SHARED_CREDENTIALS_FILE must name a credentials file that has them')
        access_key_id, secret_access_key, session_token = shared
    region = env.get('AWS_REGION') or env.get('AWS_DEFAULT_REGION') or DEFAULT_REGION
    endpoint = env.get('AWS_ENDPOINT_URL_S3') or env.get('AWS_ENDPOINT_URL') \
        or f'https://s3.{region}.amazonaws.com'
    return S3Config(
        endpoint=endpoint.rstrip('/'),
        region=region,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        session_token=session_token or None,
        connect_timeout=_env_number(env, 'S3_CONNECT_TIMEOUT', DEFAULT_CONNECT_TIMEOUT, float, 0.001),
        read_timeout=_env_number(env, 'S3_READ_TIMEOUT', DEFAULT_READ_TIMEOUT, float, 0.001),
        max_attempts=_env_number(env, 'AWS_MAX_ATTEMPTS', DEFAULT_MAX_ATTEMPTS, int, 1),
        retry_backoff=_env_number(env, 'S3_RETRY_BACKOFF', DEFAULT_RETRY_BACKOFF, float, 0),
    )

def _uri_encode(value: str, safe: str = '') -> str:
    return quote(value, safe=_UNRESERVED + safe)

def canonical_query(params: list[tuple[str, str]]) -> str:
    '''
    The query string SigV4 signs: each name and value encoded, sorted by name then value.
    The request must send exactly this string.
    '''
    encoded = sorted((_uri_encode(k), _uri_encode(v)) for k, v in params)
    return '&'.join(f'{k}={v}' for k, v in encoded)

def _hmac(key: bytes, message: str) -> bytes:
    return hmac.new(key, message.encode('utf-8'), hashlib.sha256).digest()

def signing_key(secret_access_key: str, date: str, region: str, service: str = 's3') -> bytes:
    '''
    The key a day's requests are signed with, derived from the secret so the secret itself is never used.
    '''
    k_date = _hmac(f'AWS4{secret_access_key}'.encode('utf-8'), date)
    k_region = _hmac(k_date, region)
    k_service = _hmac(k_region, service)
    return _hmac(k_service, 'aws4_request')

def sign_headers(config: S3Config, method: str, host: str, canonical_uri: str, query: str, #pylint:disable=too-many-arguments,too-many-positional-arguments,too-many-locals
                 headers: dict[str, str], payload_hash: str, now: datetime) -> dict[str, str]:
    '''
    Sign a request with SigV4 and return its headers, Authorization included.

    `canonical_uri` and `query` must already be encoded, exactly as they will be sent. Every header
    passed in is signed, along with host, x-amz-date and x-amz-content-sha256 (which S3 requires).
    '''
    amz_date = now.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    date = amz_date[:8]
    signed = {k.lower(): ' '.join(str(v).split()) for k, v in headers.items()}
    signed['host'] = host
    signed['x-amz-date'] = amz_date
    signed['x-amz-content-sha256'] = payload_hash
    if config.session_token:
        signed['x-amz-security-token'] = config.session_token
    names = sorted(signed)
    signed_header_names = ';'.join(names)
    canonical_request = '\n'.join([
        method,
        canonical_uri,
        query,
        ''.join(f'{name}:{signed[name]}\n' for name in names),
        signed_header_names,
        payload_hash,
    ])
    scope = f'{date}/{config.region}/s3/aws4_request'
    string_to_sign = '\n'.join([
        SIGNING_ALGORITHM,
        amz_date,
        scope,
        hashlib.sha256(canonical_request.encode('utf-8')).hexdigest(),
    ])
    signature = hmac.new(signing_key(config.secret_access_key, date, config.region),
                         string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()
    signed['authorization'] = (
        f'{SIGNING_ALGORITHM} Credential={config.access_key_id}/{scope}, '
        f'SignedHeaders={signed_header_names}, Signature={signature}'
    )
    # requests sets Host from the URL itself, to the same value.
    del signed['host']
    return signed

# ---------------------------------------------------------------------------
# Requests
# ---------------------------------------------------------------------------

def _local_name(tag: str) -> str:
    # S3 replies are namespaced; match on the local name so a store that omits the namespace parses too.
    return tag.rsplit('}', 1)[-1]

def _child_text(element: ET.Element, name: str) -> str | None:
    for child in element:
        if _local_name(child.tag) == name:
            return child.text
    return None

def _error_code(response: requests.Response) -> str | None:
    try:
        root = ET.fromstring(response.content)  # nosec B314
    except ET.ParseError:
        return None
    return _child_text(root, 'Code')

def _session(config: S3Config) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=config.max_attempts - 1,
        backoff_factor=config.retry_backoff,
        status_forcelist=RETRY_STATUSES,
        allowed_methods=frozenset({'GET', 'PUT', 'DELETE', 'HEAD'}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session

def s3_request(method: str, bucket: str, key: str = '', *, #pylint:disable=too-many-arguments
               params: list[tuple[str, str]] | None = None, body: bytes = b'',
               headers: dict[str, str] | None = None, config: S3Config | None = None) -> requests.Response:
    '''
    Send one signed request to `<endpoint>/<bucket>/<key>` and return the response.

    Raises S3RequestError when the store cannot be reached or answers with an error status.
    '''
    config = config or config_from_env()
    endpoint = urlsplit(config.endpoint)
    canonical_uri = endpoint.path.rstrip('/') + '/' + _uri_encode(bucket)
    if key:
        canonical_uri += '/' + _uri_encode(key, safe='/')
    query = canonical_query(params or [])
    payload_hash = hashlib.sha256(body).hexdigest() if body else EMPTY_PAYLOAD_HASH
    signed = sign_headers(config, method, endpoint.netloc, canonical_uri, query,
                          headers or {}, payload_hash, datetime.now(timezone.utc))
    # The URL goes out already encoded, so it is sent exactly as signed: requests leaves %XX escapes
    # and unreserved characters alone, and the encoding above produces nothing else.
    url = f'{endpoint.scheme}://{endpoint.netloc}{canonical_uri}' + (f'?{query}' if query else '')
    try:
        with _session(config) as session:
            response = session.request(method, url, data=body or None, headers=signed,
                                       timeout=(config.connect_timeout, config.read_timeout))
    except requests.RequestException as e:
        raise S3RequestError(f'{method} {bucket}/{key} failed: {e}') from e
    if response.status_code >= 300:
        code = _error_code(response)
        raise S3RequestError(f'{method} {bucket}/{key} returned {response.status_code} {code or ""}'.rstrip(),
                             status=response.status_code, code=code)
    return response

# ---------------------------------------------------------------------------
# The four calls
# ---------------------------------------------------------------------------

def upload_file(bucket_name: str, file_path: Path, object_name: str = None) -> bool:
    '''
    Upload a file to object storage
    '''
    file_path = Path(file_path) # Double check its a path
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
        s3_request('PUT', bucket_name, object_name, body=data, headers={'Content-MD5': md5_base64})
        return True
    except S3RequestError as e:
        raise ObjectStorageException(f'Error uploading file: {e}') from e

def get_file(bucket_name: str, object_name: str, file_path: Path) -> bool:
    '''
    Download client to path
    '''
    file_path = Path(file_path) # Double check its a path
    try:
        response = s3_request('GET', bucket_name, object_name)
    except S3RequestError as e:
        raise ObjectStorageException(f'Error downloading file: {e}') from e
    data = response.content
    # bandit B324: comparing against S3 ETag (MD5 is dictated by S3 protocol), not used for security
    computed_md5 = hashlib.md5(data, usedforsecurity=False).hexdigest()
    etag = response.headers.get('ETag', '').strip('"')
    # ETag from multi-part upload has '-N' suffix — skip check if so
    if etag and '-' not in etag and etag != computed_md5:
        logger.warning('S3 checksum mismatch: etag=%s computed=%s key=%s', etag, computed_md5, object_name)

    # A key that names its content is checked here, before anything is written: unlike the ETag check
    # above, which only warns, a mismatch means these are not the bytes that were uploaded.
    expected = key_digest(object_name)
    if expected is not None and content_digest(data) != expected:
        raise ObjectDigestMismatch(f'Checksum mismatch downloading {object_name}')

    # Write to destination file
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(data)

    return True

def _parse_last_modified(value: str) -> datetime:
    # `2025-01-01T00:00:00.000Z`; fromisoformat reads the Z suffix from Python 3.11 on.
    return datetime.fromisoformat(value)

def list_objects(bucket_name: str, prefix: str) -> list[dict]:
    '''
    List objects in an S3 bucket with the given prefix, sorted by last_modified descending.
    Returns [{'key': ..., 'last_modified': ...}] or [] if no objects found.
    '''
    results = []
    params = [('list-type', '2'), ('prefix', prefix)]
    try:
        while True:
            response = s3_request('GET', bucket_name, params=params)
            root = ET.fromstring(response.content)  # nosec B314
            for element in root:
                if _local_name(element.tag) != 'Contents':
                    continue
                results.append({
                    'key': _child_text(element, 'Key'),
                    'last_modified': _parse_last_modified(_child_text(element, 'LastModified')),
                })
            token = _child_text(root, 'NextContinuationToken')
            if _child_text(root, 'IsTruncated') != 'true' or not token:
                break
            params = [('list-type', '2'), ('prefix', prefix), ('continuation-token', token)]
    except (S3RequestError, ET.ParseError, TypeError, ValueError) as e:
        raise ObjectStorageException(f'Error listing objects: {e}') from e
    results.sort(key=lambda x: x['last_modified'], reverse=True)
    return results

def delete_file(bucket_name: str, object_name: str) -> bool:
    '''
    Delete files in object storage
    '''
    try:
        s3_request('DELETE', bucket_name, object_name)
        return True
    except S3RequestError as e:
        raise ObjectStorageException(f'Error deleting file: {e}') from e
