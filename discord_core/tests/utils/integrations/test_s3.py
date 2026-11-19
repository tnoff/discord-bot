import base64
import hashlib
from datetime import datetime, timezone
from pathlib import Path

import pytest
import requests

from discord_core.utils.integrations import s3
from discord_core.utils.integrations.s3 import (
    upload_file, get_file, delete_file, ObjectStorageException, S3Config, S3RequestError,
    canonical_query, config_from_env, content_digest, file_digest, key_digest, key_with_digest,
    s3_request, sign_headers, EMPTY_PAYLOAD_HASH,
)

ENV = {
    'AWS_ACCESS_KEY_ID': 'test-key',
    'AWS_SECRET_ACCESS_KEY': 'test-secret',
    'AWS_REGION': 'us-ashburn-1',
    'AWS_ENDPOINT_URL_S3': 'https://ns.compat.objectstorage.us-ashburn-1.oraclecloud.com',
}


def make_response(status: int = 200, content: bytes = b'', headers: dict | None = None) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response._content = content  # pylint: disable=protected-access
    response.headers.update(headers or {})
    return response


class FakeStore:
    '''
    Stands in for the HTTP session: records each request and answers from a queue.
    '''
    def __init__(self):
        self.calls = []
        self.responses = []
        self.error = None

    def __call__(self, config):
        self.config = config  # pylint: disable=attribute-defined-outside-init
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def request(self, method, url, data=None, headers=None, timeout=None):
        self.calls.append({'method': method, 'url': url, 'data': data, 'headers': headers, 'timeout': timeout})
        if self.error:
            raise self.error
        return self.responses.pop(0) if self.responses else make_response()


@pytest.fixture
def store(monkeypatch):
    for name in ('AWS_ENDPOINT_URL', 'AWS_SESSION_TOKEN', 'AWS_DEFAULT_REGION', 'AWS_MAX_ATTEMPTS',
                 'S3_CONNECT_TIMEOUT', 'S3_READ_TIMEOUT', 'S3_RETRY_BACKOFF'):
        monkeypatch.delenv(name, raising=False)
    for name, value in ENV.items():
        monkeypatch.setenv(name, value)
    fake = FakeStore()
    monkeypatch.setattr(s3, '_session', fake)
    return fake


# ---------------------------------------------------------------------------
# SigV4, against AWS's published worked examples
# (https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html)
# ---------------------------------------------------------------------------

AWS_EXAMPLE = S3Config(
    endpoint='https://examplebucket.s3.amazonaws.com',
    region='us-east-1',
    access_key_id='AKIAIOSFODNN7EXAMPLE',
    secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
)
AWS_EXAMPLE_TIME = datetime(2013, 5, 24, tzinfo=timezone.utc)
AWS_EXAMPLE_HOST = 'examplebucket.s3.amazonaws.com'


def _signature(headers: dict) -> str:
    return headers['authorization'].rsplit('Signature=', 1)[1]


def test_sigv4_get_object_example():
    headers = sign_headers(AWS_EXAMPLE, 'GET', AWS_EXAMPLE_HOST, '/test.txt', '',
                           {'Range': 'bytes=0-9'}, EMPTY_PAYLOAD_HASH, AWS_EXAMPLE_TIME)
    assert headers['authorization'] == (
        'AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, '
        'SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, '
        'Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41'
    )
    assert headers['x-amz-date'] == '20130524T000000Z'
    assert 'host' not in headers


def test_sigv4_put_object_example():
    body = b'Welcome to Amazon S3.'
    headers = sign_headers(
        AWS_EXAMPLE, 'PUT', AWS_EXAMPLE_HOST, '/' + s3._uri_encode('test$file.text'), '',  # pylint: disable=protected-access
        {'Date': 'Fri, 24 May 2013 00:00:00 GMT', 'x-amz-storage-class': 'REDUCED_REDUNDANCY'},
        hashlib.sha256(body).hexdigest(), AWS_EXAMPLE_TIME,
    )
    assert _signature(headers) == '98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd'


def test_sigv4_get_bucket_lifecycle_example():
    headers = sign_headers(AWS_EXAMPLE, 'GET', AWS_EXAMPLE_HOST, '/', canonical_query([('lifecycle', '')]),
                           {}, EMPTY_PAYLOAD_HASH, AWS_EXAMPLE_TIME)
    assert _signature(headers) == 'fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543'


def test_sigv4_list_objects_example():
    headers = sign_headers(AWS_EXAMPLE, 'GET', AWS_EXAMPLE_HOST, '/',
                           canonical_query([('prefix', 'J'), ('max-keys', '2')]),
                           {}, EMPTY_PAYLOAD_HASH, AWS_EXAMPLE_TIME)
    assert _signature(headers) == '34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7'


def test_sigv4_signs_the_session_token():
    config = S3Config(**{**AWS_EXAMPLE.__dict__, 'session_token': 'tok'})
    headers = sign_headers(config, 'GET', AWS_EXAMPLE_HOST, '/test.txt', '', {}, EMPTY_PAYLOAD_HASH,
                           AWS_EXAMPLE_TIME)
    assert headers['x-amz-security-token'] == 'tok'
    assert 'x-amz-security-token' in headers['authorization']


def test_canonical_query_encodes_and_sorts():
    assert canonical_query([('prefix', 'queue/a b'), ('list-type', '2'), ('continuation-token', 'a+b=/')]) == \
        'continuation-token=a%2Bb%3D%2F&list-type=2&prefix=queue%2Fa%20b'


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def test_config_from_env_reads_the_boto3_variables():
    config = config_from_env({**ENV, 'AWS_SESSION_TOKEN': 'tok'})
    assert config == S3Config(endpoint=ENV['AWS_ENDPOINT_URL_S3'], region='us-ashburn-1',
                              access_key_id='test-key', secret_access_key='test-secret', session_token='tok')


def test_config_from_env_falls_back_to_aws_defaults():
    config = config_from_env({'AWS_ACCESS_KEY_ID': 'k', 'AWS_SECRET_ACCESS_KEY': 's',
                              'AWS_DEFAULT_REGION': 'eu-west-1'})
    assert config.region == 'eu-west-1'
    assert config.endpoint == 'https://s3.eu-west-1.amazonaws.com'
    assert config_from_env({'AWS_ACCESS_KEY_ID': 'k', 'AWS_SECRET_ACCESS_KEY': 's'}).region == 'us-east-1'


def test_config_from_env_uses_the_generic_endpoint_and_strips_the_slash():
    config = config_from_env({'AWS_ACCESS_KEY_ID': 'k', 'AWS_SECRET_ACCESS_KEY': 's',
                              'AWS_ENDPOINT_URL': 'http://localhost:9000/'})
    assert config.endpoint == 'http://localhost:9000'


def test_config_from_env_defaults_the_tunables():
    config = config_from_env(ENV)
    assert (config.connect_timeout, config.read_timeout) == (s3.DEFAULT_CONNECT_TIMEOUT, s3.DEFAULT_READ_TIMEOUT)
    assert config.max_attempts == s3.DEFAULT_MAX_ATTEMPTS
    assert config.retry_backoff == s3.DEFAULT_RETRY_BACKOFF


def test_config_from_env_overrides_the_tunables():
    config = config_from_env({**ENV, 'S3_CONNECT_TIMEOUT': '3', 'S3_READ_TIMEOUT': '45.5',
                              'AWS_MAX_ATTEMPTS': '1', 'S3_RETRY_BACKOFF': '0', 'AWS_REGION': ''})
    assert (config.connect_timeout, config.read_timeout) == (3.0, 45.5)
    assert config.max_attempts == 1
    assert config.retry_backoff == 0.0
    # One attempt means no retries at all.
    assert s3._session(config).get_adapter('https://example.com').max_retries.total == 0  # pylint: disable=protected-access


def test_config_from_env_treats_an_empty_tunable_as_unset():
    assert config_from_env({**ENV, 'S3_READ_TIMEOUT': ''}).read_timeout == s3.DEFAULT_READ_TIMEOUT


@pytest.mark.parametrize('name, value, message', [
    ('S3_READ_TIMEOUT', 'soon', 'must be a number'),
    ('AWS_MAX_ATTEMPTS', '2.5', 'must be a number'),
    ('AWS_MAX_ATTEMPTS', '0', 'must be at least 1'),
    ('S3_CONNECT_TIMEOUT', '0', 'must be at least'),
    ('S3_RETRY_BACKOFF', '-1', 'must be at least 0'),
])
def test_config_from_env_rejects_bad_tunables(name, value, message):
    with pytest.raises(ObjectStorageException, match=f'{name} {message}'):
        config_from_env({**ENV, name: value})


def test_config_from_env_requires_keys():
    with pytest.raises(ObjectStorageException, match='AWS_ACCESS_KEY_ID'):
        config_from_env({'AWS_ACCESS_KEY_ID': 'k'})


# ---------------------------------------------------------------------------
# Requests
# ---------------------------------------------------------------------------

def test_s3_request_uses_path_style_and_encodes_the_key(store):  # pylint: disable=redefined-outer-name
    s3_request('GET', 'my-bucket', 'queue/a song+1.pcm')
    call = store.calls[0]
    assert call['url'] == ('https://ns.compat.objectstorage.us-ashburn-1.oraclecloud.com'
                           '/my-bucket/queue/a%20song%2B1.pcm')
    assert call['headers']['x-amz-content-sha256'] == EMPTY_PAYLOAD_HASH
    assert 'Credential=test-key/' in call['headers']['authorization']
    assert '/us-ashburn-1/s3/aws4_request' in call['headers']['authorization']
    assert call['data'] is None
    assert call['timeout'] == (s3.DEFAULT_CONNECT_TIMEOUT, s3.DEFAULT_READ_TIMEOUT)


def test_s3_request_keeps_an_endpoint_path_and_port(store, monkeypatch):  # pylint: disable=redefined-outer-name
    monkeypatch.setenv('AWS_ENDPOINT_URL_S3', 'http://localhost:9000/base/')
    s3_request('DELETE', 'b', 'k')
    assert store.calls[0]['url'] == 'http://localhost:9000/base/b/k'


def test_s3_request_sends_the_query_as_signed(store):  # pylint: disable=redefined-outer-name
    s3_request('GET', 'b', params=[('prefix', 'a/'), ('list-type', '2')])
    assert store.calls[0]['url'].endswith('/b?list-type=2&prefix=a%2F')


def test_s3_request_raises_with_the_error_code(store):  # pylint: disable=redefined-outer-name
    store.responses.append(make_response(403, b'<?xml version="1.0"?><Error><Code>SignatureDoesNotMatch</Code></Error>'))
    with pytest.raises(S3RequestError, match='403 SignatureDoesNotMatch') as caught:
        s3_request('GET', 'b', 'k')
    assert caught.value.status == 403
    assert caught.value.code == 'SignatureDoesNotMatch'


def test_s3_request_raises_on_an_error_without_a_body(store):  # pylint: disable=redefined-outer-name
    store.responses.append(make_response(404))
    with pytest.raises(S3RequestError) as caught:
        s3_request('GET', 'b', 'k')
    assert caught.value.code is None


def test_s3_request_wraps_connection_errors(store):  # pylint: disable=redefined-outer-name
    store.error = requests.ConnectionError('refused')
    with pytest.raises(S3RequestError, match='refused'):
        s3_request('GET', 'b', 'k')


def test_s3_request_uses_the_configured_timeouts(store, monkeypatch):  # pylint: disable=redefined-outer-name
    monkeypatch.setenv('S3_CONNECT_TIMEOUT', '2.5')
    monkeypatch.setenv('S3_READ_TIMEOUT', '60')
    s3_request('GET', 'b', 'k')
    assert store.calls[0]['timeout'] == (2.5, 60.0)


def test_session_retries_transient_failures():
    retry = s3._session(config_from_env(ENV)).get_adapter('https://example.com').max_retries  # pylint: disable=protected-access
    assert retry.total == s3.DEFAULT_MAX_ATTEMPTS - 1
    assert retry.backoff_factor == s3.DEFAULT_RETRY_BACKOFF
    assert 503 in retry.status_forcelist
    assert 'PUT' in retry.allowed_methods


# ---------------------------------------------------------------------------
# upload / get / delete
# ---------------------------------------------------------------------------

def test_upload_file_success(store, tmp_path):  # pylint: disable=redefined-outer-name
    file_path = tmp_path / "test.txt"
    file_path.write_text("hello world")

    assert upload_file("my-bucket", file_path, "test.txt") is True
    call = store.calls[0]
    assert call['method'] == 'PUT'
    assert call['url'].endswith('/my-bucket/test.txt')
    assert call['data'] == b'hello world'
    assert call['headers']['content-md5'] == base64.b64encode(hashlib.md5(b'hello world').digest()).decode()
    assert call['headers']['x-amz-content-sha256'] == hashlib.sha256(b'hello world').hexdigest()
    assert 'content-md5' in call['headers']['authorization']


def test_upload_file_missing_path():
    invalid_path = Path("/nonexistent/file.txt")

    with pytest.raises(ObjectStorageException, match="Invalid file path"):
        upload_file("my-bucket", invalid_path)


def test_upload_file_failure(store, tmp_path):  # pylint: disable=redefined-outer-name
    file_path = tmp_path / "test.txt"
    file_path.write_text("some data")
    store.responses.append(make_response(500))

    with pytest.raises(ObjectStorageException, match="Error uploading file"):
        upload_file("my-bucket", file_path)


def test_delete_file_success(store):  # pylint: disable=redefined-outer-name
    store.responses.append(make_response(204))

    assert delete_file("my-bucket", "test.txt") is True
    assert store.calls[0]['method'] == 'DELETE'
    assert store.calls[0]['url'].endswith('/my-bucket/test.txt')


def test_delete_file_failure(store):  # pylint: disable=redefined-outer-name
    store.responses.append(make_response(500))

    with pytest.raises(ObjectStorageException, match="Error deleting file"):
        delete_file("my-bucket", "test.txt")


def test_download_file_success(store, tmp_path):  # pylint: disable=redefined-outer-name
    destination_path = tmp_path / "nested" / "downloaded.txt"
    store.responses.append(make_response(200, b"downloaded content"))

    assert get_file("my-bucket", "test.txt", destination_path) is True
    assert destination_path.read_bytes() == b"downloaded content"
    assert store.calls[0]['method'] == 'GET'
    assert store.calls[0]['url'].endswith('/my-bucket/test.txt')


def test_get_file_matching_etag_no_warning(store, tmp_path, mocker):  # pylint: disable=redefined-outer-name
    '''No warning when ETag matches the computed MD5 of downloaded bytes'''
    mock_logger = mocker.patch('discord_core.utils.integrations.s3.logger')
    fake_data = b'downloaded content'
    store.responses.append(make_response(200, fake_data, {'ETag': f'"{hashlib.md5(fake_data).hexdigest()}"'}))

    get_file('my-bucket', 'test.txt', tmp_path / 'out.txt')
    mock_logger.warning.assert_not_called()


def test_get_file_mismatched_etag_logs_warning(store, tmp_path, mocker):  # pylint: disable=redefined-outer-name
    '''Warning logged when ETag does not match computed MD5'''
    mock_logger = mocker.patch('discord_core.utils.integrations.s3.logger')
    wrong_etag = 'deadbeef000000000000000000000000'
    store.responses.append(make_response(200, b'downloaded content', {'ETag': f'"{wrong_etag}"'}))

    get_file('my-bucket', 'test.txt', tmp_path / 'out.txt')
    mock_logger.warning.assert_called_once()
    args = mock_logger.warning.call_args[0]
    assert wrong_etag in args[1]
    assert 'test.txt' in args[3]


def test_get_file_multipart_etag_no_warning(store, tmp_path, mocker):  # pylint: disable=redefined-outer-name
    '''No warning for multipart ETags (contain a hyphen)'''
    mock_logger = mocker.patch('discord_core.utils.integrations.s3.logger')
    store.responses.append(make_response(200, b'downloaded content', {'ETag': '"abc123deadbeef-5"'}))

    get_file('my-bucket', 'test.txt', tmp_path / 'out.txt')
    mock_logger.warning.assert_not_called()


def test_download_file_failure(store, tmp_path):  # pylint: disable=redefined-outer-name
    store.responses.append(make_response(500))

    with pytest.raises(ObjectStorageException, match="Error downloading file"):
        get_file("my-bucket", "test.txt", tmp_path / "downloaded.txt")
    assert not (tmp_path / "downloaded.txt").exists()


# ---------------------------------------------------------------------------
# Content digests in object keys
# ---------------------------------------------------------------------------

def test_key_with_digest_round_trips():
    """The digest goes before the extension and comes back out of the key."""
    digest = content_digest(b'audio')
    key = key_with_digest('queue/youtube.abc.pcm', digest)
    assert key == f'queue/youtube.abc.sha256-{digest}.pcm'
    assert key_digest(key) == digest
    assert len(digest) == 32


def test_key_with_digest_without_an_extension():
    """A key with no extension gets the digest at the end and still reads back."""
    digest = content_digest(b'audio')
    key = key_with_digest('queue/noextension', digest)
    assert key == f'queue/noextension.sha256-{digest}'
    assert key_digest(key) == digest


@pytest.mark.parametrize('key', [
    'cache/youtube.abc.pcm',
    'queue/a.sha256-short.pcm',
    'queue/a.sha256-' + 'g' * 32 + '.pcm',
    'queue/a.sha256-' + 'a' * 32 + '.pcm.part',
    'sha256-' + 'a' * 32 + '.pcm',
])
def test_key_digest_ignores_keys_without_a_digest(key):
    """Keys from before digests (and near misses) carry none, so nothing is checked for them."""
    assert key_digest(key) is None


def test_file_digest_matches_content_digest_across_chunks(tmp_path, monkeypatch):
    """Streaming a file gives the same digest as hashing it whole, including across chunk boundaries."""
    monkeypatch.setattr(s3, 'DIGEST_CHUNK_BYTES', 4)
    path = tmp_path / 'audio.pcm'
    path.write_bytes(b'0123456789abcdef-tail')
    assert file_digest(path) == content_digest(b'0123456789abcdef-tail')


def test_get_file_verifies_the_digest_in_the_key(store, tmp_path):  # pylint: disable=redefined-outer-name
    """Bytes that match the key's digest are written."""
    data = b'downloaded content'
    key = key_with_digest('queue/a.pcm', content_digest(data))
    store.responses.append(make_response(200, data))

    assert get_file('my-bucket', key, tmp_path / 'out.pcm') is True
    assert (tmp_path / 'out.pcm').read_bytes() == data


def test_get_file_rejects_bytes_that_do_not_match_the_key(store, tmp_path):  # pylint: disable=redefined-outer-name
    """Corrupt or truncated bytes fail the fetch and nothing is written."""
    key = key_with_digest('queue/a.pcm', content_digest(b'what was uploaded'))
    store.responses.append(make_response(200, b'what was uploaded, cut sh'))

    # A distinct type, so a caller can tell a bad object from a failed request; still an
    # ObjectStorageException for everything that catches the base.
    with pytest.raises(s3.ObjectDigestMismatch, match='Checksum mismatch') as caught:
        get_file('my-bucket', key, tmp_path / 'out.pcm')
    assert isinstance(caught.value, ObjectStorageException)
    assert not (tmp_path / 'out.pcm').exists()


def test_get_file_does_not_check_a_key_without_a_digest(store, tmp_path):  # pylint: disable=redefined-outer-name
    """An old key is fetched as before, whatever its bytes."""
    store.responses.append(make_response(200, b'anything'))

    assert get_file('my-bucket', 'cache/old.pcm', tmp_path / 'out.pcm') is True


def test_upload_file_checks_the_file_against_the_digest_in_its_key(store, tmp_path):  # pylint: disable=redefined-outer-name
    """A file that matches the key's digest is uploaded; one that changed since it was hashed is refused."""
    file_path = tmp_path / 'a.pcm'
    file_path.write_bytes(b'audio')
    good_key = key_with_digest('queue/a.pcm', content_digest(b'audio'))
    assert upload_file('my-bucket', file_path, good_key) is True
    assert len(store.calls) == 1

    file_path.write_bytes(b'audio, then something else')
    with pytest.raises(ObjectStorageException, match='does not match the digest'):
        upload_file('my-bucket', file_path, good_key)
    assert len(store.calls) == 1
