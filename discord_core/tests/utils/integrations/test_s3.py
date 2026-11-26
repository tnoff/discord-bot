import asyncio
import base64
import hashlib
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import pytest
import pytest_asyncio
import requests
from aiohttp import web
from aiohttp.test_utils import TestServer

from discord_core.utils.integrations import s3
from discord_core.utils.integrations.s3 import (
    upload_file, ObjectDigestMismatch, ObjectStorageException, S3Config, S3RequestError,
    canonical_query, config_from_env, content_digest, file_digest, key_digest, key_with_digest,
    s3_request, sign_headers, EMPTY_PAYLOAD_HASH,
    delete_file_async, get_file_async, list_objects_async, s3_request_async, upload_file_async,
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
        if hasattr(data, 'read'):
            # A streamed file: take its bytes now, the caller closes it once the call returns.
            data = data.read()
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


def test_config_from_env_requires_keys(tmp_path):
    # Point the credentials file at nothing, so a real ~/.aws/credentials cannot satisfy the test.
    missing = {'AWS_SHARED_CREDENTIALS_FILE': str(tmp_path / 'absent')}
    with pytest.raises(ObjectStorageException, match='AWS_ACCESS_KEY_ID.*AWS_SHARED_CREDENTIALS_FILE'):
        config_from_env({'AWS_ACCESS_KEY_ID': 'k', **missing})


def _credentials_file(tmp_path, text):
    path = tmp_path / 'credentials'
    path.write_text(text, encoding='utf-8')
    return {'AWS_SHARED_CREDENTIALS_FILE': str(path)}


def test_config_from_env_reads_the_shared_credentials_file(tmp_path):
    # The shape the deployed pods mount: a `default` profile, endpoint and region from the environment.
    env = _credentials_file(tmp_path, '[default]\naws_access_key_id = gateway\naws_secret_access_key = gateway\n')
    config = config_from_env({**env, 'AWS_ENDPOINT_URL_S3': 'http://127.0.0.1:8333', 'AWS_DEFAULT_REGION': 'us-sanjose-1'})
    assert (config.access_key_id, config.secret_access_key, config.session_token) == ('gateway', 'gateway', None)
    assert (config.endpoint, config.region) == ('http://127.0.0.1:8333', 'us-sanjose-1')


def test_config_from_env_selects_the_profile_and_reads_its_session_token(tmp_path):
    env = _credentials_file(tmp_path, '[default]\naws_access_key_id = d\naws_secret_access_key = d\n'
                                      '[other]\naws_access_key_id = o\naws_secret_access_key = os\naws_session_token = tok\n')
    config = config_from_env({**env, 'AWS_PROFILE': 'other'})
    assert (config.access_key_id, config.secret_access_key, config.session_token) == ('o', 'os', 'tok')


def test_config_from_env_prefers_the_environment_keys_over_the_file(tmp_path):
    env = _credentials_file(tmp_path, '[default]\naws_access_key_id = file\naws_secret_access_key = file\n')
    config = config_from_env({**ENV, **env})
    assert (config.access_key_id, config.secret_access_key) == ('test-key', 'test-secret')


def test_config_from_env_falls_back_to_the_home_credentials_file(tmp_path, monkeypatch):
    (tmp_path / '.aws').mkdir()
    (tmp_path / '.aws' / 'credentials').write_text('[default]\naws_access_key_id = home\naws_secret_access_key = hs\n',
                                                   encoding='utf-8')
    monkeypatch.setenv('HOME', str(tmp_path))
    assert config_from_env({}).access_key_id == 'home'


@pytest.mark.parametrize('text', [
    '',                                                        # empty file
    '[other]\naws_access_key_id = a\naws_secret_access_key = b\n',  # no `default` profile
    '[default]\naws_access_key_id = a\n',                      # no secret
    '[default]\naws_access_key_id =\naws_secret_access_key = b\n',  # blank key
    'aws_access_key_id = a\n',                                  # no section header: unparseable
])
def test_config_from_env_rejects_an_unusable_credentials_file(tmp_path, text):
    with pytest.raises(ObjectStorageException, match='AWS_SHARED_CREDENTIALS_FILE'):
        config_from_env(_credentials_file(tmp_path, text))


def test_config_from_env_rejects_a_credentials_file_that_is_not_text(tmp_path):
    path = tmp_path / 'credentials'
    path.write_bytes(b'\xff\xfe\x00')
    with pytest.raises(ObjectStorageException, match='AWS_SHARED_CREDENTIALS_FILE'):
        config_from_env({'AWS_SHARED_CREDENTIALS_FILE': str(path)})


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


# ---------------------------------------------------------------------------
# The asyncio calls, against a real HTTP server on localhost
# ---------------------------------------------------------------------------

class FakeServer:
    '''
    An S3 stand-in: records every request it gets and answers from a queue of (status, body, headers).
    Each request is also checked as a store would check it: its signature is recomputed from what
    actually arrived on the wire, which catches a URL that was signed one way and sent another.
    '''
    def __init__(self):
        self.calls = []
        self.answers = []
        self.bad_signatures = []

    async def handle(self, request: web.Request) -> web.Response:
        body = await request.read()
        self.calls.append({'method': request.method, 'path': request.raw_path, 'headers': dict(request.headers), 'body': body})
        self._check_signature(request, body)
        status, content, headers = self.answers.pop(0) if self.answers else (200, b'', {})
        if headers.get('X-Drop-After'):
            # Promise the whole body, send part of it, then hang up.
            cut = int(headers['X-Drop-After'])
            response = web.StreamResponse(status=status, headers={'Content-Length': str(len(content))})
            await response.prepare(request)
            await response.write(content[:cut])
            request.transport.close()
            return response
        return web.Response(status=status, body=content, headers=headers)

    def _check_signature(self, request: web.Request, body: bytes):
        auth = request.headers['Authorization']
        signed_names = auth.split('SignedHeaders=', 1)[1].split(',', 1)[0].split(';')
        extra = {name: request.headers[name] for name in signed_names
                 if name not in ('host', 'x-amz-date', 'x-amz-content-sha256', 'x-amz-security-token')}
        path, _, query = request.raw_path.partition('?')
        expected = sign_headers(
            CONFIG_FOR_SERVER['config'], request.method, request.host, path, query, extra,
            request.headers['x-amz-content-sha256'],
            datetime.strptime(request.headers['x-amz-date'], '%Y%m%dT%H%M%SZ').replace(tzinfo=timezone.utc))
        if expected['authorization'] != auth or request.headers['x-amz-content-sha256'] != hashlib.sha256(body).hexdigest():
            self.bad_signatures.append(request.raw_path)


CONFIG_FOR_SERVER = {}


@pytest_asyncio.fixture(name='server')
async def server_fixture(monkeypatch):
    for name in ('AWS_ENDPOINT_URL', 'AWS_SESSION_TOKEN', 'AWS_DEFAULT_REGION', 'AWS_MAX_ATTEMPTS',
                 'S3_CONNECT_TIMEOUT', 'S3_READ_TIMEOUT'):
        monkeypatch.delenv(name, raising=False)
    fake = FakeServer()
    app = web.Application()
    app.router.add_route('*', '/{tail:.*}', fake.handle)
    async with TestServer(app) as test_server:
        for name, value in ENV.items():
            monkeypatch.setenv(name, value)
        monkeypatch.setenv('AWS_ENDPOINT_URL_S3', f'http://{test_server.host}:{test_server.port}')
        monkeypatch.setenv('S3_RETRY_BACKOFF', '0')
        CONFIG_FOR_SERVER['config'] = config_from_env()
        yield fake
    CONFIG_FOR_SERVER.clear()


@pytest.mark.asyncio
async def test_async_request_is_sent_and_signed_as_the_sync_one_is(server):  # pylint: disable=redefined-outer-name
    server.answers.append((200, b'hello', {'ETag': '"abc"'}))
    response = await s3_request_async('GET', 'bucket', 'songs/a b+c%.mp3')
    assert (response.status, response.content, response.headers['ETag']) == (200, b'hello', '"abc"')
    # The key reaches the server exactly as it was signed: space, plus and percent all escaped.
    assert server.calls[0]['path'] == '/bucket/songs/a%20b%2Bc%25.mp3'
    assert not server.bad_signatures


@pytest.mark.asyncio
async def test_async_request_sends_the_query_and_body_as_signed(server):  # pylint: disable=redefined-outer-name
    await s3_request_async('PUT', 'bucket', 'key', params=[('prefix', 'a b/c')], body=b'payload',
                           headers={'Content-MD5': 'x'})
    assert server.calls[0]['path'] == '/bucket/key?prefix=a%20b%2Fc'
    assert server.calls[0]['body'] == b'payload'
    assert not server.bad_signatures


@pytest.mark.asyncio
async def test_async_request_raises_with_the_error_code(server):  # pylint: disable=redefined-outer-name
    server.answers.append((404, b'<Error><Code>NoSuchKey</Code></Error>', {}))
    with pytest.raises(S3RequestError) as exc:
        await s3_request_async('GET', 'bucket', 'missing')
    assert (exc.value.status, exc.value.code) == (404, 'NoSuchKey')
    assert len(server.calls) == 1  # a 404 is final, not retried


@pytest.mark.asyncio
async def test_async_request_retries_throttling_and_server_errors(server):  # pylint: disable=redefined-outer-name
    server.answers.extend([(503, b'', {}), (429, b'', {}), (200, b'done', {})])
    response = await s3_request_async('GET', 'bucket', 'key')
    assert response.content == b'done'
    assert len(server.calls) == 3
    assert not server.bad_signatures


@pytest.mark.asyncio
async def test_async_request_gives_up_after_max_attempts(server, monkeypatch):  # pylint: disable=redefined-outer-name
    monkeypatch.setenv('AWS_MAX_ATTEMPTS', '3')
    server.answers.extend([(500, b'<Error><Code>InternalError</Code></Error>', {})] * 5)
    with pytest.raises(S3RequestError) as exc:
        await s3_request_async('GET', 'bucket', 'key')
    assert (exc.value.status, exc.value.code) == (500, 'InternalError')
    assert len(server.calls) == 3


@pytest.mark.asyncio
async def test_async_request_backs_off_by_doubling(server, monkeypatch, mocker):  # pylint: disable=redefined-outer-name
    monkeypatch.setenv('S3_RETRY_BACKOFF', '0.5')
    sleep = mocker.patch.object(s3.asyncio, 'sleep', mocker.AsyncMock())
    server.answers.extend([(500, b'', {})] * 3 + [(200, b'', {})])
    await s3_request_async('GET', 'bucket', 'key')
    assert [call.args[0] for call in sleep.await_args_list] == [0.5, 1.0, 2.0]


@pytest.mark.asyncio
async def test_async_request_wraps_connection_errors(server, monkeypatch):  # pylint: disable=redefined-outer-name
    monkeypatch.setenv('AWS_MAX_ATTEMPTS', '2')
    monkeypatch.setenv('AWS_ENDPOINT_URL_S3', 'http://127.0.0.1:1')  # nothing listens on port 1
    with pytest.raises(S3RequestError, match='GET bucket/key failed') as exc:
        await s3_request_async('GET', 'bucket', 'key')
    assert isinstance(exc.value.__cause__, aiohttp.ClientError)
    assert not server.calls


@pytest.mark.asyncio
@pytest.mark.usefixtures('server')
async def test_async_request_retries_a_timeout(monkeypatch, mocker):
    monkeypatch.setenv('AWS_MAX_ATTEMPTS', '2')
    real_request = aiohttp.ClientSession.request
    attempts = []

    def flaky(self, *args, **kwargs):
        attempts.append(1)
        if len(attempts) == 1:
            raise TimeoutError()
        return real_request(self, *args, **kwargs)

    mocker.patch.object(aiohttp.ClientSession, 'request', flaky)
    assert (await s3_request_async('GET', 'bucket', 'key')).status == 200
    assert len(attempts) == 2


@pytest.mark.asyncio
async def test_async_request_uses_the_configured_timeouts(server, monkeypatch, mocker):  # pylint: disable=redefined-outer-name
    monkeypatch.setenv('S3_CONNECT_TIMEOUT', '3')
    monkeypatch.setenv('S3_READ_TIMEOUT', '7')
    spy = mocker.spy(aiohttp, 'ClientSession')
    await s3_request_async('GET', 'bucket', 'key')
    timeout = spy.call_args.kwargs['timeout']
    assert (timeout.sock_connect, timeout.sock_read, timeout.total) == (3, 7, None)
    assert server.calls


@pytest.mark.asyncio
async def test_upload_file_async_sends_the_file_with_its_md5(server, tmp_path):  # pylint: disable=redefined-outer-name
    path = tmp_path / 'song.mp3'
    path.write_bytes(b'audio bytes')
    assert await upload_file_async('bucket', path, 'cache/song.mp3') is True
    call = server.calls[0]
    assert (call['method'], call['path'], call['body']) == ('PUT', '/bucket/cache/song.mp3', b'audio bytes')
    assert call['headers']['Content-MD5'] == base64.b64encode(hashlib.md5(b'audio bytes').digest()).decode()
    assert not server.bad_signatures


@pytest.mark.asyncio
async def test_upload_file_async_defaults_the_key_to_the_path(server, tmp_path):  # pylint: disable=redefined-outer-name
    path = tmp_path / 'song.mp3'
    path.write_bytes(b'x')
    await upload_file_async('bucket', str(path))
    assert server.calls[0]['path'] == '/bucket/' + str(path)


@pytest.mark.asyncio
async def test_upload_file_async_rejects_a_missing_file(server, tmp_path):  # pylint: disable=redefined-outer-name
    with pytest.raises(ObjectStorageException, match='Invalid file path'):
        await upload_file_async('bucket', tmp_path / 'nope.mp3')
    assert not server.calls


@pytest.mark.asyncio
async def test_upload_file_async_checks_the_digest_in_the_key(server, tmp_path):  # pylint: disable=redefined-outer-name
    path = tmp_path / 'song.mp3'
    path.write_bytes(b'audio')
    with pytest.raises(ObjectStorageException, match='does not match the digest'):
        await upload_file_async('bucket', path, key_with_digest('song.mp3', content_digest(b'other')))
    assert not server.calls


@pytest.mark.asyncio
async def test_upload_file_async_failure(server, tmp_path):  # pylint: disable=redefined-outer-name
    path = tmp_path / 'song.mp3'
    path.write_bytes(b'x')
    server.answers.append((403, b'<Error><Code>AccessDenied</Code></Error>', {}))
    with pytest.raises(ObjectStorageException, match='Error uploading file.*AccessDenied'):
        await upload_file_async('bucket', path, 'key')


@pytest.mark.asyncio
async def test_get_file_async_writes_the_object(server, tmp_path):  # pylint: disable=redefined-outer-name
    server.answers.append((200, b'audio', {'ETag': f'"{hashlib.md5(b"audio").hexdigest()}"'}))
    target = tmp_path / 'nested' / 'song.mp3'
    assert await get_file_async('bucket', 'cache/song.mp3', target) is True
    assert target.read_bytes() == b'audio'


@pytest.mark.asyncio
async def test_get_file_async_warns_on_an_etag_mismatch(server, tmp_path, caplog):  # pylint: disable=redefined-outer-name
    server.answers.append((200, b'audio', {'ETag': '"deadbeef"'}))
    with caplog.at_level('WARNING'):
        await get_file_async('bucket', 'key', tmp_path / 'song.mp3')
    assert 'S3 checksum mismatch' in caplog.text
    assert (tmp_path / 'song.mp3').read_bytes() == b'audio'


@pytest.mark.asyncio
async def test_get_file_async_rejects_bytes_that_do_not_match_the_key(server, tmp_path):  # pylint: disable=redefined-outer-name
    server.answers.append((200, b'truncated', {}))
    target = tmp_path / 'song.mp3'
    with pytest.raises(ObjectDigestMismatch):
        await get_file_async('bucket', key_with_digest('song.mp3', content_digest(b'whole object')), target)
    assert not target.exists()


@pytest.mark.asyncio
async def test_get_file_async_failure(server, tmp_path):  # pylint: disable=redefined-outer-name
    server.answers.append((404, b'<Error><Code>NoSuchKey</Code></Error>', {}))
    with pytest.raises(ObjectStorageException, match='Error downloading file.*NoSuchKey'):
        await get_file_async('bucket', 'missing', tmp_path / 'song.mp3')
    assert not (tmp_path / 'song.mp3').exists()


@pytest.mark.asyncio
async def test_delete_file_async(server):  # pylint: disable=redefined-outer-name
    assert await delete_file_async('bucket', 'cache/song.mp3') is True
    assert (server.calls[0]['method'], server.calls[0]['path']) == ('DELETE', '/bucket/cache/song.mp3')


@pytest.mark.asyncio
async def test_delete_file_async_failure(server):  # pylint: disable=redefined-outer-name
    server.answers.append((403, b'', {}))
    with pytest.raises(ObjectStorageException, match='Error deleting file'):
        await delete_file_async('bucket', 'key')


LISTING_PAGE_ONE = b'''<?xml version="1.0"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>true</IsTruncated>
<NextContinuationToken>tok/1+2</NextContinuationToken>
<Contents><Key>a</Key><LastModified>2025-01-01T00:00:00.000Z</LastModified></Contents></ListBucketResult>'''
LISTING_PAGE_TWO = b'''<?xml version="1.0"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>false</IsTruncated>
<Contents><Key>b</Key><LastModified>2025-02-01T00:00:00.000Z</LastModified></Contents></ListBucketResult>'''


@pytest.mark.asyncio
async def test_list_objects_async_follows_pages_and_sorts_newest_first(server):  # pylint: disable=redefined-outer-name
    server.answers.extend([(200, LISTING_PAGE_ONE, {}), (200, LISTING_PAGE_TWO, {})])
    objects = await list_objects_async('bucket', 'cache/')
    assert [o['key'] for o in objects] == ['b', 'a']
    assert server.calls[0]['path'] == '/bucket?list-type=2&prefix=cache%2F'
    assert server.calls[1]['path'] == '/bucket?continuation-token=tok%2F1%2B2&list-type=2&prefix=cache%2F'
    assert not server.bad_signatures


@pytest.mark.asyncio
async def test_list_objects_async_empty_and_failures(server):  # pylint: disable=redefined-outer-name
    server.answers.append((200, b'<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>', {}))
    assert await list_objects_async('bucket', 'none/') == []
    server.answers.append((200, b'not xml', {}))
    with pytest.raises(ObjectStorageException, match='Error listing objects'):
        await list_objects_async('bucket', 'x')
    server.answers.append((403, b'', {}))
    with pytest.raises(ObjectStorageException, match='Error listing objects'):
        await list_objects_async('bucket', 'x')


@pytest.mark.asyncio
async def test_upload_file_async_streams_a_file_larger_than_a_chunk(server, tmp_path, monkeypatch):  # pylint: disable=redefined-outer-name
    monkeypatch.setattr(s3, 'STREAM_CHUNK_BYTES', 7)
    data = bytes(range(256)) * 3
    path = tmp_path / 'song.mp3'
    path.write_bytes(data)
    assert await upload_file_async('bucket', path, key_with_digest('song.mp3', content_digest(data))) is True
    call = server.calls[0]
    assert call['body'] == data
    assert call['headers']['Content-Length'] == str(len(data))
    assert 'Transfer-Encoding' not in call['headers']
    assert not server.bad_signatures


@pytest.mark.asyncio
async def test_upload_file_async_sends_an_empty_file(server, tmp_path):  # pylint: disable=redefined-outer-name
    path = tmp_path / 'empty.mp3'
    path.write_bytes(b'')
    assert await upload_file_async('bucket', path, 'empty.mp3') is True
    assert server.calls[0]['body'] == b''
    assert not server.bad_signatures


@pytest.mark.asyncio
async def test_upload_file_async_restreams_from_the_start_on_retry(server, tmp_path, monkeypatch):  # pylint: disable=redefined-outer-name
    monkeypatch.setattr(s3, 'STREAM_CHUNK_BYTES', 5)
    path = tmp_path / 'song.mp3'
    path.write_bytes(b'0123456789abcdef')
    server.answers.append((503, b'', {}))
    assert await upload_file_async('bucket', path, 'song.mp3') is True
    assert [call['body'] for call in server.calls] == [b'0123456789abcdef'] * 2
    assert not server.bad_signatures


@pytest.mark.asyncio
async def test_upload_file_async_closes_the_file_when_the_upload_fails(server, tmp_path, mocker):  # pylint: disable=redefined-outer-name
    path = tmp_path / 'song.mp3'
    path.write_bytes(b'x')
    server.answers.append((403, b'', {}))
    opened = mocker.spy(Path, 'open')
    with pytest.raises(ObjectStorageException):
        await upload_file_async('bucket', path, 'key')
    assert opened.spy_return_list and all(handle.closed for handle in opened.spy_return_list)


@pytest.mark.asyncio
async def test_get_file_async_streams_an_object_larger_than_a_chunk(server, tmp_path, monkeypatch):  # pylint: disable=redefined-outer-name
    monkeypatch.setattr(s3, 'STREAM_CHUNK_BYTES', 7)
    data = bytes(range(256)) * 3
    server.answers.append((200, data, {'ETag': f'"{hashlib.md5(data).hexdigest()}"'}))
    target = tmp_path / 'song.mp3'
    key = key_with_digest('song.mp3', content_digest(data))
    assert await get_file_async('bucket', key, target) is True
    assert target.read_bytes() == data
    assert not list(tmp_path.glob('*.s3part'))


@pytest.mark.asyncio
async def test_get_file_async_leaves_no_partial_file_on_a_digest_mismatch(server, tmp_path):  # pylint: disable=redefined-outer-name
    server.answers.append((200, b'truncated', {}))
    with pytest.raises(ObjectDigestMismatch):
        await get_file_async('bucket', key_with_digest('song.mp3', content_digest(b'whole')), tmp_path / 'song.mp3')
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_get_file_async_keeps_an_existing_file_until_the_new_one_is_verified(server, tmp_path):  # pylint: disable=redefined-outer-name
    target = tmp_path / 'song.mp3'
    target.write_bytes(b'old')
    server.answers.append((200, b'bad', {}))
    with pytest.raises(ObjectDigestMismatch):
        await get_file_async('bucket', key_with_digest('song.mp3', content_digest(b'new')), target)
    assert target.read_bytes() == b'old'


@pytest.mark.asyncio
async def test_get_file_async_retries_a_body_cut_short(server, tmp_path, monkeypatch):  # pylint: disable=redefined-outer-name
    monkeypatch.setattr(s3, 'STREAM_CHUNK_BYTES', 4)
    data = b'0123456789abcdef'
    server.answers.append((200, data, {'X-Drop-After': '9'}))
    server.answers.append((200, data, {}))
    target = tmp_path / 'song.mp3'
    assert await get_file_async('bucket', key_with_digest('song.mp3', content_digest(data)), target) is True
    assert target.read_bytes() == data
    assert len(server.calls) == 2
    assert not list(tmp_path.glob('*.s3part'))


@pytest.mark.asyncio
async def test_get_file_async_cleans_up_when_every_attempt_is_cut_short(server, tmp_path, monkeypatch):  # pylint: disable=redefined-outer-name
    monkeypatch.setenv('AWS_MAX_ATTEMPTS', '2')
    server.answers.extend([(200, b'0123456789', {'X-Drop-After': '3'})] * 2)
    with pytest.raises(ObjectStorageException, match='Error downloading file'):
        await get_file_async('bucket', 'song.mp3', tmp_path / 'song.mp3')
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_upload_file_streams_a_file_from_disk(server, tmp_path, mocker):  # pylint: disable=redefined-outer-name
    data = bytes(range(256)) * 4096
    path = tmp_path / 'song.mp3'
    path.write_bytes(data)
    opened = mocker.spy(s3.Path, 'open')
    key = key_with_digest('song.mp3', content_digest(data))
    assert await asyncio.to_thread(upload_file, 'bucket', path, key) is True
    call = server.calls[0]
    assert call['body'] == data
    assert call['headers']['Content-Length'] == str(len(data))
    assert 'Transfer-Encoding' not in call['headers']
    assert call['headers']['Content-MD5'] == base64.b64encode(hashlib.md5(data).digest()).decode()
    assert not server.bad_signatures
    assert opened.spy_return_list and all(handle.closed for handle in opened.spy_return_list)


@pytest.mark.asyncio
async def test_upload_file_sends_an_empty_file(server, tmp_path):  # pylint: disable=redefined-outer-name
    path = tmp_path / 'empty.mp3'
    path.write_bytes(b'')
    assert await asyncio.to_thread(upload_file, 'bucket', path, 'empty.mp3') is True
    assert server.calls[0]['body'] == b''
    assert not server.bad_signatures


@pytest.mark.asyncio
async def test_upload_file_rewinds_the_file_on_retry(server, tmp_path):  # pylint: disable=redefined-outer-name
    data = b'0123456789abcdef' * 1000
    path = tmp_path / 'song.mp3'
    path.write_bytes(data)
    server.answers.append((503, b'', {}))
    assert await asyncio.to_thread(upload_file, 'bucket', path, 'song.mp3') is True
    assert [call['body'] for call in server.calls] == [data, data]
    assert not server.bad_signatures


@pytest.mark.asyncio
async def test_get_file_async_does_not_warn_when_the_etag_matches(server, tmp_path, caplog):  # pylint: disable=redefined-outer-name
    data = b'downloaded content'
    server.answers.append((200, data, {'ETag': f'"{hashlib.md5(data).hexdigest()}"'}))
    with caplog.at_level('WARNING'):
        await get_file_async('bucket', 'key', tmp_path / 'song.mp3')
    assert 'S3 checksum mismatch' not in caplog.text


@pytest.mark.asyncio
async def test_get_file_async_does_not_warn_about_a_multipart_etag(server, tmp_path, caplog):  # pylint: disable=redefined-outer-name
    server.answers.append((200, b'downloaded content', {'ETag': '"abc123deadbeef-5"'}))
    with caplog.at_level('WARNING'):
        await get_file_async('bucket', 'key', tmp_path / 'song.mp3')
    assert 'S3 checksum mismatch' not in caplog.text


@pytest.mark.asyncio
async def test_get_file_async_does_not_check_a_key_without_a_digest(server, tmp_path):  # pylint: disable=redefined-outer-name
    server.answers.append((200, b'anything', {}))
    assert await get_file_async('bucket', 'cache/old.pcm', tmp_path / 'out.pcm') is True
    assert (tmp_path / 'out.pcm').read_bytes() == b'anything'


@pytest.mark.asyncio
async def test_list_objects_async_parses_a_listing_without_a_namespace(server):  # pylint: disable=redefined-outer-name
    server.answers.append((200, b'''<ListBucketResult><IsTruncated>false</IsTruncated>
<Contents><Key>a</Key><LastModified>2025-01-01T00:00:00Z</LastModified></Contents></ListBucketResult>''', {}))
    objects = await list_objects_async('bucket', '')
    assert [(o['key'], o['last_modified']) for o in objects] == [('a', datetime(2025, 1, 1, tzinfo=timezone.utc))]
