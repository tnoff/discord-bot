
import hashlib
from unittest.mock import patch, MagicMock
from pathlib import Path

from botocore.exceptions import ClientError
import pytest

from discord_core.utils.integrations import s3
from discord_core.utils.integrations.s3 import (
    upload_file, get_file, delete_file, ObjectStorageException,
    content_digest, file_digest, key_digest, key_with_digest,
)

@pytest.fixture
def mock_s3_client():
    with patch("discord_core.utils.integrations.s3.client") as mock_client_constructor:
        mock_client = MagicMock()
        mock_client_constructor.return_value = mock_client
        yield mock_client

def test_upload_file_success(mock_s3_client, tmp_path): #pylint:disable=redefined-outer-name
    # Create a temporary file
    file_path = tmp_path / "test.txt"
    file_path.write_text("hello world")

    # Mock successful put_object
    mock_s3_client.put_object.return_value = {}

    result = upload_file("my-bucket", file_path, "test.txt")
    assert result is True
    mock_s3_client.put_object.assert_called_once()

def test_upload_file_missing_path():
    invalid_path = Path("/nonexistent/file.txt")

    with pytest.raises(ObjectStorageException, match="Invalid file path"):
        upload_file("my-bucket", invalid_path)

def test_upload_file_failure(mock_s3_client, tmp_path): #pylint:disable=redefined-outer-name
    file_path = tmp_path / "test.txt"
    file_path.write_text("some data")

    # Simulate a boto3 error
    mock_s3_client.put_object.side_effect = ClientError(
        error_response={
            "Error": {
                "Code": "500",
                "Message": "Internal Server Error"
            }
        },
        operation_name="PutObject"
    )

    with pytest.raises(ObjectStorageException, match="Error uploading file"):
        upload_file("my-bucket", file_path)

def test_delete_file_success(mock_s3_client): #pylint:disable=redefined-outer-name
    # Mock successful delete
    mock_s3_client.delete_object.return_value = {}

    result = delete_file("my-bucket", "test.txt")
    assert result is True
    mock_s3_client.delete_object.assert_called_once_with(Bucket="my-bucket", Key="test.txt")

def test_delete_file_failure(mock_s3_client): #pylint:disable=redefined-outer-name
    # Simulate a boto3 error
    mock_s3_client.delete_object.side_effect = ClientError(
        error_response={
            "Error": {
                "Code": "500",
                "Message": "Internal Server Error"
            }
        },
        operation_name="DeleteObject"
    )

    with pytest.raises(ObjectStorageException, match="Error deleting file"):
        delete_file("my-bucket", "test.txt")

def test_download_file_success(mock_s3_client, tmp_path): #pylint:disable=redefined-outer-name
    object_name = "test.txt"
    destination_path = tmp_path / "downloaded.txt"
    fake_data = b"downloaded content"

    # Mock get_object to return a body that can be read
    mock_body = MagicMock()
    mock_body.read.return_value = fake_data
    mock_s3_client.get_object.return_value = {
        "Body": mock_body
    }

    result = get_file("my-bucket", object_name, destination_path)
    assert result is True

    # File should be written correctly
    assert destination_path.read_bytes() == fake_data

    mock_s3_client.get_object.assert_called_once_with(Bucket="my-bucket", Key=object_name)

def test_get_file_matching_etag_no_warning(mock_s3_client, tmp_path, mocker): #pylint:disable=redefined-outer-name
    '''No warning when ETag matches the computed MD5 of downloaded bytes'''
    mock_logger = mocker.patch('discord_core.utils.integrations.s3.logger')
    fake_data = b'downloaded content'
    correct_md5 = hashlib.md5(fake_data).hexdigest()

    mock_body = MagicMock()
    mock_body.read.return_value = fake_data
    mock_s3_client.get_object.return_value = {'Body': mock_body, 'ETag': f'"{correct_md5}"'}

    get_file('my-bucket', 'test.txt', tmp_path / 'out.txt')
    mock_logger.warning.assert_not_called()


def test_get_file_mismatched_etag_logs_warning(mock_s3_client, tmp_path, mocker): #pylint:disable=redefined-outer-name
    '''Warning logged when ETag does not match computed MD5'''
    mock_logger = mocker.patch('discord_core.utils.integrations.s3.logger')
    fake_data = b'downloaded content'
    wrong_etag = 'deadbeef000000000000000000000000'

    mock_body = MagicMock()
    mock_body.read.return_value = fake_data
    mock_s3_client.get_object.return_value = {'Body': mock_body, 'ETag': f'"{wrong_etag}"'}

    get_file('my-bucket', 'test.txt', tmp_path / 'out.txt')
    mock_logger.warning.assert_called_once()
    args = mock_logger.warning.call_args[0]
    assert wrong_etag in args[1]
    assert 'test.txt' in args[3]


def test_get_file_multipart_etag_no_warning(mock_s3_client, tmp_path, mocker): #pylint:disable=redefined-outer-name
    '''No warning for multipart ETags (contain a hyphen)'''
    mock_logger = mocker.patch('discord_core.utils.integrations.s3.logger')
    fake_data = b'downloaded content'

    mock_body = MagicMock()
    mock_body.read.return_value = fake_data
    mock_s3_client.get_object.return_value = {'Body': mock_body, 'ETag': '"abc123deadbeef-5"'}

    get_file('my-bucket', 'test.txt', tmp_path / 'out.txt')
    mock_logger.warning.assert_not_called()


def test_download_file_failure(mock_s3_client, tmp_path): #pylint:disable=redefined-outer-name
    object_name = "test.txt"
    destination_path = tmp_path / "downloaded.txt"

    # Simulate error
    mock_s3_client.get_object.side_effect = ClientError(
        error_response={
            "Error": {
                "Code": "500",
                "Message": "Internal Server Error"
            }
        },
        operation_name="GetObject"
    )

    with pytest.raises(ObjectStorageException, match="Error downloading file"):
        get_file("my-bucket", object_name, destination_path)


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


def test_get_file_verifies_the_digest_in_the_key(mock_s3_client, tmp_path): #pylint:disable=redefined-outer-name
    """Bytes that match the key's digest are written."""
    data = b'downloaded content'
    key = key_with_digest('queue/a.pcm', content_digest(data))
    mock_body = MagicMock()
    mock_body.read.return_value = data
    mock_s3_client.get_object.return_value = {'Body': mock_body}

    assert get_file('my-bucket', key, tmp_path / 'out.pcm') is True
    assert (tmp_path / 'out.pcm').read_bytes() == data


def test_get_file_rejects_bytes_that_do_not_match_the_key(mock_s3_client, tmp_path): #pylint:disable=redefined-outer-name
    """Corrupt or truncated bytes fail the fetch and nothing is written."""
    key = key_with_digest('queue/a.pcm', content_digest(b'what was uploaded'))
    mock_body = MagicMock()
    mock_body.read.return_value = b'what was uploaded, cut sh'
    mock_s3_client.get_object.return_value = {'Body': mock_body}

    # A distinct type, so a caller can tell a bad object from a failed request; still an
    # ObjectStorageException for everything that catches the base.
    with pytest.raises(s3.ObjectDigestMismatch, match='Checksum mismatch') as caught:
        get_file('my-bucket', key, tmp_path / 'out.pcm')
    assert isinstance(caught.value, ObjectStorageException)
    assert not (tmp_path / 'out.pcm').exists()


def test_get_file_does_not_check_a_key_without_a_digest(mock_s3_client, tmp_path): #pylint:disable=redefined-outer-name
    """An old key is fetched as before, whatever its bytes."""
    mock_body = MagicMock()
    mock_body.read.return_value = b'anything'
    mock_s3_client.get_object.return_value = {'Body': mock_body}

    assert get_file('my-bucket', 'cache/old.pcm', tmp_path / 'out.pcm') is True


def test_upload_file_checks_the_file_against_the_digest_in_its_key(mock_s3_client, tmp_path): #pylint:disable=redefined-outer-name
    """A file that matches the key's digest is uploaded; one that changed since it was hashed is refused."""
    file_path = tmp_path / 'a.pcm'
    file_path.write_bytes(b'audio')
    good_key = key_with_digest('queue/a.pcm', content_digest(b'audio'))
    assert upload_file('my-bucket', file_path, good_key) is True
    mock_s3_client.put_object.assert_called_once()

    mock_s3_client.put_object.reset_mock()
    file_path.write_bytes(b'audio, then something else')
    with pytest.raises(ObjectStorageException, match='does not match the digest'):
        upload_file('my-bucket', file_path, good_key)
    mock_s3_client.put_object.assert_not_called()
