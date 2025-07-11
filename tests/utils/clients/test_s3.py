
from unittest.mock import patch, MagicMock
from pathlib import Path

from botocore.exceptions import ClientError
import pytest

from discord_bot.utils.clients.s3 import upload_file, get_file, delete_file, ObjectStorageException

@pytest.fixture
def mock_s3_client():
    with patch("discord_bot.utils.clients.s3.client") as mock_client_constructor:
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
