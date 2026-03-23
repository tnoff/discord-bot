from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from discord_bot.utils.integrations.s3 import list_objects, ObjectStorageException


def _make_client_error():
    return ClientError({'Error': {'Code': 'AccessDenied', 'Message': 'Access Denied'}}, 'ListObjectsV2')


def test_list_objects_empty(mocker):
    '''Returns empty list when prefix has no objects'''
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {'KeyCount': 0, 'Contents': []}
    mocker.patch('discord_bot.utils.integrations.s3.client', return_value=mock_s3)

    result = list_objects('my-bucket', 'backups/')

    assert not result


def test_list_objects_single_page(mocker):
    '''Returns sorted results for a single page response'''
    t1 = datetime(2025, 1, 2, tzinfo=timezone.utc)
    t2 = datetime(2025, 1, 1, tzinfo=timezone.utc)
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {
        'KeyCount': 2,
        'Contents': [
            {'Key': 'backups/old.json', 'LastModified': t2},
            {'Key': 'backups/new.json', 'LastModified': t1},
        ],
    }
    mocker.patch('discord_bot.utils.integrations.s3.client', return_value=mock_s3)

    result = list_objects('my-bucket', 'backups/')

    assert len(result) == 2
    assert result[0] == {'key': 'backups/new.json', 'last_modified': t1}
    assert result[1] == {'key': 'backups/old.json', 'last_modified': t2}


def test_list_objects_pagination(mocker):
    '''Follows NextContinuationToken to fetch all pages'''
    t1 = datetime(2025, 1, 3, tzinfo=timezone.utc)
    t2 = datetime(2025, 1, 2, tzinfo=timezone.utc)
    t3 = datetime(2025, 1, 1, tzinfo=timezone.utc)
    page1 = {
        'KeyCount': 2,
        'Contents': [
            {'Key': 'backups/c.json', 'LastModified': t3},
            {'Key': 'backups/b.json', 'LastModified': t2},
        ],
        'NextContinuationToken': 'token-abc',
    }
    page2 = {
        'KeyCount': 1,
        'Contents': [
            {'Key': 'backups/a.json', 'LastModified': t1},
        ],
    }
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.side_effect = [page1, page2]
    mocker.patch('discord_bot.utils.integrations.s3.client', return_value=mock_s3)

    result = list_objects('my-bucket', 'backups/')

    assert len(result) == 3
    assert result[0]['key'] == 'backups/a.json'
    assert result[1]['key'] == 'backups/b.json'
    assert result[2]['key'] == 'backups/c.json'
    # Verify second call used the continuation token
    second_call_kwargs = mock_s3.list_objects_v2.call_args_list[1][1]
    assert second_call_kwargs['ContinuationToken'] == 'token-abc'


def test_list_objects_raises_on_boto_error(mocker):
    '''Raises ObjectStorageException on boto ClientError'''
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.side_effect = _make_client_error()
    mocker.patch('discord_bot.utils.integrations.s3.client', return_value=mock_s3)

    with pytest.raises(ObjectStorageException):
        list_objects('my-bucket', 'backups/')


def test_list_objects_no_contents_key(mocker):
    '''Handles response with no Contents key (empty prefix)'''
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {'KeyCount': 0}
    mocker.patch('discord_bot.utils.integrations.s3.client', return_value=mock_s3)

    result = list_objects('my-bucket', 'nonexistent/')

    assert not result
