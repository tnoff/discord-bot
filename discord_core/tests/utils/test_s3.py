from datetime import datetime, timezone

import pytest

from discord_core.utils.integrations.s3 import list_objects, ObjectStorageException, S3RequestError

NS = 'http://s3.amazonaws.com/doc/2006-03-01/'


def _page(contents: list[tuple[str, str]], token: str | None = None, namespace: str = NS) -> bytes:
    xmlns = f' xmlns="{namespace}"' if namespace else ''
    items = ''.join(f'<Contents><Key>{key}</Key><LastModified>{modified}</LastModified>'
                    f'<ETag>"x"</ETag><Size>1</Size></Contents>' for key, modified in contents)
    truncated = 'true' if token else 'false'
    next_token = f'<NextContinuationToken>{token}</NextContinuationToken>' if token else ''
    return (f'<?xml version="1.0" encoding="UTF-8"?><ListBucketResult{xmlns}><Name>my-bucket</Name>'
            f'<KeyCount>{len(contents)}</KeyCount><IsTruncated>{truncated}</IsTruncated>{next_token}'
            f'{items}</ListBucketResult>').encode()


class _Response:
    def __init__(self, content: bytes):
        self.content = content


@pytest.fixture
def pages(mocker):
    '''Patch s3_request to answer from a list of pages, recording each call's params.'''
    replies = []
    calls = []

    def fake_request(method, bucket, key='', **kwargs):
        calls.append({'method': method, 'bucket': bucket, 'key': key, **kwargs})
        reply = replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        return _Response(reply)

    mocker.patch('discord_core.utils.integrations.s3.s3_request', side_effect=fake_request)
    return replies, calls


def test_list_objects_empty(pages):  # pylint: disable=redefined-outer-name
    '''Returns empty list when prefix has no objects'''
    replies, calls = pages
    replies.append(_page([]))

    assert not list_objects('my-bucket', 'backups/')
    assert calls[0]['method'] == 'GET'
    assert calls[0]['bucket'] == 'my-bucket'
    assert calls[0]['params'] == [('list-type', '2'), ('prefix', 'backups/')]


def test_list_objects_single_page(pages):  # pylint: disable=redefined-outer-name
    '''Returns sorted results for a single page response'''
    replies, _ = pages
    replies.append(_page([('backups/old.json', '2025-01-01T00:00:00.000Z'),
                          ('backups/new.json', '2025-01-02T00:00:00.000Z')]))

    result = list_objects('my-bucket', 'backups/')

    assert result == [
        {'key': 'backups/new.json', 'last_modified': datetime(2025, 1, 2, tzinfo=timezone.utc)},
        {'key': 'backups/old.json', 'last_modified': datetime(2025, 1, 1, tzinfo=timezone.utc)},
    ]


def test_list_objects_pagination(pages):  # pylint: disable=redefined-outer-name
    '''Follows NextContinuationToken to fetch all pages'''
    replies, calls = pages
    replies.append(_page([('backups/c.json', '2025-01-01T00:00:00.000Z'),
                          ('backups/b.json', '2025-01-02T00:00:00.000Z')], token='token+abc/='))
    replies.append(_page([('backups/a.json', '2025-01-03T00:00:00.000Z')]))

    result = list_objects('my-bucket', 'backups/')

    assert [r['key'] for r in result] == ['backups/a.json', 'backups/b.json', 'backups/c.json']
    assert ('continuation-token', 'token+abc/=') in calls[1]['params']


def test_list_objects_without_a_namespace(pages):  # pylint: disable=redefined-outer-name
    '''A store that omits the S3 namespace parses the same'''
    replies, _ = pages
    replies.append(_page([('a', '2025-01-01T00:00:00Z')], namespace=''))

    assert list_objects('my-bucket', '')[0]['key'] == 'a'


def test_list_objects_raises_on_request_error(pages):  # pylint: disable=redefined-outer-name
    '''Raises ObjectStorageException when the store answers with an error'''
    replies, _ = pages
    replies.append(S3RequestError('GET my-bucket/ returned 403 AccessDenied', status=403, code='AccessDenied'))

    with pytest.raises(ObjectStorageException, match='Error listing objects'):
        list_objects('my-bucket', 'backups/')


def test_list_objects_raises_on_a_malformed_reply(pages):  # pylint: disable=redefined-outer-name
    '''A reply that is not XML is an ObjectStorageException, not a parser error'''
    replies, _ = pages
    replies.append(b'not xml')

    with pytest.raises(ObjectStorageException, match='Error listing objects'):
        list_objects('my-bucket', 'backups/')
