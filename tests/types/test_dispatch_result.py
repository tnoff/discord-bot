'''Tests for dispatch result error encoding and classification.'''
from discord.errors import NotFound

from discord_bot.clients.dispatch_client_base import DispatchRemoteError
from discord_bot.types.dispatch_result import (
    UNKNOWN_MESSAGE_CODE,
    encode_error,
    is_not_found_error,
)

from tests.helpers import FakeResponse


def test_encode_error_captures_status_and_code():
    '''encode_error carries the discord status/code that str(exc) alone would erase'''
    exc = NotFound(FakeResponse(), {'code': UNKNOWN_MESSAGE_CODE, 'message': 'Unknown Message'})
    detail = encode_error(exc)
    assert detail['status'] == 404
    assert detail['code'] == UNKNOWN_MESSAGE_CODE
    assert detail['type'] == 'NotFound'
    assert 'Unknown Message' in detail['message']


def test_encode_error_plain_exception_has_no_status():
    '''An exception without status/code encodes them as None rather than raising'''
    detail = encode_error(ValueError('boom'))
    assert detail == {'message': 'boom', 'type': 'ValueError', 'status': None, 'code': None}


def test_is_not_found_error_matches_raw_discord_notfound():
    '''A raw discord.NotFound is still recognised (in-process callers)'''
    assert is_not_found_error(NotFound(FakeResponse(), 'gone')) is True


def test_is_not_found_error_matches_dispatch_remote_error():
    '''
    The transport-rebuilt error is recognised too.

    This is the case an isinstance(error, NotFound) check silently misses: the
    dispatcher flattens the exception to JSON, so the cog only ever sees a
    DispatchRemoteError.
    '''
    rebuilt = DispatchRemoteError.from_payload({
        'error': '404 Not Found (error code: 10008): Unknown Message',
        'error_detail': {'status': 404, 'code': UNKNOWN_MESSAGE_CODE, 'type': 'NotFound'},
    })
    assert is_not_found_error(rebuilt) is True


def test_is_not_found_error_rejects_other_failures():
    '''A non-404 failure is not treated as recoverable'''
    assert is_not_found_error(ValueError('boom')) is False
    assert is_not_found_error(DispatchRemoteError('boom', status=500)) is False


def test_dispatch_remote_error_from_payload_without_detail():
    '''
    A payload from a not-yet-rolled dispatcher still yields the message.

    Bot and dispatcher roll together, but during the skew window the older
    dispatcher emits 'error' with no 'error_detail'; that must degrade to the
    old string-only behaviour rather than KeyError.
    '''
    err = DispatchRemoteError.from_payload({'error': 'something broke'})
    assert str(err) == 'something broke'
    assert err.status is None
    assert err.code is None
    assert err.error_type is None
    assert is_not_found_error(err) is False
