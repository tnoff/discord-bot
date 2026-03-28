from discord_bot.utils.dispatch_envelope import (
    RequestType, ResultType,
    StreamEnvelope, StreamResult, new_request_id,
)


def test_new_request_id_unique():
    '''new_request_id returns distinct UUIDs each call.'''
    assert new_request_id() != new_request_id()


def test_stream_envelope_encode_decode_roundtrip():
    '''StreamEnvelope.encode then decode returns original values.'''
    payload = {'key': 'k1', 'guild_id': 123, 'content': ['msg'], 'channel_id': 456}
    env = StreamEnvelope(RequestType.UPDATE_MUTABLE, payload, 'proc-1', 'req-abc')

    decoded = StreamEnvelope.decode(env.encode())
    assert decoded.req_type == RequestType.UPDATE_MUTABLE
    assert decoded.payload == payload
    assert decoded.process_id == 'proc-1'
    assert decoded.request_id == 'req-abc'


def test_stream_envelope_all_request_types():
    '''All RequestType members survive a StreamEnvelope roundtrip.'''
    for req_type in RequestType:
        env = StreamEnvelope(req_type, {'guild_id': 1}, 'p', 'r')
        assert StreamEnvelope.decode(env.encode()).req_type == req_type


def test_stream_result_encode_decode_roundtrip():
    '''StreamResult.encode then decode returns original values.'''
    payload = {'guild_id': 1, 'channel_id': 2, 'messages': []}
    res = StreamResult(RequestType.FETCH_HISTORY, 'req-xyz', ResultType.HISTORY, payload)

    decoded = StreamResult.decode(res.encode())
    assert decoded.req_type == RequestType.FETCH_HISTORY
    assert decoded.request_id == 'req-xyz'
    assert decoded.result_type == ResultType.HISTORY
    assert decoded.payload == payload


def test_stream_result_error_roundtrip():
    '''ResultType.ERROR payload is preserved through StreamResult encode/decode.'''
    res = StreamResult(RequestType.FETCH_HISTORY, 'req-1', ResultType.ERROR, {'error': 'boom'})
    decoded = StreamResult.decode(res.encode())
    assert decoded.result_type == ResultType.ERROR
    assert decoded.payload['error'] == 'boom'


def test_stream_result_ok_roundtrip():
    '''ResultType.OK roundtrip works.'''
    res = StreamResult(RequestType.UPDATE_MUTABLE, 'req-2', ResultType.OK, {})
    decoded = StreamResult.decode(res.encode())
    assert decoded.result_type == ResultType.OK


def test_request_type_values_are_strings():
    '''RequestType members compare equal to their string values.'''
    assert RequestType.UPDATE_MUTABLE == 'update_mutable'
    assert RequestType.FETCH_HISTORY == 'fetch_history'
    assert RequestType.FETCH_EMOJIS == 'fetch_emojis'


def test_result_type_values_are_strings():
    '''ResultType members compare equal to their string values.'''
    assert ResultType.OK == 'ok'
    assert ResultType.ERROR == 'error'
    assert ResultType.HISTORY == 'history'
