import asyncio
import json

import pytest

from discord_bot.types.dispatch_request import (
    DeleteRequest,
    FetchChannelHistoryRequest,
    FetchGuildEmojisRequest,
    SendRequest,
)
from discord_bot.types.dispatch_result import ChannelHistoryResult, GuildEmojisResult
from discord_bot.utils.dispatch_envelope import (
    RequestType, ResultType,
    StreamResult,
)
from discord_bot.utils.redis_dispatch_client import DispatchRemoteError, RedisDispatchClient
from discord_bot.utils.redis_stream_helpers import input_stream_key, result_stream_key


@pytest.mark.asyncio
async def test_update_mutable_sends_to_input_stream(redis_client):
    '''update_mutable XADDs an envelope with the correct req_type to the input stream.'''
    client = RedisDispatchClient(redis_client, 'proc1', shard_id=0)
    await client.start()

    client.update_mutable('key1', 123, ['msg'], 456)
    await asyncio.sleep(0)  # allow fire-and-forget task to run

    msgs = await redis_client.xread({input_stream_key(0): '0-0'}, count=10)
    assert msgs, 'expected a message in the input stream'
    _, stream_msgs = msgs[0]
    _, fields = stream_msgs[0]
    assert fields['req_type'] == RequestType.UPDATE_MUTABLE
    payload = json.loads(fields['payload'])
    assert payload['key'] == 'key1'
    assert payload['guild_id'] == 123
    assert payload['content'] == ['msg']

    client.stop()


@pytest.mark.asyncio
async def test_update_mutable_empty_content_sends_remove(redis_client):
    '''update_mutable with empty content XADDs a REMOVE_MUTABLE envelope instead.'''
    client = RedisDispatchClient(redis_client, 'proc1', shard_id=0)
    await client.start()

    client.update_mutable('key1', 123, [], 456)
    await asyncio.sleep(0)

    msgs = await redis_client.xread({input_stream_key(0): '0-0'}, count=10)
    assert msgs, 'expected a message in the input stream'
    _, stream_msgs = msgs[0]
    _, fields = stream_msgs[0]
    assert fields['req_type'] == RequestType.REMOVE_MUTABLE
    payload = json.loads(fields['payload'])
    assert payload['key'] == 'key1'

    client.stop()


@pytest.mark.asyncio
async def test_fetch_history_awaits_result(redis_client, mocker):
    '''dispatch_channel_history resolves when a matching result arrives on the result stream.'''
    known_req_id = 'req-fixed-id'
    mocker.patch('discord_bot.utils.redis_dispatch_client.new_request_id', return_value=known_req_id)

    client = RedisDispatchClient(redis_client, 'proc1', shard_id=0)
    await client.start()

    async def inject_result():
        # wait for dispatch_channel_history to enqueue its request
        await asyncio.sleep(0)
        result_fields = StreamResult(
            RequestType.FETCH_HISTORY, known_req_id, ResultType.HISTORY,
            {'guild_id': 1, 'channel_id': 2, 'after_message_id': None, 'messages': []},
        )
        await redis_client.xadd(result_stream_key('proc1'), result_fields.encode())

    asyncio.create_task(inject_result())

    result = await asyncio.wait_for(
        client.dispatch_channel_history(1, 2, limit=10),
        timeout=5.0,
    )
    assert isinstance(result, ChannelHistoryResult)
    assert result.guild_id == 1
    assert result.messages == []

    client.stop()


@pytest.mark.asyncio
async def test_fetch_history_raises_on_error_result(redis_client, mocker):
    '''dispatch_channel_history raises DispatchRemoteError when the result is ResultType.ERROR.'''
    known_req_id = 'req-error-id'
    mocker.patch('discord_bot.utils.redis_dispatch_client.new_request_id', return_value=known_req_id)

    client = RedisDispatchClient(redis_client, 'proc1', shard_id=0)
    await client.start()

    async def inject_error():
        await asyncio.sleep(0)
        error_fields = StreamResult(
            RequestType.FETCH_HISTORY, known_req_id, ResultType.ERROR,
            {'error': 'channel not found'},
        )
        await redis_client.xadd(result_stream_key('proc1'), error_fields.encode())

    asyncio.create_task(inject_error())

    with pytest.raises(DispatchRemoteError, match='channel not found'):
        await asyncio.wait_for(
            client.dispatch_channel_history(1, 2, limit=10),
            timeout=5.0,
        )

    client.stop()


# ---------------------------------------------------------------------------
# register_cog_queue
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_register_cog_queue(redis_client):
    '''register_cog_queue returns a new asyncio.Queue for the given cog name.'''
    client = RedisDispatchClient(redis_client, 'proc1')
    q = client.register_cog_queue('mycog')
    assert isinstance(q, asyncio.Queue)


# ---------------------------------------------------------------------------
# Fire-and-forget helpers
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_remove_mutable_sends_to_stream(redis_client):
    '''remove_mutable XADDs a RequestType.REMOVE_MUTABLE envelope to the input stream.'''
    client = RedisDispatchClient(redis_client, 'proc1')
    await client.start()

    client.remove_mutable('mykey')
    await asyncio.sleep(0)

    msgs = await redis_client.xread({input_stream_key(0): '0-0'}, count=10)
    _, stream_msgs = msgs[0]
    _, fields = stream_msgs[0]
    assert fields['req_type'] == RequestType.REMOVE_MUTABLE
    assert json.loads(fields['payload'])['key'] == 'mykey'
    client.stop()


@pytest.mark.asyncio
async def test_update_mutable_channel_sends_to_stream(redis_client):
    '''update_mutable_channel XADDs a RequestType.UPDATE_MUTABLE_CHANNEL envelope to the input stream.'''
    client = RedisDispatchClient(redis_client, 'proc1')
    await client.start()

    client.update_mutable_channel('mykey', 1, 999)
    await asyncio.sleep(0)

    msgs = await redis_client.xread({input_stream_key(0): '0-0'}, count=10)
    _, stream_msgs = msgs[0]
    _, fields = stream_msgs[0]
    assert fields['req_type'] == RequestType.UPDATE_MUTABLE_CHANNEL
    payload = json.loads(fields['payload'])
    assert payload['key'] == 'mykey'
    assert payload['new_channel_id'] == 999
    client.stop()


@pytest.mark.asyncio
async def test_send_message_sends_to_stream(redis_client):
    '''send_message XADDs a RequestType.SEND envelope to the input stream.'''
    client = RedisDispatchClient(redis_client, 'proc1')
    await client.start()

    client.send_message(1, 2, 'hello')
    await asyncio.sleep(0)

    msgs = await redis_client.xread({input_stream_key(0): '0-0'}, count=10)
    _, stream_msgs = msgs[0]
    _, fields = stream_msgs[0]
    assert fields['req_type'] == RequestType.SEND
    assert json.loads(fields['payload'])['content'] == 'hello'
    client.stop()


@pytest.mark.asyncio
async def test_delete_message_sends_to_stream(redis_client):
    '''delete_message XADDs a RequestType.DELETE envelope to the input stream.'''
    client = RedisDispatchClient(redis_client, 'proc1')
    await client.start()

    client.delete_message(1, 2, 12345)
    await asyncio.sleep(0)

    msgs = await redis_client.xread({input_stream_key(0): '0-0'}, count=10)
    _, stream_msgs = msgs[0]
    _, fields = stream_msgs[0]
    assert fields['req_type'] == RequestType.DELETE
    assert json.loads(fields['payload'])['message_id'] == 12345
    client.stop()


# ---------------------------------------------------------------------------
# dispatch_guild_emojis
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dispatch_guild_emojis_returns_result(redis_client, mocker):
    '''dispatch_guild_emojis resolves when a matching result arrives on the result stream.'''
    known_req_id = 'req-emojis-id'
    mocker.patch('discord_bot.utils.redis_dispatch_client.new_request_id', return_value=known_req_id)

    client = RedisDispatchClient(redis_client, 'proc1')
    await client.start()

    async def inject_result():
        await asyncio.sleep(0)
        await redis_client.xadd(result_stream_key('proc1'), StreamResult(
            RequestType.FETCH_EMOJIS, known_req_id, ResultType.EMOJIS,
            {'guild_id': 1, 'emojis': [{'id': 10, 'name': 'wave', 'animated': False}]},
        ).encode())

    asyncio.create_task(inject_result())
    result = await asyncio.wait_for(client.dispatch_guild_emojis(1), timeout=5.0)
    assert isinstance(result, GuildEmojisResult)
    assert result.guild_id == 1
    client.stop()


# ---------------------------------------------------------------------------
# submit_request routing
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_submit_request_send(redis_client):
    '''submit_request(SendRequest) enqueues RequestType.SEND on the input stream.'''
    client = RedisDispatchClient(redis_client, 'proc1')
    await client.start()

    await client.submit_request(SendRequest(guild_id=1, channel_id=2, content='hi'))
    await asyncio.sleep(0)

    msgs = await redis_client.xread({input_stream_key(0): '0-0'}, count=10)
    _, stream_msgs = msgs[0]
    _, fields = stream_msgs[0]
    assert fields['req_type'] == RequestType.SEND
    client.stop()


@pytest.mark.asyncio
async def test_submit_request_delete(redis_client):
    '''submit_request(DeleteRequest) enqueues RequestType.DELETE on the input stream.'''
    client = RedisDispatchClient(redis_client, 'proc1')
    await client.start()

    await client.submit_request(DeleteRequest(guild_id=1, channel_id=2, message_id=999))
    await asyncio.sleep(0)

    msgs = await redis_client.xread({input_stream_key(0): '0-0'}, count=10)
    _, stream_msgs = msgs[0]
    _, fields = stream_msgs[0]
    assert fields['req_type'] == RequestType.DELETE
    client.stop()


@pytest.mark.asyncio
async def test_submit_request_history_delivers_to_queue(redis_client, mocker):
    '''submit_request(FetchChannelHistoryRequest) delivers a ChannelHistoryResult to the cog queue.'''
    known_req_id = 'req-hist-submit'
    mocker.patch('discord_bot.utils.redis_dispatch_client.new_request_id', return_value=known_req_id)

    client = RedisDispatchClient(redis_client, 'proc1')
    await client.start()
    q = client.register_cog_queue('testcog')

    async def inject():
        await asyncio.sleep(0)
        await redis_client.xadd(result_stream_key('proc1'), StreamResult(
            RequestType.FETCH_HISTORY, known_req_id, ResultType.HISTORY,
            {'guild_id': 1, 'channel_id': 2, 'after_message_id': None, 'messages': []},
        ).encode())

    asyncio.create_task(inject())
    await client.submit_request(FetchChannelHistoryRequest(
        guild_id=1, channel_id=2, limit=10, cog_name='testcog',
    ))
    result = await asyncio.wait_for(q.get(), timeout=5.0)
    assert isinstance(result, ChannelHistoryResult)
    client.stop()


@pytest.mark.asyncio
async def test_submit_request_history_no_queue(redis_client):
    '''submit_request with FetchChannelHistoryRequest for unregistered cog enqueues nothing.'''
    client = RedisDispatchClient(redis_client, 'proc1')
    await client.start()

    await client.submit_request(FetchChannelHistoryRequest(
        guild_id=1, channel_id=2, limit=10, cog_name='notregistered',
    ))
    await asyncio.sleep(0.05)

    msgs = await redis_client.xread({input_stream_key(0): '0-0'}, count=10)
    assert not msgs
    client.stop()


@pytest.mark.asyncio
async def test_submit_request_history_error_delivers_to_queue(redis_client, mocker):
    '''submit_request(FetchChannelHistoryRequest) delivers an error ChannelHistoryResult on ResultType.ERROR.'''
    known_req_id = 'req-hist-err'
    mocker.patch('discord_bot.utils.redis_dispatch_client.new_request_id', return_value=known_req_id)

    client = RedisDispatchClient(redis_client, 'proc1')
    await client.start()
    q = client.register_cog_queue('testcog')

    async def inject_error():
        await asyncio.sleep(0)
        await redis_client.xadd(result_stream_key('proc1'), StreamResult(
            RequestType.FETCH_HISTORY, known_req_id, ResultType.ERROR, {'error': 'gone'},
        ).encode())

    asyncio.create_task(inject_error())
    await client.submit_request(FetchChannelHistoryRequest(
        guild_id=1, channel_id=2, limit=10, cog_name='testcog',
    ))
    result = await asyncio.wait_for(q.get(), timeout=5.0)
    assert isinstance(result, ChannelHistoryResult)
    assert result.error is not None
    client.stop()


@pytest.mark.asyncio
async def test_submit_request_emojis_delivers_to_queue(redis_client, mocker):
    '''submit_request(FetchGuildEmojisRequest) delivers a GuildEmojisResult to the cog queue.'''
    known_req_id = 'req-emoji-ok'
    mocker.patch('discord_bot.utils.redis_dispatch_client.new_request_id', return_value=known_req_id)

    client = RedisDispatchClient(redis_client, 'proc1')
    await client.start()
    q = client.register_cog_queue('testcog')

    async def inject():
        await asyncio.sleep(0)
        await redis_client.xadd(result_stream_key('proc1'), StreamResult(
            RequestType.FETCH_EMOJIS, known_req_id, ResultType.EMOJIS, {'guild_id': 1, 'emojis': []},
        ).encode())

    asyncio.create_task(inject())
    await client.submit_request(FetchGuildEmojisRequest(guild_id=1, cog_name='testcog'))
    result = await asyncio.wait_for(q.get(), timeout=5.0)
    assert isinstance(result, GuildEmojisResult)
    client.stop()


@pytest.mark.asyncio
async def test_submit_request_emojis_no_queue(redis_client):
    '''submit_request with FetchGuildEmojisRequest for unregistered cog enqueues nothing.'''
    client = RedisDispatchClient(redis_client, 'proc1')
    await client.start()

    await client.submit_request(FetchGuildEmojisRequest(guild_id=1, cog_name='notregistered'))
    await asyncio.sleep(0.05)

    msgs = await redis_client.xread({input_stream_key(0): '0-0'}, count=10)
    assert not msgs
    client.stop()


@pytest.mark.asyncio
async def test_submit_request_emojis_error_delivers_to_queue(redis_client, mocker):
    '''submit_request(FetchGuildEmojisRequest) delivers an error GuildEmojisResult on ResultType.ERROR.'''
    known_req_id = 'req-emoji-err'
    mocker.patch('discord_bot.utils.redis_dispatch_client.new_request_id', return_value=known_req_id)

    client = RedisDispatchClient(redis_client, 'proc1')
    await client.start()
    q = client.register_cog_queue('testcog')

    async def inject_error():
        await asyncio.sleep(0)
        await redis_client.xadd(result_stream_key('proc1'), StreamResult(
            RequestType.FETCH_EMOJIS, known_req_id, ResultType.ERROR, {'error': 'gone'},
        ).encode())

    asyncio.create_task(inject_error())
    await client.submit_request(FetchGuildEmojisRequest(guild_id=1, cog_name='testcog'))
    result = await asyncio.wait_for(q.get(), timeout=5.0)
    assert isinstance(result, GuildEmojisResult)
    assert result.error is not None
    client.stop()
