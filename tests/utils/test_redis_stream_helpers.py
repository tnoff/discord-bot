from unittest.mock import AsyncMock

import pytest
import fakeredis.aioredis

from discord_bot.utils.redis_stream_helpers import (
    STREAM_TTL_SECONDS,
    StreamKey,
    ensure_consumer_group,
    input_stream_key,
    result_stream_key,
    xack,
    xadd,
    xread_latest,
    xreadgroup,
)


@pytest.mark.asyncio
async def test_ensure_consumer_group_creates_group():
    '''First call creates the consumer group without raising.'''
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    stream_key = input_stream_key(0)
    await ensure_consumer_group(client, stream_key)
    groups = await client.xinfo_groups(stream_key)
    assert any(g['name'] == StreamKey.CONSUMER_GROUP.value for g in groups)


@pytest.mark.asyncio
async def test_ensure_consumer_group_idempotent():
    '''Second call with BUSYGROUP does not raise.'''
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    stream_key = input_stream_key(0)
    await ensure_consumer_group(client, stream_key)
    await ensure_consumer_group(client, stream_key)  # should not raise


@pytest.mark.asyncio
async def test_xadd_and_xreadgroup_roundtrip():
    '''XADD then XREADGROUP returns the message.'''
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    stream_key = input_stream_key(0)
    await ensure_consumer_group(client, stream_key)
    await xadd(client, stream_key, {'req_type': 'send', 'payload': 'data'})

    messages = await xreadgroup(client, stream_key, 'worker-1', count=10, block_ms=0)
    assert len(messages) == 1
    msg_id, fields = messages[0]
    assert fields['req_type'] == 'send'
    assert fields['payload'] == 'data'
    assert msg_id


@pytest.mark.asyncio
async def test_xack_removes_pending():
    '''After XACK the message no longer appears in pending reads.'''
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    stream_key = input_stream_key(0)
    await ensure_consumer_group(client, stream_key)
    await xadd(client, stream_key, {'k': 'v'})

    messages = await xreadgroup(client, stream_key, 'worker-1', count=10, block_ms=0)
    msg_id, _ = messages[0]
    await xack(client, stream_key, msg_id)

    # Pending entries (re-delivered unacked) should now be empty
    pending = await client.xpending_range(stream_key, StreamKey.CONSUMER_GROUP.value, min='-', max='+', count=10)
    assert pending == []


@pytest.mark.asyncio
async def test_xread_latest_returns_new_messages():
    '''xread_latest with last_id="0" returns all messages in the stream.'''
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    stream_key = result_stream_key('proc1')
    await xadd(client, stream_key, {'result_type': 'ok', 'payload': '{}'})
    await xadd(client, stream_key, {'result_type': 'ok', 'payload': '{}'})

    messages = await xread_latest(client, stream_key, last_id='0', count=100, block_ms=0)
    assert len(messages) == 2


@pytest.mark.asyncio
async def test_ensure_consumer_group_reraises_non_busygroup_error(mocker):
    '''ensure_consumer_group re-raises exceptions that do not contain BUSYGROUP.'''
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    mocker.patch.object(client, 'xgroup_create', new=AsyncMock(side_effect=Exception('WRONGTYPE error')))
    with pytest.raises(Exception, match='WRONGTYPE error'):
        await ensure_consumer_group(client, input_stream_key(0))


@pytest.mark.asyncio
async def test_xreadgroup_returns_empty_list_when_no_messages(mocker):
    '''xreadgroup returns [] when the underlying client returns None (timeout expired).'''
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    stream_key = input_stream_key(0)
    await ensure_consumer_group(client, stream_key)
    mocker.patch.object(client, 'xreadgroup', new=AsyncMock(return_value=None))
    result = await xreadgroup(client, stream_key, 'worker-1', count=10, block_ms=0)
    assert result == []


@pytest.mark.asyncio
async def test_xread_latest_returns_empty_list_when_no_messages(mocker):
    '''xread_latest returns [] when the underlying client returns None (timeout expired).'''
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    stream_key = result_stream_key('proc1')
    mocker.patch.object(client, 'xread', new=AsyncMock(return_value=None))
    result = await xread_latest(client, stream_key, block_ms=0)
    assert result == []


@pytest.mark.asyncio
async def test_ensure_consumer_group_sets_ttl():
    '''ensure_consumer_group sets a TTL on the stream key.'''
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    stream_key = input_stream_key(0)
    await ensure_consumer_group(client, stream_key)
    ttl = await client.ttl(stream_key)
    assert 0 < ttl <= STREAM_TTL_SECONDS


@pytest.mark.asyncio
async def test_xadd_sets_ttl():
    '''xadd sets a TTL on the stream key after writing.'''
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    stream_key = result_stream_key('proc1')
    await xadd(client, stream_key, {'k': 'v'})
    ttl = await client.ttl(stream_key)
    assert 0 < ttl <= STREAM_TTL_SECONDS
