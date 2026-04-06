from enum import Enum

import redis.asyncio as aioredis

STREAM_TTL_SECONDS = 86400  # 1 day — last-resort cleanup for stuck stream keys


class StreamKey(Enum):
    '''Redis Stream key templates for the dispatch infrastructure.'''
    INPUT = 'discord_bot:dispatch:input:shard:{shard_id}'
    RESULT = 'discord_bot:dispatch:result:{process_id}'
    CONSUMER_GROUP = 'discord_bot:dispatch:workers'


def input_stream_key(shard_id: int) -> str:
    '''Return the Redis Stream key for the dispatch input stream of the given shard.'''
    return StreamKey.INPUT.value.format(shard_id=shard_id)


def result_stream_key(process_id: str) -> str:
    '''Return the Redis Stream key for the result stream of the given process.'''
    return StreamKey.RESULT.value.format(process_id=process_id)


async def ensure_consumer_group(client: aioredis.Redis, stream_key: str) -> None:
    '''Create consumer group if it does not exist (MKSTREAM creates stream too).'''
    try:
        await client.xgroup_create(stream_key, StreamKey.CONSUMER_GROUP.value, id='0', mkstream=True)
    except Exception as exc:
        if 'BUSYGROUP' not in str(exc):
            raise
    await client.expire(stream_key, STREAM_TTL_SECONDS)


async def xadd(client: aioredis.Redis, stream_key: str, fields: dict) -> str:
    '''Append fields to stream, return message id.'''
    msg_id = await client.xadd(stream_key, fields)
    await client.expire(stream_key, STREAM_TTL_SECONDS)
    return msg_id


async def xreadgroup(
    client: aioredis.Redis,
    stream_key: str,
    consumer_name: str,
    count: int = 10,
    block_ms: int = 2000,
) -> list:
    '''Read pending messages from consumer group. Returns list of (msg_id, fields).'''
    results = await client.xreadgroup(
        StreamKey.CONSUMER_GROUP.value, consumer_name,
        {stream_key: '>'},
        count=count, block=block_ms,
    )
    if not results:
        return []
    _, messages = results[0]
    return messages


async def xack(client: aioredis.Redis, stream_key: str, msg_id: str) -> None:
    '''Acknowledge a message in the consumer group, removing it from pending.'''
    await client.xack(stream_key, StreamKey.CONSUMER_GROUP.value, msg_id)


async def xread_latest(
    client: aioredis.Redis,
    stream_key: str,
    last_id: str = '0',
    count: int = 100,
    block_ms: int = 2000,
) -> list:
    '''Read new messages from stream (no consumer group). Returns list of (msg_id, fields).'''
    results = await client.xread({stream_key: last_id}, count=count, block=block_ms)
    if not results:
        return []
    _, messages = results[0]
    return messages
