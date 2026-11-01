'''
Opening a guild ensures its history playlist (workers/guild_queue.py), off the request and best-effort.
'''
import asyncio
import logging
from unittest.mock import AsyncMock

import fakeredis.aioredis
import pytest

from discord_core.clients.redis_client import RedisManager

from discord_broker.workers.broker_registry import RedisBrokerRegistry
from discord_broker.workers.guild_queue import GuildQueueBroker
from discord_broker.workers.guild_queue_registry import GuildQueueRegistry
from discord_broker.workers.redis_broker import RedisBroker

GUILD = 11


def make_queue(ensure=None) -> GuildQueueBroker:
    manager = RedisManager.from_client(fakeredis.aioredis.FakeRedis(decode_responses=True))
    broker = RedisBroker(RedisBrokerRegistry(manager), bucket_name='bucket')
    return GuildQueueBroker(broker, GuildQueueRegistry(manager), ensure_history_playlist=ensure)


async def settle(queue: GuildQueueBroker) -> None:
    '''Let the ensure task the open spawned finish.'''
    while queue._ensures:  # pylint: disable=protected-access
        await asyncio.wait(list(queue._ensures))  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_opening_a_guild_ensures_its_history_playlist():
    ensure = AsyncMock(return_value=5)
    queue = make_queue(ensure)
    await queue.open(GUILD, 22)
    await settle(queue)
    ensure.assert_awaited_once_with(GUILD)


@pytest.mark.asyncio
async def test_the_open_does_not_wait_for_the_db_pod():
    release = asyncio.Event()

    async def slow(_guild_id):
        await release.wait()
        return 5

    queue = make_queue(slow)
    await asyncio.wait_for(queue.open(GUILD, 22), timeout=1)
    assert queue._ensures  # pylint: disable=protected-access
    release.set()
    await settle(queue)


@pytest.mark.asyncio
async def test_a_failing_ensure_is_logged_and_the_open_still_succeeds(caplog):
    queue = make_queue(AsyncMock(side_effect=RuntimeError('db down')))
    with caplog.at_level(logging.WARNING):
        assert await queue.open(GUILD, 22) is None
        await settle(queue)
    assert f'Could not ensure the history playlist for guild {GUILD}' in caplog.text


@pytest.mark.asyncio
async def test_without_a_db_pod_nothing_is_ensured():
    queue = make_queue()
    await queue.open(GUILD, 22)
    assert not queue._ensures  # pylint: disable=protected-access
