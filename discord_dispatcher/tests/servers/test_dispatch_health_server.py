'''Tests for the dispatcher's HTTP health server.'''
import asyncio
from unittest.mock import AsyncMock

import fakeredis.aioredis
import pytest

from discord_core.clients.redis_client import RedisManager

from discord_dispatcher.servers.dispatch_health_server import DispatchHealthServer

from testing.net_support import cancel_task, raw_request, wait_for_port


class TestDispatchHealthServerInit:
    """Sync tests for DispatchHealthServer constructor."""

    def test_init(self):
        """Constructor sets attributes correctly."""
        manager = RedisManager('redis://localhost:6379/0')
        hs = DispatchHealthServer(manager, port=9090)
        assert hs.redis_manager is manager
        assert hs.port == 9090

    def test_init_default_port(self):
        """Default port is 8080."""
        manager = RedisManager('redis://localhost:6379/0')
        hs = DispatchHealthServer(manager)
        assert hs.port == 8080


@pytest.mark.asyncio
class TestDispatchHealthServerAsync:
    """Async tests for DispatchHealthServer HTTP responses."""

    async def test_health_ok(self):
        """Returns 200 when Redis ping succeeds."""
        fake_redis = fakeredis.aioredis.FakeRedis(protocol=2)
        hs = DispatchHealthServer(RedisManager.from_client(fake_redis), port=18090)
        task = asyncio.create_task(hs.serve())
        await wait_for_port(18090)
        try:
            response = await raw_request(18090)
            assert '200 OK' in response
            assert '"ok"' in response
        finally:
            await cancel_task(task)

    async def test_health_stays_ok_when_redis_unavailable(self):
        """Liveness does not ask Redis: a restart cannot fix it, and three failed probes restart the pod."""
        fake_redis = AsyncMock()
        fake_redis.ping = AsyncMock(side_effect=ConnectionError('redis down'))
        hs = DispatchHealthServer(RedisManager.from_client(fake_redis), port=18091)
        task = asyncio.create_task(hs.serve())
        await wait_for_port(18091)
        try:
            response = await raw_request(18091)
            assert '200 OK' in response
            fake_redis.ping.assert_not_awaited()
        finally:
            await cancel_task(task)

    async def test_ready_redis_unavailable(self):
        """Readiness is where Redis is checked: 503 when the ping raises."""
        fake_redis = AsyncMock()
        fake_redis.ping = AsyncMock(side_effect=ConnectionError('redis down'))
        hs = DispatchHealthServer(RedisManager.from_client(fake_redis), port=18095)
        task = asyncio.create_task(hs.serve())
        await wait_for_port(18095)
        try:
            response = await raw_request(18095, '/ready')
            assert '503' in response
            assert 'unavailable' in response
        finally:
            await cancel_task(task)
