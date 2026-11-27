'''Tests for the gateway's HTTP health server.'''
import asyncio
import json
import logging
from unittest.mock import AsyncMock, Mock

import pytest

from discord_core.utils.loop_health import LOOP_HEALTH

from discord_gateway.servers.health_server import HealthServer

from tests.net_support import cancel_task, raw_request, wait_for_port


class _FakeClock:
    '''Manually advanced monotonic clock so staleness is tested without sleeping.'''
    def __init__(self, now: float = 1000.0):
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        '''Move the clock forward.'''
        self.now += seconds


def _make_bot(is_ready=True, is_closed=False):
    bot = Mock()
    bot.is_ready.return_value = is_ready
    bot.is_closed.return_value = is_closed
    return bot


class TestHealthServerInit:
    """Sync tests for HealthServer constructor."""

    def test_init(self):
        """Constructor sets attributes correctly."""
        bot = _make_bot()
        hs = HealthServer(bot, port=9090)
        assert hs.bot is bot
        assert logging.getLogger('discord_gateway.servers.health_server').name == 'discord_gateway.servers.health_server'
        assert hs.port == 9090

    def test_init_default_port(self):
        """Default port is 8080."""
        bot = _make_bot()
        hs = HealthServer(bot)
        assert hs.port == 8080


@pytest.mark.asyncio
class TestHealthServerAsync:
    """Async tests for HealthServer HTTP responses."""

    async def test_health_ok(self):
        """Returns 200 when bot is ready and not closed."""
        bot = _make_bot(is_ready=True, is_closed=False)
        hs = HealthServer(bot, port=18080)
        task = asyncio.create_task(hs.serve())
        await wait_for_port(18080)
        try:
            response = await raw_request(18080)
            assert '200 OK' in response
            assert '"ok"' in response
        finally:
            await cancel_task(task)

    async def test_health_not_ready(self):
        """Returns 503 when bot is not ready."""
        bot = _make_bot(is_ready=False, is_closed=False)
        hs = HealthServer(bot, port=18081)
        task = asyncio.create_task(hs.serve())
        await wait_for_port(18081)
        try:
            response = await raw_request(18081)
            assert '503' in response
            assert 'unavailable' in response
        finally:
            await cancel_task(task)

    async def test_health_closed(self):
        """Returns 503 when bot is closed."""
        bot = _make_bot(is_ready=True, is_closed=True)
        hs = HealthServer(bot, port=18082)
        task = asyncio.create_task(hs.serve())
        await wait_for_port(18082)
        try:
            response = await raw_request(18082)
            assert '503' in response
            assert 'unavailable' in response
        finally:
            await cancel_task(task)

    async def test_health_is_bot_only_and_carries_no_db_key(self):
        """/health reports the bot and nothing else.

        The SELECT 1 that used to run here is gone with the engine, and no peer
        is folded in: an unreachable peer must not make the kubelet restart this
        container. /ready does not probe peers either; see
        TestHealthServerReadiness.
        """
        bot = _make_bot(is_ready=True, is_closed=False)
        hs = HealthServer(bot, port=18084)
        task = asyncio.create_task(hs.serve())
        await wait_for_port(18084)
        try:
            response = await raw_request(18084)
            assert '200 OK' in response
            body = json.loads(response.split('\r\n\r\n', 1)[1])
            assert body == {'status': 'ok'}
        finally:
            await cancel_task(task)


@pytest.mark.asyncio
class TestHealthServerReadiness:
    '''/ready answers from the bot alone: no peer pod is probed.'''

    @pytest.mark.parametrize('is_ready,is_closed,status', [
        (True, False, '200 OK'),
        (False, False, '503'),
        (True, True, '503'),
    ])
    async def test_ready_mirrors_liveness(self, is_ready, is_closed, status):
        '''/ready is 200 exactly when /health is.'''
        bot = _make_bot(is_ready=is_ready, is_closed=is_closed)
        hs = HealthServer(bot, port=18100)
        task = asyncio.create_task(hs.serve())
        await wait_for_port(18100)
        try:
            ready = await raw_request(18100, path='/ready')
            health = await raw_request(18100, path='/health')
            assert status in ready
            assert status in health
            if status == '200 OK':
                assert json.loads(ready.split('\r\n\r\n', 1)[1]) == {'status': 'ok'}
        finally:
            await cancel_task(task)

    async def test_ready_does_not_depend_on_any_peer(self, mocker):
        '''The peer probe is never called from /ready, so a dead peer cannot flip it.'''
        probe = mocker.patch('discord_gateway.utils.peer_probe.tcp_probe', new=AsyncMock(return_value=False))
        bot = _make_bot(is_ready=True, is_closed=False)
        hs = HealthServer(bot, port=18101)
        task = asyncio.create_task(hs.serve())
        await wait_for_port(18101)
        try:
            response = await raw_request(18101, path='/ready')
            assert '200 OK' in response
            probe.assert_not_called()
        finally:
            await cancel_task(task)


@pytest.mark.asyncio(loop_scope="session")
class TestHealthServerLoopHealth:
    """Background-loop health is folded into every health server's probe result.

    This is the coupling the 2026-07-31 finding asked for: the heartbeat gauge
    and the k8s probe read the same bit, so "the alert fired" and "the pod is
    unhealthy" can never disagree.
    """

    async def test_stalled_loop_fails_liveness_and_is_named_in_the_payload(self):
        clock = _FakeClock()
        LOOP_HEALTH.register('process_search_results', stale_after_seconds=60, time_func=clock)
        clock.advance(61)
        bot = _make_bot(is_ready=True, is_closed=False)
        hs = HealthServer(bot, port=18110)
        task = asyncio.create_task(hs.serve())
        await wait_for_port(18110)
        try:
            response = await raw_request(18110, path='/health')
            assert '503' in response
            body = json.loads(response.split('\r\n\r\n', 1)[1])
            assert body['loops'] == {'process_search_results': 'stalled'}
        finally:
            await cancel_task(task)

    async def test_healthy_loop_reports_ok_in_the_payload(self):
        LOOP_HEALTH.register('process_search_results', stale_after_seconds=60)
        bot = _make_bot(is_ready=True, is_closed=False)
        hs = HealthServer(bot, port=18111)
        task = asyncio.create_task(hs.serve())
        await wait_for_port(18111)
        try:
            response = await raw_request(18111, path='/health')
            assert '200 OK' in response
            body = json.loads(response.split('\r\n\r\n', 1)[1])
            assert body['loops'] == {'process_search_results': 'ok'}
        finally:
            await cancel_task(task)

    async def test_stopped_loop_does_not_fail_a_draining_pod(self):
        # cog_unload marks loops stopped; the pod must keep passing liveness
        # while it drains rather than being killed mid-shutdown.
        clock = _FakeClock()
        health = LOOP_HEALTH.register('process_search_results', stale_after_seconds=60, time_func=clock)
        health.mark_stopped()
        clock.advance(600)
        bot = _make_bot(is_ready=True, is_closed=False)
        hs = HealthServer(bot, port=18112)
        task = asyncio.create_task(hs.serve())
        await wait_for_port(18112)
        try:
            response = await raw_request(18112, path='/health')
            assert '200 OK' in response
            body = json.loads(response.split('\r\n\r\n', 1)[1])
            assert body['loops'] == {'process_search_results': 'stopped'}
        finally:
            await cancel_task(task)

    async def test_readiness_also_reflects_loop_health(self):
        clock = _FakeClock()
        LOOP_HEALTH.register('process_search_results', stale_after_seconds=60, time_func=clock)
        clock.advance(61)
        bot = _make_bot(is_ready=True, is_closed=False)
        hs = HealthServer(bot, port=18113)
        task = asyncio.create_task(hs.serve())
        await wait_for_port(18113)
        try:
            response = await raw_request(18113, path='/ready')
            assert '503' in response
        finally:
            await cancel_task(task)
