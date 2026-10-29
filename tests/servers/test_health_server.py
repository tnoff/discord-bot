"""
Tests for the HTTP health server.
"""
import asyncio
import json
import logging
from unittest.mock import AsyncMock, MagicMock, Mock

import fakeredis.aioredis
import pytest

from discord_core.clients.redis_client import RedisManager
from discord_core.servers.health_server_base import HealthServerBase, close_writer
from discord_core.utils.loop_health import LOOP_HEALTH

from discord_gateway.servers.health_server import HealthServer
from discord_dispatcher.servers.dispatch_health_server import DispatchHealthServer


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


async def _wait_for_port(port: int, timeout: float = 5.0) -> None:
    """Poll until the port accepts a TCP connection or timeout expires."""
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        try:
            _, writer = await asyncio.open_connection('127.0.0.1', port)
            await close_writer(writer)
            return
        except OSError:
            if asyncio.get_event_loop().time() >= deadline:
                raise
            await asyncio.sleep(0.01)


async def _raw_request(port: int, path: str = '/health') -> str:
    """Open a raw TCP connection and send a minimal HTTP GET, return the full response."""
    reader, writer = await asyncio.open_connection('127.0.0.1', port)
    writer.write(f'GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n'.encode())
    await writer.drain()
    response = b''
    try:
        response = await asyncio.wait_for(reader.read(4096), timeout=3)
    finally:
        await close_writer(writer)
    return response.decode()


def _make_reader(*lines):
    """Return a mock reader whose readline yields each line in sequence."""
    reader = MagicMock()
    reader.readline = AsyncMock(side_effect=list(lines))
    return reader


def _make_writer():
    """Return a mock writer suitable for _handle calls."""
    writer = MagicMock()
    writer.write = MagicMock()
    writer.drain = AsyncMock()
    writer.close = MagicMock()
    writer.wait_closed = AsyncMock()
    return writer


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
        await _wait_for_port(18080)
        try:
            response = await _raw_request(18080)
            assert '200 OK' in response
            assert '"ok"' in response
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def test_health_not_ready(self):
        """Returns 503 when bot is not ready."""
        bot = _make_bot(is_ready=False, is_closed=False)
        hs = HealthServer(bot, port=18081)
        task = asyncio.create_task(hs.serve())
        await _wait_for_port(18081)
        try:
            response = await _raw_request(18081)
            assert '503' in response
            assert 'unavailable' in response
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def test_health_closed(self):
        """Returns 503 when bot is closed."""
        bot = _make_bot(is_ready=True, is_closed=True)
        hs = HealthServer(bot, port=18082)
        task = asyncio.create_task(hs.serve())
        await _wait_for_port(18082)
        try:
            response = await _raw_request(18082)
            assert '503' in response
            assert 'unavailable' in response
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

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
        await _wait_for_port(18084)
        try:
            response = await _raw_request(18084)
            assert '200 OK' in response
            body = json.loads(response.split('\r\n\r\n', 1)[1])
            assert body == {'status': 'ok'}
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    async def test_handle_exception_during_request(self):
        """Exception mid-request is caught and writer is still closed cleanly"""
        bot = _make_bot()
        hs = HealthServer(bot)
        reader = MagicMock()
        reader.readline = AsyncMock(side_effect=ConnectionResetError('connection reset'))
        writer = MagicMock()
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()
        await getattr(hs, '_handle')(reader, writer)
        writer.close.assert_called_once()

    async def test_handle_wait_closed_exception(self):
        """Exception in wait_closed is swallowed; writer.close still called"""
        bot = _make_bot()
        hs = HealthServer(bot)
        reader = MagicMock()
        reader.readline = AsyncMock(side_effect=ConnectionResetError('connection reset'))
        writer = MagicMock()
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock(side_effect=OSError('broken pipe'))
        await getattr(hs, '_handle')(reader, writer)
        writer.close.assert_called_once()


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
        await _wait_for_port(18090)
        try:
            response = await _raw_request(18090)
            assert '200 OK' in response
            assert '"ok"' in response
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def test_health_stays_ok_when_redis_unavailable(self):
        """Liveness does not ask Redis: a restart cannot fix it, and three failed probes restart the pod."""
        fake_redis = AsyncMock()
        fake_redis.ping = AsyncMock(side_effect=ConnectionError('redis down'))
        hs = DispatchHealthServer(RedisManager.from_client(fake_redis), port=18091)
        task = asyncio.create_task(hs.serve())
        await _wait_for_port(18091)
        try:
            response = await _raw_request(18091)
            assert '200 OK' in response
            fake_redis.ping.assert_not_awaited()
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def test_ready_redis_unavailable(self):
        """Readiness is where Redis is checked: 503 when the ping raises."""
        fake_redis = AsyncMock()
        fake_redis.ping = AsyncMock(side_effect=ConnectionError('redis down'))
        hs = DispatchHealthServer(RedisManager.from_client(fake_redis), port=18095)
        task = asyncio.create_task(hs.serve())
        await _wait_for_port(18095)
        try:
            response = await _raw_request(18095, '/ready')
            assert '503' in response
            assert 'unavailable' in response
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def test_handle_exception_during_request(self):
        """Exception mid-request is caught and writer is still closed cleanly."""
        fake_redis = AsyncMock()
        hs = DispatchHealthServer(RedisManager.from_client(fake_redis))
        reader = MagicMock()
        reader.readline = AsyncMock(side_effect=ConnectionResetError('connection reset'))
        writer = MagicMock()
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()
        await getattr(hs, '_handle')(reader, writer)
        writer.close.assert_called_once()

    async def test_handle_wait_closed_exception(self):
        """Exception in wait_closed is swallowed; writer.close still called."""
        fake_redis = AsyncMock()
        hs = DispatchHealthServer(RedisManager.from_client(fake_redis))
        reader = MagicMock()
        reader.readline = AsyncMock(side_effect=ConnectionResetError('connection reset'))
        writer = MagicMock()
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock(side_effect=OSError('broken pipe'))
        await getattr(hs, '_handle')(reader, writer)
        writer.close.assert_called_once()


@pytest.mark.asyncio
async def test_health_server_base_check_not_implemented():
    '''HealthServerBase._check raises NotImplementedError — subclasses must override it.'''
    base = HealthServerBase(port=0, bind_address='127.0.0.1')
    with pytest.raises(NotImplementedError):
        await base._check()  #pylint:disable=protected-access


@pytest.mark.asyncio
async def test_health_server_base_readiness_defaults_to_check():
    '''HealthServerBase._readiness_check delegates to _check by default.'''
    class _Stub(HealthServerBase):
        async def _check(self):
            return False, {'why': 'stub'}
    stub = _Stub(port=0, bind_address='127.0.0.1')
    ok, extra = await stub._readiness_check()  #pylint:disable=protected-access
    assert ok is False
    assert extra == {'why': 'stub'}


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
        await _wait_for_port(18100)
        try:
            ready = await _raw_request(18100, path='/ready')
            health = await _raw_request(18100, path='/health')
            assert status in ready
            assert status in health
            if status == '200 OK':
                assert json.loads(ready.split('\r\n\r\n', 1)[1]) == {'status': 'ok'}
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def test_ready_does_not_depend_on_any_peer(self, mocker):
        '''The peer probe is never called from /ready, so a dead peer cannot flip it.'''
        probe = mocker.patch('discord_gateway.utils.peer_probe.tcp_probe', new=AsyncMock(return_value=False))
        bot = _make_bot(is_ready=True, is_closed=False)
        hs = HealthServer(bot, port=18101)
        task = asyncio.create_task(hs.serve())
        await _wait_for_port(18101)
        try:
            response = await _raw_request(18101, path='/ready')
            assert '200 OK' in response
            probe.assert_not_called()
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass


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
        await _wait_for_port(18110)
        try:
            response = await _raw_request(18110, path='/health')
            assert '503' in response
            body = json.loads(response.split('\r\n\r\n', 1)[1])
            assert body['loops'] == {'process_search_results': 'stalled'}
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def test_healthy_loop_reports_ok_in_the_payload(self):
        LOOP_HEALTH.register('process_search_results', stale_after_seconds=60)
        bot = _make_bot(is_ready=True, is_closed=False)
        hs = HealthServer(bot, port=18111)
        task = asyncio.create_task(hs.serve())
        await _wait_for_port(18111)
        try:
            response = await _raw_request(18111, path='/health')
            assert '200 OK' in response
            body = json.loads(response.split('\r\n\r\n', 1)[1])
            assert body['loops'] == {'process_search_results': 'ok'}
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

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
        await _wait_for_port(18112)
        try:
            response = await _raw_request(18112, path='/health')
            assert '200 OK' in response
            body = json.loads(response.split('\r\n\r\n', 1)[1])
            assert body['loops'] == {'process_search_results': 'stopped'}
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def test_readiness_also_reflects_loop_health(self):
        clock = _FakeClock()
        LOOP_HEALTH.register('process_search_results', stale_after_seconds=60, time_func=clock)
        clock.advance(61)
        bot = _make_bot(is_ready=True, is_closed=False)
        hs = HealthServer(bot, port=18113)
        task = asyncio.create_task(hs.serve())
        await _wait_for_port(18113)
        try:
            response = await _raw_request(18113, path='/ready')
            assert '503' in response
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass


def test_apply_loop_health_omits_the_key_when_no_loops_are_registered():
    # Processes without background loops are unaffected — no empty 'loops' dict.
    ok, extra = HealthServerBase._apply_loop_health(True, {'db': 'ok'})  #pylint:disable=protected-access
    assert ok is True
    assert extra == {'db': 'ok'}


def test_apply_loop_health_cannot_rescue_an_already_failing_check():
    # Loop health only ever adds a reason to fail, never masks one.
    LOOP_HEALTH.register('process_search_results', stale_after_seconds=60)
    ok, extra = HealthServerBase._apply_loop_health(False, {})  #pylint:disable=protected-access
    assert ok is False
    assert extra == {'loops': {'process_search_results': 'ok'}}
