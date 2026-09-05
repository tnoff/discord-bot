"""
Tests for the HTTP health server.
"""
import asyncio
import json
import logging
from unittest.mock import AsyncMock, MagicMock, Mock

import fakeredis.aioredis
import pytest

from discord_bot.clients.redis_client import RedisManager
from discord_bot.servers.dispatch_health_server import DispatchHealthServer
from discord_bot.servers.health_server import HealthServer
from discord_bot.servers.health_server_base import HealthServerBase, close_writer
from discord_bot.utils.loop_health import LOOP_HEALTH


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
        assert logging.getLogger('discord_bot.servers.health_server').name == 'discord_bot.servers.health_server'
        assert hs.port == 9090

    def test_init_default_port(self):
        """Default port is 8080."""
        bot = _make_bot()
        hs = HealthServer(bot)
        assert hs.port == 8080

    def test_init_with_database_http_url(self):
        """The db pod's URL is stored when provided."""
        bot = _make_bot()
        hs = HealthServer(bot, database_http_url='http://discord-db:8085')
        assert hs._database_http_url == 'http://discord-db:8085'  #pylint:disable=protected-access

    def test_init_without_database_http_url(self):
        """database_http_url defaults to None, and nothing is probed."""
        hs = HealthServer(_make_bot())
        assert hs._database_http_url is None  #pylint:disable=protected-access


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

        The SELECT 1 that used to run here is gone with the engine, and the db
        pod is deliberately NOT folded back in as a liveness check: an
        unreachable peer must not make the kubelet restart this container. It
        belongs on /ready, where TestHealthServerReadiness covers it.
        """
        bot = _make_bot(is_ready=True, is_closed=False)
        hs = HealthServer(bot, port=18084, database_http_url='http://discord-db:8085')
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
        await asyncio.sleep(0.05)
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

    async def test_health_redis_unavailable(self):
        """Returns 503 when Redis ping raises."""
        fake_redis = AsyncMock()
        fake_redis.ping = AsyncMock(side_effect=ConnectionError('redis down'))
        hs = DispatchHealthServer(RedisManager.from_client(fake_redis), port=18091)
        task = asyncio.create_task(hs.serve())
        await asyncio.sleep(0.05)
        try:
            response = await _raw_request(18091)
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
    '''/ready routes to _readiness_check; with dispatch_http_url it adds a TCP probe.'''

    async def test_ready_without_dispatch_url_returns_liveness(self):
        '''When dispatch_http_url is unset, /ready mirrors /health.'''
        bot = _make_bot(is_ready=True, is_closed=False)
        hs = HealthServer(bot, port=18100)
        task = asyncio.create_task(hs.serve())
        await _wait_for_port(18100)
        try:
            response = await _raw_request(18100, path='/ready')
            assert '200 OK' in response
            body = json.loads(response.split('\r\n\r\n', 1)[1])
            assert body == {'status': 'ok'}
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def test_ready_check_increments_counter_by_outcome(self, mocker):
        '''_readiness_check records the dispatcher probe outcome on the counter.'''
        counter = mocker.patch('discord_bot.servers.health_server._READY_CHECK_COUNTER')
        bot = _make_bot(is_ready=True, is_closed=False)
        hs = HealthServer(bot, port=18110, dispatch_http_url='http://dispatcher:8082')

        mocker.patch.object(hs, '_tcp_probe', AsyncMock(return_value=True))
        await hs._readiness_check()  # pylint: disable=protected-access
        counter.add.assert_called_once_with(1, {'outcome': 'ok'})

        counter.reset_mock()
        mocker.patch.object(hs, '_tcp_probe', AsyncMock(return_value=False))
        await hs._readiness_check()  # pylint: disable=protected-access
        counter.add.assert_called_once_with(1, {'outcome': 'unavailable'})

    async def test_database_peer_lands_on_its_own_counter(self, mocker):
        '''The db probe increments DATABASE_PEER_READY_CHECK, never the dispatcher's.

        Two counters rather than one counter with a `peer` dimension, because the
        docker-apps dashboard panel sums dispatcher_ready_check by outcome under a
        dispatcher-titled panel: a dimension would fold db results into it and
        leave a correct-looking graph answering a different question. This asserts
        the separation directly, since nothing else would notice it breaking.
        '''
        dispatch_counter = mocker.patch(
            'discord_bot.servers.health_server._READY_CHECK_COUNTER')
        database_counter = mocker.patch(
            'discord_bot.servers.health_server._DATABASE_PEER_COUNTER')
        bot = _make_bot(is_ready=True, is_closed=False)
        hs = HealthServer(bot, port=18112, database_http_url='http://discord-db:8085')

        mocker.patch.object(hs, '_tcp_probe', AsyncMock(return_value=True))
        ok, payload = await hs._readiness_check()  # pylint: disable=protected-access

        assert ok is True
        assert payload == {'database': 'ok'}
        database_counter.add.assert_called_once_with(1, {'outcome': 'ok'})
        dispatch_counter.add.assert_not_called()

    async def test_both_peers_are_probed_and_reported_independently(self, mocker):
        '''One unreachable peer fails readiness without masking the other's state.

        Short-circuiting on the first failure would make the payload say which
        probe ran rather than which dependency is down -- with two peers that is
        the difference between "the dispatcher is gone" and "everything is gone",
        which is exactly what an operator reads this endpoint to find out.
        '''
        bot = _make_bot(is_ready=True, is_closed=False)
        hs = HealthServer(bot, port=18113,
                          dispatch_http_url='http://dispatcher:8082',
                          database_http_url='http://discord-db:8085')

        async def probe(url):
            return 'dispatcher' in url

        mocker.patch.object(hs, '_tcp_probe', AsyncMock(side_effect=probe))
        ok, payload = await hs._readiness_check()  # pylint: disable=protected-access

        assert ok is False
        assert payload == {'dispatch': 'ok', 'database': 'unavailable'}

    async def test_ready_check_skips_counter_without_dispatch_url(self, mocker):
        '''No probe and no counter increment when dispatch_http_url is unset.'''
        counter = mocker.patch('discord_bot.servers.health_server._READY_CHECK_COUNTER')
        bot = _make_bot(is_ready=True, is_closed=False)
        hs = HealthServer(bot, port=18111)
        await hs._readiness_check()  # pylint: disable=protected-access
        counter.add.assert_not_called()

    async def test_ready_with_unreachable_dispatcher_returns_503(self):
        '''/ready 503s and reports dispatch:unavailable when the URL doesn't connect.'''
        bot = _make_bot(is_ready=True, is_closed=False)
        # Port 1 is reserved (tcpmux); a connect on localhost:1 fast-fails on Linux
        hs = HealthServer(bot, port=18101, dispatch_http_url='http://127.0.0.1:1')
        task = asyncio.create_task(hs.serve())
        await _wait_for_port(18101)
        try:
            response = await _raw_request(18101, path='/ready')
            assert '503' in response
            body = json.loads(response.split('\r\n\r\n', 1)[1])
            assert body['status'] == 'unavailable'
            assert body['dispatch'] == 'unavailable'
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def test_ready_with_reachable_dispatcher_returns_200(self):
        '''/ready 200s and reports dispatch:ok when a TCP listener accepts the connect.'''
        bot = _make_bot(is_ready=True, is_closed=False)
        # Stand up a stub listener that accepts and immediately closes
        async def _accept(_reader, writer):
            await close_writer(writer)
        stub = await asyncio.start_server(_accept, '127.0.0.1', 18102)
        stub_task = asyncio.create_task(stub.serve_forever())
        hs = HealthServer(bot, port=18103, dispatch_http_url='http://127.0.0.1:18102')
        task = asyncio.create_task(hs.serve())
        await _wait_for_port(18103)
        try:
            response = await _raw_request(18103, path='/ready')
            assert '200 OK' in response
            body = json.loads(response.split('\r\n\r\n', 1)[1])
            assert body['status'] == 'ok'
            assert body['dispatch'] == 'ok'
        finally:
            task.cancel()
            stub_task.cancel()
            stub.close()
            try:
                await task
            except asyncio.CancelledError:
                pass
            try:
                await stub_task
            except asyncio.CancelledError:
                pass

    async def test_health_endpoint_does_not_probe_dispatcher(self):
        '''/health stays purely liveness; dispatcher reachability never gates it.'''
        bot = _make_bot(is_ready=True, is_closed=False)
        # Even with an unreachable dispatcher, /health must return 200
        hs = HealthServer(bot, port=18104, dispatch_http_url='http://127.0.0.1:1')
        task = asyncio.create_task(hs.serve())
        await _wait_for_port(18104)
        try:
            response = await _raw_request(18104, path='/health')
            assert '200 OK' in response
            body = json.loads(response.split('\r\n\r\n', 1)[1])
            assert 'dispatch' not in body
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def test_tcp_probe_invalid_url_returns_false(self):
        '''_tcp_probe returns False when the URL has no host:port.'''
        hs = HealthServer(_make_bot())
        assert await hs._tcp_probe('not-a-url') is False  #pylint:disable=protected-access


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
