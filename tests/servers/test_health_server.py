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
from discord_bot.servers.health_server import HealthServer, DispatchHealthServer


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
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass
            return
        except OSError:
            if asyncio.get_event_loop().time() >= deadline:
                raise
            await asyncio.sleep(0.01)


async def _raw_request(port: int) -> str:
    """Open a raw TCP connection and send a minimal HTTP GET, return the full response."""
    reader, writer = await asyncio.open_connection('127.0.0.1', port)
    writer.write(b'GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n')
    await writer.drain()
    response = b''
    try:
        response = await asyncio.wait_for(reader.read(4096), timeout=3)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except OSError:
            pass
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


def _make_db_engine(fail=False):
    """Return a mock AsyncEngine whose connect() context manager succeeds or raises."""
    engine = MagicMock()
    if fail:
        engine.connect.return_value = AsyncMock(
            __aenter__=AsyncMock(side_effect=Exception('db down')),
            __aexit__=AsyncMock(return_value=False),
        )
    else:
        mock_conn = AsyncMock()
        engine.connect.return_value = AsyncMock(
            __aenter__=AsyncMock(return_value=mock_conn),
            __aexit__=AsyncMock(return_value=False),
        )
    return engine


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

    def test_init_with_db_engine(self):
        """db_engine is stored when provided."""
        bot = _make_bot()
        engine = _make_db_engine()
        hs = HealthServer(bot, db_engine=engine)
        assert hs._db_engine is engine  #pylint:disable=protected-access

    def test_init_without_db_engine(self):
        """db_engine defaults to None."""
        hs = HealthServer(_make_bot())
        assert hs._db_engine is None  #pylint:disable=protected-access


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

    async def test_health_no_db_engine_no_db_key_in_response(self):
        """When no db_engine, response body has no 'db' key."""

        bot = _make_bot(is_ready=True, is_closed=False)
        hs = HealthServer(bot, port=18083)
        task = asyncio.create_task(hs.serve())
        await _wait_for_port(18083)
        try:
            response = await _raw_request(18083)
            body = json.loads(response.split('\r\n\r\n', 1)[1])
            assert 'db' not in body
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def test_health_db_ok(self):
        """Returns 200 with db:ok when bot is ready and DB ping succeeds."""

        bot = _make_bot(is_ready=True, is_closed=False)
        hs = HealthServer(bot, port=18084, db_engine=_make_db_engine(fail=False))
        task = asyncio.create_task(hs.serve())
        await _wait_for_port(18084)
        try:
            response = await _raw_request(18084)
            assert '200 OK' in response
            body = json.loads(response.split('\r\n\r\n', 1)[1])
            assert body['status'] == 'ok'
            assert body['db'] == 'ok'
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def test_health_db_unavailable(self):
        """Returns 503 with db:unavailable when DB ping fails."""

        bot = _make_bot(is_ready=True, is_closed=False)
        hs = HealthServer(bot, port=18085, db_engine=_make_db_engine(fail=True))
        task = asyncio.create_task(hs.serve())
        await _wait_for_port(18085)
        try:
            response = await _raw_request(18085)
            assert '503' in response
            body = json.loads(response.split('\r\n\r\n', 1)[1])
            assert body['status'] == 'unavailable'
            assert body['db'] == 'unavailable'
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def test_health_bot_not_ready_db_ok_returns_503(self):
        """Returns 503 when bot is not ready even if DB is healthy."""

        bot = _make_bot(is_ready=False, is_closed=False)
        hs = HealthServer(bot, port=18086, db_engine=_make_db_engine(fail=False))
        task = asyncio.create_task(hs.serve())
        await _wait_for_port(18086)
        try:
            response = await _raw_request(18086)
            assert '503' in response
            body = json.loads(response.split('\r\n\r\n', 1)[1])
            assert body['status'] == 'unavailable'
            assert body['db'] == 'ok'
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def test_db_ping_returns_true_on_success(self):
        """_db_ping returns True when engine connects successfully."""
        hs = HealthServer(_make_bot(), db_engine=_make_db_engine(fail=False))
        assert await hs._db_ping() is True  #pylint:disable=protected-access

    async def test_db_ping_returns_false_on_failure(self):
        """_db_ping returns False when engine raises."""
        hs = HealthServer(_make_bot(), db_engine=_make_db_engine(fail=True))
        assert await hs._db_ping() is False  #pylint:disable=protected-access

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
        fake_redis = fakeredis.aioredis.FakeRedis()
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
