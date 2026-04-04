"""
Tests for the HTTP health server.
"""
import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from discord_bot.servers.health_server import HealthServer


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
