'''Tests for BrokerHealthServer — the broker pod's Redis-backed /health endpoint.'''
from unittest.mock import AsyncMock, MagicMock

import pytest

from discord_bot.servers.broker_health_server import BrokerHealthServer


def _server(ping):
    redis_manager = MagicMock()
    redis_manager.client.ping = ping
    return BrokerHealthServer(redis_manager, port=19400)


@pytest.mark.asyncio
async def test_check_ok_when_redis_pings():
    server = _server(AsyncMock(return_value=True))
    ok, payload = await server._check()  # pylint: disable=protected-access
    assert ok is True
    assert payload == {}


@pytest.mark.asyncio
async def test_check_unavailable_when_redis_raises():
    server = _server(AsyncMock(side_effect=ConnectionError('redis down')))
    ok, payload = await server._check()  # pylint: disable=protected-access
    assert ok is False
    assert payload == {}


@pytest.mark.asyncio
async def test_check_counts_ok_outcome(mocker):
    counter = mocker.patch('discord_bot.servers.broker_health_server._READY_CHECK_COUNTER')
    server = _server(AsyncMock(return_value=True))
    ok, _ = await server._check()  # pylint: disable=protected-access
    assert ok is True
    counter.add.assert_called_once_with(1, {'outcome': 'ok'})


@pytest.mark.asyncio
async def test_check_counts_unavailable_outcome(mocker):
    counter = mocker.patch('discord_bot.servers.broker_health_server._READY_CHECK_COUNTER')
    server = _server(AsyncMock(side_effect=ConnectionError('redis down')))
    ok, _ = await server._check()  # pylint: disable=protected-access
    assert ok is False
    counter.add.assert_called_once_with(1, {'outcome': 'unavailable'})
