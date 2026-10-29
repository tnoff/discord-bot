'''Tests for BrokerHealthServer: liveness ignores Redis, readiness pings it.'''
from unittest.mock import AsyncMock, MagicMock

import pytest

from discord_broker.servers.broker_health_server import BrokerHealthServer


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
async def test_liveness_stays_ok_when_redis_raises():
    '''A broker whose Redis is away is not restarted for it: a restart cannot bring Redis back.'''
    server = _server(AsyncMock(side_effect=ConnectionError('redis down')))
    ok, payload = await getattr(server, '_check')()
    assert ok is True
    assert payload == {}


@pytest.mark.asyncio
async def test_readiness_unavailable_when_redis_raises():
    server = _server(AsyncMock(side_effect=ConnectionError('redis down')))
    ok, payload = await getattr(server, '_readiness_check')()
    assert ok is False
    assert payload == {}
