'''
RedisPingHealthServer: liveness does not ask Redis, readiness does.

The kubelet restarts a pod after three failed liveness probes. When liveness was the
Redis ping, a Redis that was away for a minute or more restarted the dispatcher, broker,
downloader and search pods for a fault a restart cannot fix, and a ping against a Service
with no ready endpoints hangs rather than refuses, so a probe that merely timed out was
enough. These tests pin the split, including the hang.
'''
import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from discord_core.servers import redis_health_server
from discord_core.servers.redis_health_server import RedisPingHealthServer
from discord_core.tests.servers.test_health_server_base import _probe
from discord_core.utils.loop_health import LOOP_HEALTH


@pytest.fixture(name='loops', autouse=True)
def _loops():
    '''Every test starts and ends with no registered loops, whatever it registers.'''
    LOOP_HEALTH.reset()
    yield LOOP_HEALTH
    LOOP_HEALTH.reset()


def _server(ping):
    redis_manager = MagicMock()
    redis_manager.client.ping = ping
    return RedisPingHealthServer(redis_manager, pod='dispatcher')


async def _never_answers():
    await asyncio.Event().wait()


@pytest.mark.asyncio
async def test_liveness_is_ok_while_redis_is_down():
    '''Redis refusing connections must not fail the probe that restarts the pod.'''
    ping = AsyncMock(side_effect=ConnectionError('redis is away'))

    status, payload = await _probe(_server(ping), b'/health')

    assert (status, payload) == (200, {'status': 'ok'})


@pytest.mark.asyncio
async def test_liveness_never_waits_on_redis():
    '''A Service with no ready endpoints hangs the connect. Liveness must not wait on it at all,
    or the probe times out, which the kubelet counts as a failure.'''
    ping = AsyncMock(side_effect=_never_answers)

    status, _ = await asyncio.wait_for(_probe(_server(ping), b'/health'), timeout=2)

    assert status == 200
    ping.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize('path', [b'/health', b'/', b'/anything'])
async def test_every_non_readiness_path_is_liveness(path):
    '''HealthServerBase treats any path that is not a readiness one as liveness.'''
    status, _ = await _probe(_server(AsyncMock(side_effect=ConnectionError('redis is away'))), path)

    assert status == 200


@pytest.mark.asyncio
@pytest.mark.parametrize('path', [b'/ready', b'/readyz', b'/readiness'])
async def test_readiness_is_ok_when_redis_answers(path):
    ping = AsyncMock(return_value=True)

    status, payload = await _probe(_server(ping), path)

    assert (status, payload) == (200, {'status': 'ok'})
    ping.assert_awaited_once()


@pytest.mark.asyncio
async def test_readiness_is_unavailable_when_redis_raises():
    status, payload = await _probe(_server(AsyncMock(side_effect=ConnectionError('redis is away'))), b'/ready')

    assert (status, payload) == (503, {'status': 'unavailable'})


@pytest.mark.asyncio
async def test_readiness_is_unavailable_when_redis_hangs(mocker):
    '''The bound is what keeps a hung ping from holding the request open past the probe timeout.'''
    mocker.patch.object(redis_health_server, 'READINESS_PING_TIMEOUT_SECONDS', 0.05)

    status, payload = await asyncio.wait_for(
        _probe(_server(AsyncMock(side_effect=_never_answers)), b'/ready'), timeout=2)

    assert (status, payload) == (503, {'status': 'unavailable'})


@pytest.mark.asyncio
async def test_a_stalled_loop_still_fails_liveness(loops):
    '''Ignoring Redis must not make the pod unkillable: a wedged consumer loop is what liveness is for.'''
    health = loops.register('consumer', stale_after_seconds=0)
    health.mark_running()
    await asyncio.sleep(0.01)

    status, payload = await _probe(_server(AsyncMock(return_value=True)), b'/health')

    assert status == 503
    assert payload['loops']['consumer'] != 'ok'
