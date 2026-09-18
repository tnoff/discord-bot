'''
pod_ready_check: recorded once per probe, for every pod, after loop health.

Two properties, and the second is a bug fix rather than a refactor. The broker
and db health servers used to count inside their own `_check`, which runs BEFORE
`_apply_loop_health` folds in background-loop state -- so a stalled consumer loop
made the endpoint return 503 while the metric still said 'ok'. The other four
pods counted nothing at all, which is why only two of six appeared in Mimir.
'''
import asyncio
import json

import pytest

from discord_bot.servers.health_server_base import HealthServerBase
from discord_bot.utils.otel import AttributeNaming


class _AlwaysOk(HealthServerBase):
    '''Minimal health server whose dependency is always reachable.'''

    async def _check(self):
        return True, {}


async def _probe(server, path=b'/health'):
    '''Drive one request through _handle and return (status, payload).'''
    reader = asyncio.StreamReader()
    reader.feed_data(b'GET ' + path + b' HTTP/1.1\r\n\r\n')
    reader.feed_eof()
    written = []

    class _Writer:
        def write(self, data):
            written.append(data)

        async def drain(self):
            return None

        def close(self):
            return None

        async def wait_closed(self):
            return None

    await server._handle(reader, _Writer())  # pylint: disable=protected-access
    raw = b''.join(written)
    status = int(raw.split(b' ', 2)[1])
    body = raw.split(b'\r\n\r\n', 1)[1]
    return status, json.loads(body)


@pytest.mark.asyncio
async def test_every_pod_records_its_own_name(mocker):
    '''
    The pod label comes from the server, so no pod reports anonymously.

    RedisPingHealthServer fronts BOTH the downloader and the search pod, which is
    why `pod` is a constructor argument rather than a class attribute -- a shared
    default would file two pods under one series, which is exactly the collision
    the naming scheme exists to prevent.
    '''
    counter = mocker.patch('discord_bot.servers.health_server_base._POD_READY_CHECK_COUNTER')
    for pod in ('downloader', 'search'):
        counter.reset_mock()
        await _probe(_AlwaysOk(port=0, bind_address='127.0.0.1', pod=pod))
        assert counter.add.call_args.args[1][AttributeNaming.POD.value] == pod


@pytest.mark.asyncio
async def test_a_stalled_loop_makes_the_metric_agree_with_the_status(mocker):
    '''
    The metric and the HTTP status can no longer disagree.

    This is the case the old placement got wrong: `_check` says the dependency is
    fine, a registered loop is stalled, the endpoint 503s -- and the counter had
    already recorded 'ok' because it ran first.
    '''
    counter = mocker.patch('discord_bot.servers.health_server_base._POD_READY_CHECK_COUNTER')
    mocker.patch.object(HealthServerBase, '_apply_loop_health',
                        staticmethod(lambda ok, extra: (False, extra)))
    status, _ = await _probe(_AlwaysOk(port=0, bind_address='127.0.0.1', pod='broker'))
    assert status == 503
    assert counter.add.call_args.args[1][AttributeNaming.OUTCOME.value] == 'unavailable', (
        'the metric reported healthy while the endpoint reported 503'
    )


@pytest.mark.asyncio
async def test_one_observation_per_probe(mocker):
    '''Not one per _check plus another for the readiness path.'''
    counter = mocker.patch('discord_bot.servers.health_server_base._POD_READY_CHECK_COUNTER')
    await _probe(_AlwaysOk(port=0, bind_address='127.0.0.1', pod='bot'), path=b'/ready')
    assert counter.add.call_count == 1
