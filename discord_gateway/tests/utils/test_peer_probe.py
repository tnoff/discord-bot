"""Tests for the shared peer TCP probe and the startup wait."""
import asyncio
import logging
from unittest.mock import AsyncMock

import pytest

from discord_gateway.utils import peer_probe
from discord_gateway.utils.peer_probe import tcp_probe, wait_for_peers

LOGGER = logging.getLogger('peer-probe-test')


@pytest.mark.asyncio
async def test_tcp_probe_true_for_a_listening_port():
    server = await asyncio.start_server(lambda r, w: w.close(), '127.0.0.1', 0)
    port = server.sockets[0].getsockname()[1]
    try:
        assert await tcp_probe(f'http://127.0.0.1:{port}')
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
@pytest.mark.parametrize('url', [None, '', 'http://no-port-host', 'http://127.0.0.1:1'])
async def test_tcp_probe_false_for_missing_or_closed(url):
    assert not await tcp_probe(url)


@pytest.mark.asyncio
async def test_tcp_probe_false_on_timeout(mocker):
    async def hang(*_args):
        await asyncio.sleep(10)
    mocker.patch.object(peer_probe.asyncio, 'open_connection', hang)
    mocker.patch.object(peer_probe, 'PROBE_TIMEOUT_SECONDS', 0.01)
    assert not await tcp_probe('http://127.0.0.1:9')


@pytest.mark.asyncio
async def test_wait_returns_immediately_when_all_peers_are_up(mocker):
    probe = mocker.patch.object(peer_probe, 'tcp_probe', AsyncMock(return_value=True))
    assert await wait_for_peers({'a': 'http://a:1', 'b': None}, LOGGER, interval=0)
    probe.assert_awaited_once_with('http://a:1')


@pytest.mark.asyncio
async def test_wait_holds_until_the_late_peer_appears(mocker, caplog):
    late_calls = []

    async def probe(url):
        if url != 'http://late:1':
            return True
        late_calls.append(url)
        return len(late_calls) >= 3
    mocker.patch.object(peer_probe, 'tcp_probe', probe)
    with caplog.at_level(logging.INFO, logger=LOGGER.name):
        assert await wait_for_peers({'up': 'http://up:1', 'late': 'http://late:1'}, LOGGER, interval=0)
    waiting = [r for r in caplog.records if 'Waiting for peers' in r.getMessage()]
    assert len(waiting) == 1
    assert 'late' in waiting[0].getMessage()


@pytest.mark.asyncio
async def test_wait_gives_up_after_the_timeout_and_names_the_peer(mocker, caplog):
    mocker.patch.object(peer_probe, 'tcp_probe', AsyncMock(return_value=False))
    with caplog.at_level(logging.WARNING, logger=LOGGER.name):
        assert not await wait_for_peers({'gone': 'http://gone:1'}, LOGGER, timeout=0, interval=0)
    assert 'gone' in caplog.text
