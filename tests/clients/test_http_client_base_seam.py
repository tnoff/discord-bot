'''
HttpClientMixin's seam-check hook: the one line each pod calls.

The hook lives on the mixin rather than on each client because the bot,
downloader and search pods all talk to the same broker. These tests cover the
three ways it declines to do anything, which matter more than the happy path —
a client that silently starts no check looks identical, from outside, to one
whose peer is healthy.

See docs/projects/http-seam-contract.md, acceptance criteria five and six.
'''

import pytest

from discord_bot.clients.http_broker_client import HttpBrokerClient
from discord_bot.clients.http_client_base import HttpClientMixin
from discord_bot.utils.common import SeamContractConfig


class _SeamlessClient(HttpClientMixin):
    '''A client on a seam that has no route registry yet.'''

    def __init__(self):
        self._base_url = 'http://peer'
        self._session = None


def test_a_client_with_no_seam_starts_nothing():
    '''Four of the five seams have no registry yet, and must stay silent.

    Returning None rather than raising is what lets the shared worker-pod entry
    point call this unconditionally, instead of every caller having to know which
    clients are wired.
    '''
    assert _SeamlessClient().start_seam_check() is None


def test_a_client_with_no_config_starts_nothing():
    '''Constructed without a seam_contract block — as every test double is.'''
    client = HttpBrokerClient('http://broker:8081')
    assert client.SEAM == 'broker'
    assert client.ROUTES_CALLED
    assert client.start_seam_check() is None


@pytest.mark.asyncio(loop_scope='session')
async def test_close_stops_a_running_check():
    '''The teardown path, and the reason close() was overridden at all.

    A probe loop outliving its client would keep polling a peer with a closed
    session for the life of the process.
    '''
    client = HttpBrokerClient('http://broker:8081',
                              seam_contract=SeamContractConfig(interval_seconds=60))
    check = client.start_seam_check()
    assert check is not None
    assert check.probe_task is not None

    await client.close()
    assert check.probe_task is None


@pytest.mark.asyncio(loop_scope='session')
async def test_start_is_idempotent_through_the_client():
    client = HttpBrokerClient('http://broker:8081',
                              seam_contract=SeamContractConfig(interval_seconds=60))
    try:
        first = client.start_seam_check()
        again = client.start_seam_check()
        assert first is again
        assert first.probe_task is again.probe_task
    finally:
        await client.close()


@pytest.mark.asyncio(loop_scope='session')
async def test_close_without_a_check_is_safe():
    client = HttpBrokerClient('http://broker:8081')
    await client.close()
    assert client.start_seam_check() is None
