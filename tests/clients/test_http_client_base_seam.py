'''
HttpClientMixin's seam-check hook: the one line each pod calls.

The hook lives on the mixin rather than on each client because the bot,
downloader and search pods all talk to the same broker. These tests cover the
three ways it declines to do anything, which matter more than the happy path —
a client that silently starts no check looks identical, from outside, to one
whose peer is healthy.

See docs/projects/http-seam-contract.md, acceptance criteria five and six.
'''

import gc

import pytest

from discord_bot.seams.queue_worker.clients.http_broker_client import HttpBrokerClient
from discord_bot.seams.broker.clients.http_client_base import (HttpClientMixin, SEAM_CLIENTS,
                                                  start_seam_checks)
from discord_bot.clients.http_dispatch_client import HttpDispatchClient
from discord_bot.core.utils.common import SeamContractConfig


class _SeamlessClient(HttpClientMixin):
    '''A client on a seam that has no route registry yet.'''

    def __init__(self):
        self._base_url = 'http://peer'
        self._session = None


def test_a_client_with_no_seam_starts_nothing():
    '''A client whose class declares no SEAM must stay silent.

    All five seams now have registries, so no shipped client is in this state --
    but the mixin default still is, and a new client added without a SEAM would
    inherit it. Returning None rather than raising is what lets the pod entry
    points call this unconditionally, instead of every caller having to know
    which clients are wired.
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


@pytest.mark.asyncio(loop_scope='session')
async def test_start_seam_checks_starts_every_client_built():
    """The fan-out the pods call once, with no list to get wrong.

    Two clients on different seams, because the bug this guards against is a
    loop that starts the first and returns. Neither is passed in: both enrolled
    themselves when they were handed a config, which is the property that makes
    a client impossible to leave unchecked.
    """
    config = SeamContractConfig(interval_seconds=60)
    SEAM_CLIENTS.reset()
    broker = HttpBrokerClient('http://broker:8081', seam_contract=config)
    dispatch = HttpDispatchClient('http://dispatcher:8082', seam_contract=config)

    start_seam_checks()
    try:
        assert broker.seam_check is not None
        assert dispatch.seam_check is not None
        assert {broker.seam_check.seam, dispatch.seam_check.seam} == {'broker', 'dispatch'}
    finally:
        await broker.close()
        await dispatch.close()


@pytest.mark.asyncio(loop_scope='session')
async def test_a_client_built_without_a_config_never_enrols():
    """A configured absence, not a bug.

    The broker pod's dispatch client is None when general.dispatch_http_url is
    unset, and a client can also be built with no seam_contract at all -- both
    supported. Enrolment keyed on the config means those simply never appear,
    so the fan-out needs no guard per client.
    """
    SEAM_CLIENTS.reset()
    unconfigured = HttpBrokerClient('http://broker:8081')
    assert not SEAM_CLIENTS.clients

    start_seam_checks()
    try:
        assert unconfigured.seam_check is None
    finally:
        await unconfigured.close()


@pytest.mark.asyncio(loop_scope='session')
async def test_closing_a_client_withdraws_it():
    """A closed client must not be restarted by a later fan-out.

    The bot calls start_seam_checks from two places, so a client closed between
    them would otherwise get a fresh probe task against a closed session.
    """
    SEAM_CLIENTS.reset()
    client = HttpBrokerClient('http://broker:8081',
                              seam_contract=SeamContractConfig(interval_seconds=60))
    assert len(SEAM_CLIENTS.clients) == 1
    await client.close()
    assert not SEAM_CLIENTS.clients

    start_seam_checks()
    assert client.seam_check is None


def test_enrolment_does_not_keep_a_client_alive():
    """Weak references, because the suite builds hundreds of these.

    A registry that kept its own strong reference would turn every client ever
    constructed into a permanent one, and start_seam_checks would walk the
    wreckage of every earlier test.
    """
    SEAM_CLIENTS.reset()
    client = HttpBrokerClient('http://broker:8081',
                              seam_contract=SeamContractConfig(interval_seconds=60))
    assert len(SEAM_CLIENTS.clients) == 1
    del client
    gc.collect()
    assert not SEAM_CLIENTS.clients


class _ConfigPeek(HttpBrokerClient):
    """Reads the descriptor from inside a client class, as the real ones do."""

    @property
    def config(self):
        return self._seam_contract_config

    @classmethod
    def unbound_config(cls):
        """What the descriptor yields off the CLASS, with no instance."""
        return cls._seam_contract_config


def test_the_config_still_reads_back_normally():
    """The descriptor must stay invisible to everything except enrolment.

    Five concrete clients assign this attribute and read it back through
    start_seam_check. If the descriptor changed what a read returns, every one
    of them would break, so the read path is pinned here rather than left to be
    covered incidentally.
    """
    SEAM_CLIENTS.reset()
    config = SeamContractConfig(interval_seconds=60)
    assert _ConfigPeek('http://broker:8081', seam_contract=config).config is config
    assert _ConfigPeek('http://broker:8081').config is None
    assert _ConfigPeek.unbound_config() is None
