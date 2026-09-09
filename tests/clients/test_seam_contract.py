'''
The client-side seam check and its clock.

Acceptance criteria five and six of docs/projects/http-seam-contract.md. The
two incidents in docs/findings/2026-07-31-discord-search-seam-deploy-skew.md are
the same seam failing two different ways, and the design only works if the check
tells them apart: a ~20-second rolling-update skew must stay silent, and a
~9.5-hour steady-state pin skew must not.
'''
import asyncio

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from discord_bot.routes import broker as broker_routes
from discord_bot.routes import contract
from discord_bot.clients.seam_contract import (DEFAULT_GRACE_SECONDS, PeerContractStatus,
                                               SeamContractCheck)


class _Clock:
    '''Manual monotonic clock, so the grace window is tested without sleeping.'''

    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def _peer_app(served, contract_status=200):
    '''A peer advertising `served`, or 404ing the contract route entirely.'''
    app = web.Application()

    async def handler(_request):
        if contract_status != 200:
            return web.Response(status=contract_status)
        return web.json_response(contract.encode(served))

    app.router.add_route(contract.CONTRACT_ROUTE.method,
                         contract.CONTRACT_ROUTE.template, handler)
    return app


async def _check_against(app, routes_called, clock, grace=DEFAULT_GRACE_SECONDS):
    server = TestServer(app)
    await server.start_server()
    session = aiohttp.ClientSession()
    check = SeamContractCheck('broker', str(server.make_url('')), routes_called,
                              lambda: session, grace_seconds=grace, clock=clock)
    return check, server, session


async def _close(server, session):
    await session.close()
    await server.close()


CALLED = (broker_routes.NEXT_SEARCH_RESULT, broker_routes.REGISTER_REQUEST)
ENTRIES = {(r.method, r.template) for r in CALLED}


@pytest.mark.asyncio(loop_scope='session')
async def test_a_peer_serving_everything_is_quiet():
    clock = _Clock()
    check, server, session = await _check_against(_peer_app(ENTRIES), CALLED, clock)
    try:
        assert await check.probe() is PeerContractStatus.OK
        clock.advance(DEFAULT_GRACE_SECONDS * 10)
        assert check.breached is False
        assert check.observations()[0].value == 0
    finally:
        await _close(server, session)


@pytest.mark.asyncio(loop_scope='session')
async def test_a_peer_serving_a_superset_is_quiet():
    '''The normal steady state: pods update at different rates by design.

    Criterion five is subset, not equality — a peer with routes this client
    never calls must not produce a signal, or the check is muted within a week.
    '''
    clock = _Clock()
    superset = ENTRIES | {('POST', '/some/route/this/client/never/calls')}
    check, server, session = await _check_against(_peer_app(superset), CALLED, clock)
    try:
        assert await check.probe() is PeerContractStatus.OK
        assert check.missing_routes == set()
    finally:
        await _close(server, session)


@pytest.mark.asyncio(loop_scope='session')
async def test_a_short_roll_never_breaches():
    '''2026-07-31, the ~20-second skew. Real, self-resolving, must stay silent.'''
    clock = _Clock()
    without = ENTRIES - {(broker_routes.NEXT_SEARCH_RESULT.method,
                          broker_routes.NEXT_SEARCH_RESULT.template)}
    check, server, session = await _check_against(_peer_app(without), CALLED, clock)
    try:
        assert await check.probe() is PeerContractStatus.ROUTES_MISSING
        clock.advance(20)
        assert check.breached is False
        assert check.observations()[0].value == 0
    finally:
        await _close(server, session)


@pytest.mark.asyncio(loop_scope='session')
async def test_a_pin_skew_breaches_and_is_labelled():
    '''2026-08-03, the ~9.5-hour pin skew that nothing alerted on.'''
    clock = _Clock()
    without = ENTRIES - {(broker_routes.NEXT_SEARCH_RESULT.method,
                          broker_routes.NEXT_SEARCH_RESULT.template)}
    check, server, session = await _check_against(_peer_app(without), CALLED, clock)
    try:
        await check.probe()
        clock.advance(9.5 * 3600)
        assert check.breached is True
        observation = check.observations()[0]
        assert observation.value == 1
        assert observation.attributes['seam'] == 'broker'
        assert observation.attributes['seam_contract_reason'] == 'routes_missing'
        assert check.missing_routes == {(broker_routes.NEXT_SEARCH_RESULT.method,
                                         broker_routes.NEXT_SEARCH_RESULT.template)}
    finally:
        await _close(server, session)


@pytest.mark.asyncio(loop_scope='session')
async def test_recovery_clears_the_clock():
    '''A peer that catches up resets the deadline rather than staying latched.'''
    clock = _Clock()
    without = ENTRIES - {(broker_routes.REGISTER_REQUEST.method,
                          broker_routes.REGISTER_REQUEST.template)}
    app = _peer_app(without)
    check, server, session = await _check_against(app, CALLED, clock)
    try:
        await check.probe()
        clock.advance(DEFAULT_GRACE_SECONDS * 2)
        assert check.breached is True
        without.update(ENTRIES)
        assert await check.probe() is PeerContractStatus.OK
        assert check.breached is False
    finally:
        await _close(server, session)


@pytest.mark.asyncio(loop_scope='session')
async def test_a_peer_without_the_endpoint_is_its_own_reason():
    '''Rollout of the advertisement itself: every peer 404s until it upgrades.

    Reported separately from routes_missing so an operator can alert on a real
    seam gap first and turn this one on once the fleet has rolled.
    '''
    clock = _Clock()
    check, server, session = await _check_against(
        _peer_app(ENTRIES, contract_status=404), CALLED, clock)
    try:
        assert await check.probe() is PeerContractStatus.NO_CONTRACT_ENDPOINT
        clock.advance(DEFAULT_GRACE_SECONDS * 2)
        assert check.observations()[0].attributes['seam_contract_reason'] == \
            'no_contract_endpoint'
    finally:
        await _close(server, session)


@pytest.mark.asyncio(loop_scope='session')
async def test_an_unreachable_peer_leaves_the_clock_alone():
    '''A peer being down is a different fault, with its own heartbeat alert.

    It must not CLEAR a running mismatch clock — a flapping peer would reset the
    deadline forever while a real mismatch sat underneath — and it must not
    START one either.
    '''
    clock = _Clock()
    without = ENTRIES - {(broker_routes.REGISTER_REQUEST.method,
                          broker_routes.REGISTER_REQUEST.template)}
    check, server, session = await _check_against(_peer_app(without), CALLED, clock)
    await check.probe()
    clock.advance(DEFAULT_GRACE_SECONDS * 2)
    assert check.breached is True
    await server.close()

    # Peer is now gone entirely; the client's own session is still healthy.
    assert await check.probe() is PeerContractStatus.UNREACHABLE
    assert check.breached is True, 'an unreachable peer must not clear the clock'
    assert not check.observations(), 'and must not report a verdict it cannot support'
    await session.close()


@pytest.mark.asyncio(loop_scope='session')
async def test_it_emits_nothing_before_a_first_answer():
    '''A permanently-0 series for a check that never ran reads as "verified
    healthy" on a dashboard, which is the opposite of the truth.'''
    check = SeamContractCheck('broker', 'http://127.0.0.1:1', CALLED, lambda: None,
                              clock=_Clock())
    assert check.status is None
    assert not check.observations()


@pytest.mark.asyncio(loop_scope='session')
async def test_probe_never_raises_at_a_dead_address():
    '''The startup path must survive a peer that is simply not up yet.'''
    session = aiohttp.ClientSession()
    try:
        check = SeamContractCheck('broker', 'http://127.0.0.1:1', CALLED,
                                  lambda: session, clock=_Clock())
        assert await check.probe() is PeerContractStatus.UNREACHABLE
    finally:
        await session.close()


@pytest.mark.asyncio(loop_scope='session')
async def test_run_keeps_probing_until_cancelled():
    clock = _Clock()
    check, server, session = await _check_against(_peer_app(ENTRIES), CALLED, clock)
    task = asyncio.create_task(check.run(interval=0.001))
    try:
        async with asyncio.timeout(5):
            while check.status is None:
                await asyncio.sleep(0.001)
        assert check.status is PeerContractStatus.OK
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await _close(server, session)


@pytest.mark.asyncio(loop_scope='session')
async def test_a_session_closed_mid_probe_is_survivable():
    '''Teardown race: the owning client closes while a probe is in flight.

    aiohttp raises RuntimeError rather than a ClientError for this, so it needs
    naming explicitly — it is a normal shutdown race, not a seam fault, and it
    must not propagate out of a background task.
    '''
    session = aiohttp.ClientSession()
    await session.close()
    check = SeamContractCheck('broker', 'http://127.0.0.1:1', CALLED,
                              lambda: session, clock=_Clock())
    assert await check.probe() is PeerContractStatus.UNREACHABLE


@pytest.mark.asyncio(loop_scope='session')
async def test_a_breach_stays_breached_across_probes():
    """Re-probing past the grace escalates rather than restarting the clock.

    The clock is set once, on the first mismatch, so a check that probes every
    60 seconds through a 9.5-hour skew must not keep resetting its own deadline
    — which is how a bounded tolerance quietly becomes an unbounded one.
    """
    clock = _Clock()
    without = ENTRIES - {(broker_routes.NEXT_SEARCH_RESULT.method,
                          broker_routes.NEXT_SEARCH_RESULT.template)}
    check, server, session = await _check_against(_peer_app(without), CALLED, clock)
    try:
        await check.probe()
        for _ in range(5):
            clock.advance(DEFAULT_GRACE_SECONDS)
            assert await check.probe() is PeerContractStatus.ROUTES_MISSING
        assert check.breached is True
        assert check.observations()[0].value == 1
    finally:
        await _close(server, session)


def test_register_gauge_publishes_the_breach_metric():
    """The gauge is what the docker-apps alert rule reads."""
    published = {}

    class _MeterProvider:
        @staticmethod
        def create_observable_gauge(name, callbacks, unit, description):
            published.update(name=name, callbacks=callbacks,
                             unit=unit, description=description)

    check = SeamContractCheck('broker', 'http://peer', CALLED, lambda: None,
                              clock=_Clock())
    check.register_gauge(_MeterProvider())
    assert published['name'] == 'seam_contract_breach'
    assert published['callbacks'] == [check.observations]
