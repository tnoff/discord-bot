'''
Rolling-deploy version skew between the bot and the broker.

Reproduces the 2026-07-31 incident (docs findings/2026-07-31-discord-search-seam-deploy-skew):
the bot came up on a build whose ``process_search_results`` loop calls
``GET /search-results/next`` while a pre-MR2 broker pod was still the only
endpoint behind the Service, so every call 404'd. The loop gave up after 5
consecutive errors and the search-result consumer stayed dead for the life of
the pod — ``!play`` posted, searched, and never downloaded.

The existing round-trip test in test_broker_client.py only ever talks to a
*same-build* server, which is exactly why this shipped green. These tests drive a
real ``BrokerHttpServer`` whose ``build_app()`` has had the ``/search-results*``
routes removed, standing in for the older peer.
'''
import asyncio
import logging

import pytest
from aiohttp import web
from aiohttp.client_exceptions import ClientResponseError
from aiohttp.test_utils import TestClient, TestServer

from discord_bot.clients.broker_client import HttpBrokerClient
from discord_bot.servers.broker_server import BrokerHttpServer
from discord_bot.types.search_resolution import SearchResolution
from discord_bot.utils.common import return_loop_runner
from discord_bot.utils.loop_health import LoopHealth
from discord_bot.workers.asyncio_broker import AsyncioBroker as MediaBroker
from discord_bot.workers.asyncio_queues import AsyncioSearchResultQueue

from tests.helpers import fake_bot_yielder, fake_source_dict, generate_fake_context


def _make_broker():
    return MediaBroker()


def _make_request():
    return fake_source_dict(generate_fake_context())


def _pre_mr2_app(server: BrokerHttpServer) -> web.Application:
    '''Build the broker app as it existed before the search-result seam landed.

    Rebuilds from the real ``build_app()`` and drops the two routes MR2 added, so
    the stand-in stays honest as the rest of the broker's routes evolve — every
    other route behaves exactly as the current build does.
    '''
    live = server.build_app()
    app = web.Application(middlewares=live.middlewares)
    for route in live.router.routes():
        if route.resource.canonical.startswith('/search-results'):
            continue
        app.router.add_route(route.method, route.resource.canonical, route.handler)
    return app


@pytest.mark.asyncio(loop_scope="session")
class TestPreMr2BrokerRoutes:
    '''The stand-in really is missing the routes (guards the test itself).'''

    async def test_old_broker_404s_the_search_result_routes(self):
        server = BrokerHttpServer(_make_broker())
        async with TestClient(TestServer(_pre_mr2_app(server))) as tc:
            assert (await tc.get('/search-results/next')).status == 404
            assert (await tc.post('/search-results', json={})).status == 404

    async def test_current_broker_serves_them(self):
        # 204 (empty queue) and 422 (empty body) — never 404. Confirms the two
        # apps differ only in the routes under test.
        server = BrokerHttpServer(_make_broker())
        async with TestClient(TestServer(server.build_app())) as tc:
            assert (await tc.get('/search-results/next')).status == 204
            assert (await tc.post('/search-results', json={})).status == 422


@pytest.mark.asyncio(loop_scope="session")
class TestSeamClientToleratesSkew:
    '''A 404 from an un-upgraded peer means "not there yet", not "fatal".'''

    async def test_next_search_result_treats_404_as_empty(self):
        server = BrokerHttpServer(_make_broker())
        async with TestClient(TestServer(_pre_mr2_app(server))) as tc:
            hc = HttpBrokerClient(str(tc.make_url('')), session=tc.session)
            assert await hc.next_search_result() is None

    async def test_register_search_result_treats_404_as_a_noop(self):
        # The producer half of the seam: the bot may also be POSTing resolutions
        # to a broker that predates the route.
        server = BrokerHttpServer(_make_broker())
        async with TestClient(TestServer(_pre_mr2_app(server))) as tc:
            hc = HttpBrokerClient(str(tc.make_url('')), session=tc.session)
            await hc.register_search_result(
                SearchResolution(media_request=_make_request(), span_context={'t': 1}))

    async def test_next_result_treats_404_as_empty(self):
        # Same tolerance on the download seam, for symmetry.
        server = BrokerHttpServer(_make_broker())
        app = server.build_app()
        stripped = web.Application(middlewares=app.middlewares)
        for route in app.router.routes():
            if route.resource.canonical == '/results/next':
                continue
            stripped.router.add_route(route.method, route.resource.canonical, route.handler)
        async with TestClient(TestServer(stripped)) as tc:
            hc = HttpBrokerClient(str(tc.make_url('')), session=tc.session)
            assert await hc.next_result() is None

    async def test_register_search_result_still_raises_on_other_4xx(self):
        # 404 means "route not there yet"; any other client error is real and
        # must not be swallowed by the skew tolerance.
        async def _bad_request(_request):
            raise web.HTTPBadRequest()
        app = web.Application()
        app.router.add_post('/search-results', _bad_request)
        async with TestClient(TestServer(app)) as tc:
            hc = HttpBrokerClient(str(tc.make_url('')), session=tc.session)
            with pytest.raises(ClientResponseError):
                await hc.register_search_result(
                    SearchResolution(media_request=_make_request(), span_context={'t': 1}))

    async def test_a_real_server_error_still_raises(self):
        # 404 tolerance must not swallow genuine failures.
        async def _boom(_request):
            raise web.HTTPInternalServerError()
        app = web.Application()
        app.router.add_get('/search-results/next', _boom)
        async with TestClient(TestServer(app)) as tc:
            hc = HttpBrokerClient(str(tc.make_url('')), session=tc.session)
            with pytest.raises(Exception):  #pylint:disable=broad-exception-caught
                await hc.next_search_result()


@pytest.mark.asyncio(loop_scope="session")
class TestSearchResultLoopSurvivesSkew:
    '''End to end: the consumer loop through a skew and out the other side.'''

    async def test_loop_survives_the_skew_and_delivers_once_the_peer_upgrades(self):
        '''The incident, replayed: (a) the loop does not exit, (b) it stays
        healthy — a ~20 s skew is a blip, not an alert — and (c) it delivers as
        soon as the upgraded broker is serving.'''
        broker = _make_broker()
        search_queue = AsyncioSearchResultQueue()
        server = BrokerHttpServer(broker, search_result_queue=search_queue)
        delivered = []
        fake_bot = fake_bot_yielder()()
        health = LoopHealth('process_search_results', stale_after_seconds=60)

        # Both peers stay up for the whole test — the Service endpoint the client
        # resolves to is what changes, exactly as it does mid-rolling-update.
        async with TestClient(TestServer(_pre_mr2_app(server))) as old_peer, \
                TestClient(TestServer(server.build_app())) as new_peer:
            # Phase 1: the Service still points at the pre-MR2 pod.
            client = HttpBrokerClient(str(old_peer.make_url('')), session=old_peer.session)

            async def consume():
                resolution = await client.next_search_result()
                if resolution is not None:
                    delivered.append(resolution)
                await asyncio.sleep(0)

            runner = return_loop_runner(consume, fake_bot, logging, health=health)
            task = asyncio.get_event_loop().create_task(runner())
            await asyncio.sleep(0.1)

            # (a) still running — the old build had exited by now (5 errors, ~5s
            # in prod, immediately here) and never came back.
            assert not task.done()
            # (b) and not alarming: a skew the client absorbs isn't even an error.
            assert health.is_healthy
            assert health.consecutive_errors == 0
            assert not delivered

            # Phase 2: the old pod drops out; only the upgraded broker serves.
            client._base_url = str(new_peer.make_url('')).rstrip('/')  #pylint:disable=protected-access
            client._session = new_peer.session  #pylint:disable=protected-access
            await client.register_search_result(
                SearchResolution(media_request=_make_request(), span_context={'t': 1}))

            # (c) the same loop task — never restarted — picks it up.
            for _ in range(100):
                if delivered:
                    break
                await asyncio.sleep(0.02)
            fake_bot.bot_closed = True
            await asyncio.wait_for(task, timeout=2)

        assert len(delivered) == 1
        assert health.is_healthy

    async def test_a_persistently_broken_peer_still_reports_unhealthy(self):
        '''The other half of the contract: tolerating skew must not hide a real
        outage. A broker returning 500s keeps the loop alive but drives its
        health — and so the heartbeat gauge and the k8s probe — to unhealthy.'''
        clock_now = [1000.0]
        health = LoopHealth('process_search_results', stale_after_seconds=60,
                            time_func=lambda: clock_now[0])
        fake_bot = fake_bot_yielder()()

        async def _boom(_request):
            raise web.HTTPInternalServerError()

        app = web.Application()
        app.router.add_get('/search-results/next', _boom)
        async with TestClient(TestServer(app)) as tc:
            client = HttpBrokerClient(str(tc.make_url('')), session=tc.session)
            calls = [0]

            async def consume():
                calls[0] += 1
                clock_now[0] += 10  # each failed attempt takes the loop further from a success
                await client.next_search_result()

            runner = return_loop_runner(consume, fake_bot, logging, health=health)
            with pytest.MonkeyPatch.context() as patcher:
                # Collapse the retry backoff so the outage plays out immediately.
                patcher.setattr('discord_bot.utils.common.sleep', lambda _seconds: asyncio.sleep(0))
                task = asyncio.get_event_loop().create_task(runner())
                # Checked while the outage is still in progress: the loop is
                # alive (so it can recover) but reporting unhealthy.
                for _ in range(200):
                    if calls[0] >= 8:
                        break
                    await asyncio.sleep(0.01)
                assert not task.done()  # never gave up
                assert not health.is_healthy  # but honestly reports the outage
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task

        assert calls[0] >= 8
