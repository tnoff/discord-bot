'''
CompositeHttpServer — one listener, several route families.

The heartbeat propagation tests are the reason this class exists, so they assert
the gauge the pod actually publishes rather than the flag behind it: a child
merged into a composite used to report is_serving False forever, which would have
made `youtube_music_search_server` read a flat 0 on the Discord Health dashboard
and in the DiscordHeartbeat alert while the listener was up and healthy.
'''
import asyncio

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from discord_bot.servers.base import AiohttpServerBase
from discord_bot.servers.composite_server import CompositeHttpServer


class _RouteFamily(AiohttpServerBase):
    '''Minimal AiohttpServerBase with one GET route, for merge tests.'''

    def __init__(self, path: str, payload: str):
        super().__init__()
        self._path = path
        self._payload = payload
        self._host = '127.0.0.1'
        self._port = 0

    def build_app(self) -> web.Application:
        app = web.Application(middlewares=[self._get_drain_middleware()])
        app.router.add_get(self._path, self._handle)
        return app

    async def _handle(self, _request):
        return web.json_response({'from': self._payload})

    def heartbeat_value(self) -> int:
        '''Stand-in for the real servers' observable gauge, built on is_serving.'''
        return 1 if self.is_serving else 0


def test_rejects_an_empty_server_list():
    with pytest.raises(ValueError, match='at least one server'):
        CompositeHttpServer([])


@pytest.mark.asyncio
async def test_every_family_is_reachable_on_one_bind():
    '''Both children answer on their own absolute paths, unprefixed.'''
    composite = CompositeHttpServer([_RouteFamily('/search/ytmusic/status', 'ytmusic'),
                                     _RouteFamily('/search/spotify', 'spotify')])
    async with TestClient(TestServer(composite.build_app())) as tc:
        first = await (await tc.get('/search/ytmusic/status')).json()
        second = await (await tc.get('/search/spotify')).json()
    assert first == {'from': 'ytmusic'}
    assert second == {'from': 'spotify'}


@pytest.mark.asyncio
async def test_child_paths_are_not_prefixed_again():
    '''
    A subapp mount would have served /search/search/spotify.

    Worth its own test: the paths are a cross-pod contract, and doubling the
    prefix is the kind of change that passes every in-process test and 404s in
    prod against a bot that was never told.
    '''
    composite = CompositeHttpServer([_RouteFamily('/search/spotify', 'spotify')])
    async with TestClient(TestServer(composite.build_app())) as tc:
        assert (await tc.get('/search/spotify')).status == 200
        assert (await tc.get('/search/search/spotify')).status == 404


def test_children_report_the_listener_as_up():
    '''
    The trap this class closes: a merged child's own serve() never runs.

    Without propagation its is_serving stays False forever, so a heartbeat gauge
    built on it publishes 0 for a listener that is serving fine.
    '''
    ytmusic = _RouteFamily('/search/ytmusic/status', 'ytmusic')
    media = _RouteFamily('/search/spotify', 'spotify')
    composite = CompositeHttpServer([ytmusic, media])

    assert ytmusic.heartbeat_value() == 0
    assert media.heartbeat_value() == 0

    composite.set_serving(True)
    assert composite.is_serving is True
    assert ytmusic.heartbeat_value() == 1
    assert media.heartbeat_value() == 1

    composite.set_serving(False)
    assert ytmusic.heartbeat_value() == 0
    assert media.heartbeat_value() == 0


def test_draining_takes_the_children_down_too():
    '''A drain is a property of the shared listener, so it reaches every child.'''
    ytmusic = _RouteFamily('/search/ytmusic/status', 'ytmusic')
    media = _RouteFamily('/search/spotify', 'spotify')
    composite = CompositeHttpServer([ytmusic, media])
    composite.set_serving(True)

    composite.start_draining()

    assert composite.is_serving is False
    assert ytmusic.heartbeat_value() == 0
    assert media.heartbeat_value() == 0


@pytest.mark.asyncio
async def test_draining_refuses_requests_for_every_family():
    '''One drain middleware on the composite covers routes from all children.'''
    composite = CompositeHttpServer([_RouteFamily('/search/ytmusic/status', 'ytmusic'),
                                     _RouteFamily('/search/spotify', 'spotify')])
    async with TestClient(TestServer(composite.build_app())) as tc:
        composite.start_draining()
        assert (await tc.get('/search/ytmusic/status')).status == 503
        assert (await tc.get('/search/spotify')).status == 503


@pytest.mark.asyncio
async def test_serve_marks_children_up_then_down():
    '''End to end through the real serve() lifecycle, not just set_serving.'''
    child = _RouteFamily('/search/spotify', 'spotify')
    composite = CompositeHttpServer([child], host='127.0.0.1', port=0)

    task = asyncio.create_task(composite.serve())
    for _ in range(100):
        if child.is_serving:
            break
        await asyncio.sleep(0.01)
    assert child.heartbeat_value() == 1

    await composite.drain_and_stop(timeout=1.0)
    await asyncio.wait_for(task, timeout=5)
    assert child.heartbeat_value() == 0
