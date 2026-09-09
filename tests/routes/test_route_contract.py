'''
Every application server advertises the routes it actually serves.

The advertisement is what lets a client ask its peer "do you serve the routes I
call?" without comparing build SHAs — which would differ almost always, since
the mean commit touches 3.08 of 6 images. See
docs/projects/http-seam-contract.md, acceptance criterion four.

The load-bearing property is that the answer is read from the BUILT ROUTER at
request time, not from a constant. `database_server` registers its store groups
conditionally, so a constant would describe a pod that was never deployed.
'''
import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from discord_bot.routes import contract
from discord_bot.servers.base import AiohttpServerBase
from discord_bot.servers.broker_server import BrokerHttpServer
from discord_bot.servers.composite_server import CompositeHttpServer
from discord_bot.servers.database_server import DatabaseHttpServer
from discord_bot.workers.asyncio_broker import AsyncioBroker

CONTRACT_ENTRY = (contract.CONTRACT_ROUTE.method, contract.CONTRACT_ROUTE.template)


class _RouteFamily(AiohttpServerBase):
    '''Minimal server with one route, mirroring how real ones build an app.'''

    def __init__(self, path: str):
        super().__init__()
        self._path = path
        self._host, self._port = '127.0.0.1', 0

    def build_app(self) -> web.Application:
        app = web.Application(middlewares=[self._get_drain_middleware()])
        app.router.add_get(self._path, self._handle)
        self.add_contract_route(app)
        return app

    async def _handle(self, _request):
        return web.json_response({})


async def _advertised(app: web.Application) -> set[tuple[str, str]]:
    async with TestClient(TestServer(app)) as client:
        response = await client.get(contract.CONTRACT_ROUTE.template)
        assert response.status == 200
        return contract.decode(await response.json())


@pytest.mark.asyncio(loop_scope='session')
async def test_broker_advertises_what_it_serves():
    app = BrokerHttpServer(AsyncioBroker()).build_app()
    assert await _advertised(app) == contract.served_routes(app)


@pytest.mark.asyncio(loop_scope='session')
async def test_the_endpoint_advertises_itself():
    '''It is a route this listener serves, so omitting it would be a lie.'''
    app = BrokerHttpServer(AsyncioBroker()).build_app()
    assert CONTRACT_ENTRY in await _advertised(app)


@pytest.mark.asyncio(loop_scope='session')
async def test_database_advertises_only_built_groups():
    '''The runtime read, and the reason criterion four insists on one.

    A db pod constructed with no stores serves no store routes. A constant
    listing all 33 would claim routes this pod will 404, which is precisely the
    false advertisement the whole mechanism exists to avoid.
    '''
    bare = await _advertised(DatabaseHttpServer().build_app())
    assert bare == {CONTRACT_ENTRY}

    with_playlists = await _advertised(DatabaseHttpServer(playlist_store=object()).build_app())
    assert with_playlists > bare
    assert all(template.startswith('/database/playlist/')
               for method, template in with_playlists - bare)


@pytest.mark.asyncio(loop_scope='session')
async def test_composite_advertises_the_merged_set():
    '''The search pod: two route families, one bind, one honest advertisement.

    Each child registers the contract endpoint, so the composite has to skip
    theirs and serve its own — otherwise aiohttp refuses the duplicate route and
    the pod does not start.
    '''
    composite = CompositeHttpServer([_RouteFamily('/search/one'), _RouteFamily('/search/two')])
    advertised = await _advertised(composite.build_app())
    # HEAD is present because aiohttp's add_get registers it, and the
    # advertisement is a literal read of the router rather than a curated list.
    # A client's called set never contains HEAD, so the extra entries are inert
    # for the subset check and honest for anyone reading the endpoint.
    assert advertised == {
        ('GET', '/search/one'), ('HEAD', '/search/one'),
        ('GET', '/search/two'), ('HEAD', '/search/two'),
        CONTRACT_ENTRY,
    }


def test_composite_still_rejects_real_collisions():
    '''Skipping the contract route must not turn a genuine clash into a silent win.'''
    composite = CompositeHttpServer([_RouteFamily('/search/same'), _RouteFamily('/search/same')])
    with pytest.raises(RuntimeError, match='already registered'):
        composite.build_app()


def test_decode_round_trips_an_encoded_set():
    routes = {('GET', '/a'), ('POST', '/b/{id}')}
    assert contract.decode(contract.encode(routes)) == routes


@pytest.mark.parametrize('payload', [{}, {'routes': 'nope'}, {'routes': [{'method': 'GET'}]}, []])
def test_decode_treats_junk_as_no_routes(payload):
    '''Unreadable is not the same as empty-and-authoritative.

    decode() returning a set rather than raising is what keeps a garbled peer
    response off the pod's startup path; the CALLER decides that an unreadable
    answer means "unknown", never "the peer is missing everything".
    '''
    assert contract.decode(payload) == set()
