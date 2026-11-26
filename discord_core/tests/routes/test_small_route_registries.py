'''Tests for the queue-worker, media-search, database and contract route registries.'''
import pytest
from aiohttp import web

from discord_core.routes import contract, media_search, queue_worker
from discord_core.routes.database import DatabaseGroup, GROUPS, MARKOV, ROUTE_PREFIX
from discord_core.routes.route import Route


def test_queue_worker_prefixes_build_the_four_routes():
    '''at_prefix hangs submit, clear, block and status off one prefix'''
    routes = queue_worker.at_prefix('/x')
    assert routes.submit == Route('POST', '/x')
    assert routes.clear == Route('POST', '/x/clear')
    assert routes.block == Route('POST', '/x/block')
    assert routes.status == Route('GET', '/x/status')
    assert routes.prefix == '/x'
    assert routes.all == (routes.submit, routes.clear, routes.block, routes.status)


def test_queue_worker_serves_downloads_and_ytmusic_prefixes():
    '''The registry lists eight routes across the two pods, with no overlap'''
    assert queue_worker.DOWNLOADS.prefix == '/downloads'
    assert queue_worker.YTMUSIC.prefix == '/search/ytmusic'
    assert len(queue_worker.ALL) == 8
    assert len(set(queue_worker.ALL)) == 8


def test_media_search_registry_lists_the_two_provider_routes():
    '''Spotify and YouTube expansion are POSTs under /search, collected in order'''
    assert media_search.SPOTIFY == Route('POST', '/search/spotify')
    assert media_search.YOUTUBE == Route('POST', '/search/youtube')
    assert media_search.ALL == (media_search.SPOTIFY, media_search.YOUTUBE)


def test_media_search_routes_do_not_collide_with_ytmusic():
    '''Both seams share the search pod, so no route may appear in both'''
    assert not set(media_search.ALL) & set(queue_worker.ALL)


def test_database_group_prefix_and_routes():
    '''A group exposes its prefix and its routes in definition order'''
    assert MARKOV.prefix == f'{ROUTE_PREFIX}/markov'
    assert MARKOV.all[0] == Route('POST', '/database/markov/list_channels')
    assert [group.name for group in GROUPS] == ['guild_analytics', 'markov', 'playlist', 'video_cache']


def test_database_group_bind_pairs_routes_with_handlers():
    '''bind keys each handler by its Route when the names match exactly'''
    group = DatabaseGroup('demo', {'one': Route('POST', '/database/demo/one'),
                                   'two': Route('POST', '/database/demo/two')})

    async def handler():
        '''Do nothing.'''

    bound = group.bind({'one': handler, 'two': handler})
    assert bound == {Route('POST', '/database/demo/one'): handler, Route('POST', '/database/demo/two'): handler}


def test_database_group_bind_names_missing_and_unknown_handlers():
    '''A mismatch raises ValueError that says which side is wrong'''
    group = DatabaseGroup('demo', {'one': Route('POST', '/database/demo/one')})

    async def handler():
        '''Do nothing.'''

    with pytest.raises(ValueError) as caught:
        group.bind({'extra': handler})
    message = str(caught.value)
    assert "database group 'demo' handler mismatch" in message
    assert "no handler for ['one']" in message
    assert "handler for undefined ['extra']" in message


def test_contract_encode_and_decode_round_trip_sorted():
    '''encode sorts for stable diffing and decode recovers the set'''
    routes = {('POST', '/b'), ('GET', '/a')}
    payload = contract.encode(routes)
    assert payload == {'routes': [{'method': 'GET', 'template': '/a'},
                                  {'method': 'POST', 'template': '/b'}]}
    assert contract.decode(payload) == routes


def test_contract_decode_tolerates_malformed_payloads():
    '''Garbage in gives an empty set, and bad entries are skipped'''
    assert contract.decode('nope') == set()
    assert contract.decode({'routes': 'nope'}) == set()
    assert contract.decode({'routes': [{'method': 'GET'}, 3, {'method': 'GET', 'template': '/ok'}]}) == {
        ('GET', '/ok')}


def test_contract_served_routes_reads_the_built_router():
    '''served_routes returns (method, canonical template) from the live app'''
    async def handler(_request):
        '''Return an empty response.'''
        return web.Response()

    app = web.Application()
    app.router.add_get('/things/{id}', handler)
    served = contract.served_routes(app)
    assert ('GET', '/things/{id}') in served
    assert all(method != 'HEAD' or template == '/things/{id}' for method, template in served)
