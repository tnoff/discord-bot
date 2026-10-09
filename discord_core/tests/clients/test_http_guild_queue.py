'''
HttpGuildQueueMixin against a stub broker.

The stub registers a handler for every guild-queue route from the shared registry, records what
the client sent, and answers with canned bodies built from the response models.  That checks
both directions of the contract at once: the client sends what the route promises, and parses
what the models describe.  The real server is exercised in discord_broker's own tests.
'''
import ast
import contextlib
import inspect
from unittest.mock import patch

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from discord_core.clients import http_guild_queue
from discord_core.clients.http_broker_client import HttpBrokerClient
from discord_core.clients.http_client_base import SeamResponseInvalid
from discord_core.cogs.music_helpers.common import SearchType
from discord_core.interfaces.guild_queue_client import GuildQueueClient
from discord_core.routes import broker as broker_routes
from discord_core.routes import guild_queue as guild_queue_routes
from discord_core.types.checkout_result import CheckoutResult
from discord_core.types.media_request import MediaRequest
from discord_core.types.search import SearchResult

GUILD = 42
BUCKET = 'media-bucket'

GUILD_ROUTES = guild_queue_routes.ALL


class StubBroker:
    '''A broker that serves the guild-queue routes and remembers every call.'''

    def __init__(self):
        self.calls = []
        self.answers = {}

    def answer(self, route, body=None, status=200):
        '''Set what a route answers.  body None sends an empty response.'''
        self.answers[route] = (status, body)

    def _handler(self, route):
        async def handle(request: web.Request) -> web.StreamResponse:
            body = await request.json() if request.can_read_body else None
            self.calls.append({'route': route, 'guild_id': request.match_info['guild_id'],
                               'query': dict(request.query), 'body': body})
            status, answer = self.answers[route]
            if answer is None:
                return web.Response(status=status)
            return web.json_response(answer, status=status)
        return handle

    def build_app(self) -> web.Application:
        app = web.Application()
        for route in GUILD_ROUTES:
            app.router.add_route(route.method, route.template, self._handler(route))
        return app

    def last(self, route):
        return [call for call in self.calls if call['route'] == route][-1]


@contextlib.asynccontextmanager
async def _connected(stub: StubBroker):
    async with TestServer(stub.build_app()) as server:
        async with aiohttp.ClientSession() as session:
            yield HttpBrokerClient(str(server.make_url('')), bucket_name=BUCKET, session=session)


def _request(search: str = 'https://example.com/a') -> MediaRequest:
    return MediaRequest(
        guild_id=GUILD, channel_id=2, requester_name='tester', requester_id=9,
        search_result=SearchResult(search_type=SearchType.DIRECT, raw_search_string=search),
    )


def _track(request: MediaRequest, title: str = 'Song', cache_hit: bool = False) -> dict:
    '''A QueuedDownload body for request.'''
    return {
        'request': request.model_dump(mode='json'),
        'file_path': f'{title}.mp3',
        'file_size_bytes': 1234,
        'cache_hit': cache_hit,
        'ytdl_data': {'id': title, 'title': title, 'webpage_url': f'https://example.com/{title}',
                      'uploader': 'Someone', 'duration': 90, 'extractor': 'youtube'},
    }


# ---------------------------------------------------------------------------
# shape of the contract
# ---------------------------------------------------------------------------

def test_the_mixin_implements_the_whole_protocol():
    '''Every GuildQueueClient method exists on HttpBrokerClient with the same parameters.'''
    for name, member in inspect.getmembers(GuildQueueClient, inspect.isfunction):
        if name.startswith('_'):
            continue
        implemented = getattr(HttpBrokerClient, name)
        assert list(inspect.signature(implemented).parameters) == list(inspect.signature(member).parameters), name


def test_the_mixin_calls_every_route_in_the_registry():
    '''Every route declared in routes/guild_queue.py is referenced by the mixin's source.

    Mirrors the check the broker seam has for ROUTES_CALLED.  It is what makes folding this
    registry into the broker seam later mechanical: the day ROUTES_CALLED grows to include it,
    the declaration cannot already be a lie.
    '''
    tree = ast.parse(inspect.getsource(http_guild_queue))
    referenced = {
        getattr(node, 'attr')
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name) and node.value.id == 'guild_queue_routes'
    }
    declared = {name for name, value in vars(guild_queue_routes).items() if value in GUILD_ROUTES}
    assert referenced == declared


def test_the_registry_is_well_formed():
    '''Fourteen distinct routes, all under one guild.'''
    assert len(GUILD_ROUTES) == 14
    assert len(set(GUILD_ROUTES)) == 14
    assert all(route.template.startswith('/guilds/{guild_id}/') for route in GUILD_ROUTES)


def test_the_guild_routes_are_part_of_the_broker_seam():
    '''They are served by the broker pod, so the seam registry and the peer route check cover them.

    ROUTES_CALLED is what a client compares against the routes its broker advertises; leaving
    these out would make the check blind to the whole queue.
    '''
    assert set(GUILD_ROUTES) <= set(broker_routes.ALL)
    assert set(GUILD_ROUTES) <= set(HttpBrokerClient.ROUTES_CALLED)
    assert len(broker_routes.ALL) == len(set(broker_routes.ALL))


# ---------------------------------------------------------------------------
# enqueue
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize('result', ['ok', 'closed', 'full', 'duplicate'])
async def test_enqueue_returns_the_broker_result(result):
    '''Rejections come back as values, not exceptions.'''
    stub = StubBroker()
    stub.answer(guild_queue_routes.ENQUEUE_TRACK, {'result': result})
    async with _connected(stub) as client:
        assert await client.enqueue_track(GUILD, 'abc', max_size=7) == result
    call = stub.last(guild_queue_routes.ENQUEUE_TRACK)
    assert call['guild_id'] == str(GUILD)
    assert call['body'] == {'uuid': 'abc', 'max_size': 7}


@pytest.mark.asyncio
async def test_enqueue_defaults_to_an_unbounded_queue():
    '''max_size 0 is how "no cap" is spelled on the wire.'''
    stub = StubBroker()
    stub.answer(guild_queue_routes.ENQUEUE_TRACK, {'result': 'ok'})
    async with _connected(stub) as client:
        await client.enqueue_track(GUILD, 'abc')
    assert stub.last(guild_queue_routes.ENQUEUE_TRACK)['body']['max_size'] == 0


@pytest.mark.asyncio
async def test_an_unknown_enqueue_result_names_the_peer():
    '''A result this build does not know is a seam error, not a silent None.'''
    stub = StubBroker()
    stub.answer(guild_queue_routes.ENQUEUE_TRACK, {'result': 'maybe'})
    async with _connected(stub) as client:
        with pytest.raises(SeamResponseInvalid):
            await client.enqueue_track(GUILD, 'abc')


@pytest.mark.asyncio
async def test_a_missing_route_is_loud_not_an_empty_queue():
    '''A broker that predates these routes 404s; the queue is essential, so that must raise.'''
    app = web.Application()
    async with TestServer(app) as server, aiohttp.ClientSession() as session:
        client = HttpBrokerClient(str(server.make_url('')), session=session)
        with pytest.raises(aiohttp.ClientResponseError) as caught:
            await client.enqueue_track(GUILD, 'abc')
    assert caught.value.status == 404


# ---------------------------------------------------------------------------
# read the queue
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_guild_queue_rebuilds_downloads_in_order():
    '''Wire tracks come back as real MediaDownloads with their request intact.'''
    first, second = _request('https://example.com/1'), _request('https://example.com/2')
    stub = StubBroker()
    stub.answer(guild_queue_routes.GET_GUILD_QUEUE, {
        'version': 9, 'items': [_track(first, 'one'), _track(second, 'two', cache_hit=True)],
        'playing': None, 'skip_for': None, 'closed': False})
    async with _connected(stub) as client:
        queue = await client.get_guild_queue(GUILD)

    assert queue.version == 9
    assert [item.title for item in queue.items] == ['one', 'two']
    assert [item.media_request.uuid for item in queue.items] == [first.uuid, second.uuid]
    assert [item.cache_hit for item in queue.items] == [False, True]
    assert str(queue.items[0].file_path) == 'one.mp3'
    assert queue.items[0].uploader == 'Someone'
    assert queue.playing is None
    assert queue.skip_for is None
    assert queue.closed is False


@pytest.mark.asyncio
async def test_get_guild_queue_carries_playing_skip_and_closed():
    '''The playing track, a pending skip and the closed flag all survive the trip.'''
    playing_request = _request()
    stub = StubBroker()
    stub.answer(guild_queue_routes.GET_GUILD_QUEUE, {
        'version': 3, 'items': [],
        'playing': {'uuid': str(playing_request.uuid), 'started_at': 1000.5, 'gateway_id': 'gw-1',
                    'download': _track(playing_request, 'now')},
        'skip_for': str(playing_request.uuid), 'closed': True, 'text_channel_id': 777})
    async with _connected(stub) as client:
        queue = await client.get_guild_queue(GUILD)

    assert queue.playing.uuid == str(playing_request.uuid)
    assert queue.playing.started_at == 1000.5
    assert queue.playing.gateway_id == 'gw-1'
    assert queue.playing.download.title == 'now'
    assert queue.skip_for == str(playing_request.uuid)
    assert queue.closed is True
    assert queue.text_channel_id == 777


@pytest.mark.asyncio
async def test_get_guild_queue_without_a_text_channel_reads_as_none():
    '''A broker that predates the field, or a guild nobody opened, omits it.'''
    stub = StubBroker()
    stub.answer(guild_queue_routes.GET_GUILD_QUEUE, {'version': 0, 'items': [], 'closed': False})
    async with _connected(stub) as client:
        assert (await client.get_guild_queue(GUILD)).text_channel_id is None


@pytest.mark.asyncio
async def test_get_guild_queue_tolerates_a_playing_track_whose_entry_expired():
    '''No download on the playing record means no MediaDownload, not an error.'''
    stub = StubBroker()
    stub.answer(guild_queue_routes.GET_GUILD_QUEUE, {
        'version': 1, 'items': [],
        'playing': {'uuid': 'u', 'started_at': 1.0, 'gateway_id': 'gw-1', 'download': None},
        'closed': False})
    async with _connected(stub) as client:
        queue = await client.get_guild_queue(GUILD)
    assert queue.playing.download is None
    assert queue.skip_for is None


# ---------------------------------------------------------------------------
# remove / bump / shuffle / clear
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize('method,route,flag', [
    ('remove_queued_track', guild_queue_routes.REMOVE_QUEUED_TRACK, 'removed'),
    ('bump_queued_track', guild_queue_routes.BUMP_QUEUED_TRACK, 'bumped'),
])
async def test_remove_and_bump_return_the_track_or_none(method, route, flag):
    '''A hit returns the MediaDownload that was affected; a miss returns None.'''
    request = _request()
    stub = StubBroker()
    async with _connected(stub) as client:
        stub.answer(route, {flag: True, 'download': _track(request, 'hit')})
        track = await getattr(client, method)(GUILD, str(request.uuid))
        assert track.title == 'hit'
        assert track.media_request.uuid == request.uuid
        assert stub.last(route)['body'] == {'uuid': str(request.uuid)}

        stub.answer(route, {flag: False})
        assert await getattr(client, method)(GUILD, 'gone') is None


@pytest.mark.asyncio
async def test_a_hit_with_no_download_is_treated_as_a_miss():
    '''If the broker reports success but could not attach the entry, there is nothing to return.'''
    stub = StubBroker()
    stub.answer(guild_queue_routes.REMOVE_QUEUED_TRACK, {'removed': True, 'download': None})
    async with _connected(stub) as client:
        assert await client.remove_queued_track(GUILD, 'u') is None


@pytest.mark.asyncio
async def test_shuffle_and_clear():
    '''Shuffle answers whether it shuffled, clear says how many it dropped.'''
    stub = StubBroker()
    stub.answer(guild_queue_routes.SHUFFLE_QUEUE, {'shuffled': True})
    stub.answer(guild_queue_routes.CLEAR_QUEUE, {'cleared': 4})
    async with _connected(stub) as client:
        assert await client.shuffle_queue(GUILD) is True
        assert await client.clear_queue(GUILD) == 4
    assert stub.last(guild_queue_routes.SHUFFLE_QUEUE)['guild_id'] == str(GUILD)
    assert stub.last(guild_queue_routes.CLEAR_QUEUE)['guild_id'] == str(GUILD)


# ---------------------------------------------------------------------------
# poll
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_poll_returns_version_and_skip():
    '''A changed queue answers with the version and any pending skip.'''
    stub = StubBroker()
    stub.answer(guild_queue_routes.POLL_GUILD_QUEUE, {'version': 12, 'skip_for': 'abc'})
    async with _connected(stub) as client:
        assert await client.poll_guild_queue(GUILD) == (12, 'abc')


@pytest.mark.asyncio
async def test_poll_without_a_skip_reports_none_for_it():
    '''skip_for is optional on the wire.'''
    stub = StubBroker()
    stub.answer(guild_queue_routes.POLL_GUILD_QUEUE, {'version': 2})
    async with _connected(stub) as client:
        assert await client.poll_guild_queue(GUILD) == (2, None)


@pytest.mark.asyncio
async def test_poll_sends_since_only_when_given():
    '''The first poll has nothing to compare against; later ones pass the version they hold.'''
    stub = StubBroker()
    stub.answer(guild_queue_routes.POLL_GUILD_QUEUE, {'version': 5})
    async with _connected(stub) as client:
        await client.poll_guild_queue(GUILD)
        assert stub.last(guild_queue_routes.POLL_GUILD_QUEUE)['query'] == {}
        await client.poll_guild_queue(GUILD, since=5)
        assert stub.last(guild_queue_routes.POLL_GUILD_QUEUE)['query'] == {'since': '5'}
        await client.poll_guild_queue(GUILD, since=0)
        assert stub.last(guild_queue_routes.POLL_GUILD_QUEUE)['query'] == {'since': '0'}


@pytest.mark.asyncio
async def test_poll_of_an_unchanged_queue_is_none_and_mints_no_span():
    '''An idle poll is a 204 and costs no span: it runs about once a second per active guild.'''
    stub = StubBroker()
    stub.answer(guild_queue_routes.POLL_GUILD_QUEUE, None, status=204)
    async with _connected(stub) as client:
        with patch.object(http_guild_queue, 'async_otel_span_wrapper') as span:
            assert await client.poll_guild_queue(GUILD, since=5) is None
    span.assert_not_called()


@pytest.mark.asyncio
async def test_poll_with_news_opens_a_span():
    '''Once there is a body to parse, the span is worth having.'''
    stub = StubBroker()
    stub.answer(guild_queue_routes.POLL_GUILD_QUEUE, {'version': 6})
    async with _connected(stub) as client:
        with patch.object(http_guild_queue, 'async_otel_span_wrapper',
                          wraps=http_guild_queue.async_otel_span_wrapper) as span:
            await client.poll_guild_queue(GUILD, since=5)
    assert span.call_args.args[0] == 'broker.poll_guild_queue'


@pytest.mark.asyncio
async def test_poll_error_status_raises():
    '''A broker error is not an unchanged queue.'''
    stub = StubBroker()
    stub.answer(guild_queue_routes.POLL_GUILD_QUEUE, {'error': 'x'}, status=500)
    async with _connected(stub) as client:
        with pytest.raises(aiohttp.ClientResponseError):
            await client.poll_guild_queue(GUILD)


# ---------------------------------------------------------------------------
# claim
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_claim_returns_the_download_and_an_s3_checkout():
    '''A claim carries the track and where to fetch it; the bucket is the client's own.'''
    request = _request()
    stub = StubBroker()
    stub.answer(guild_queue_routes.CLAIM_TRACK, {
        'claimed': True, 'download': _track(request, 'claimed'), 's3_key': 'claimed.mp3'})
    async with _connected(stub) as client:
        claimed = await client.claim_next_track(GUILD, 'gw-1')

    assert claimed.download.title == 'claimed'
    assert claimed.download.media_request.uuid == request.uuid
    assert claimed.checkout == CheckoutResult(s3_key='claimed.mp3', bucket_name=BUCKET)
    assert stub.last(guild_queue_routes.CLAIM_TRACK)['body'] == {'gateway_id': 'gw-1'}


@pytest.mark.asyncio
async def test_claim_of_an_empty_queue_is_none():
    '''Nothing queued answers claimed false.'''
    stub = StubBroker()
    stub.answer(guild_queue_routes.CLAIM_TRACK, {'claimed': False})
    async with _connected(stub) as client:
        assert await client.claim_next_track(GUILD, 'gw-1') is None


@pytest.mark.asyncio
async def test_a_claim_hit_missing_its_key_names_the_peer():
    '''A hit that cannot be parsed as one is a seam error, not a miss.'''
    stub = StubBroker()
    stub.answer(guild_queue_routes.CLAIM_TRACK, {'claimed': True})
    async with _connected(stub) as client:
        with pytest.raises(SeamResponseInvalid):
            await client.claim_next_track(GUILD, 'gw-1')


@pytest.mark.asyncio
async def test_a_claim_with_no_body_names_the_peer():
    '''An empty answer is neither a hit nor a miss.'''
    stub = StubBroker()
    stub.answer(guild_queue_routes.CLAIM_TRACK, None, status=200)
    async with _connected(stub) as client:
        with pytest.raises(SeamResponseInvalid):
            await client.claim_next_track(GUILD, 'gw-1')


# ---------------------------------------------------------------------------
# heartbeat / skip / finish
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize('alive', [True, False])
async def test_heartbeat(alive):
    '''The heartbeat reports whether the named track is still the one playing.'''
    stub = StubBroker()
    stub.answer(guild_queue_routes.PLAYING_HEARTBEAT, {'alive': alive})
    async with _connected(stub) as client:
        assert await client.playing_heartbeat(GUILD, 'abc') is alive
    assert stub.last(guild_queue_routes.PLAYING_HEARTBEAT)['body'] == {'uuid': 'abc'}


@pytest.mark.asyncio
@pytest.mark.parametrize('result', ['ok', 'no_player', 'not_current'])
async def test_skip_returns_the_result(result):
    '''Skip names the track it means and reports what happened.'''
    stub = StubBroker()
    stub.answer(guild_queue_routes.SKIP_TRACK, {'result': result})
    async with _connected(stub) as client:
        assert await client.skip_track(GUILD, 'abc') == result
    assert stub.last(guild_queue_routes.SKIP_TRACK)['body'] == {'uuid': 'abc'}


@pytest.mark.asyncio
async def test_finish_sends_everything_the_broker_needs():
    '''Finish carries whether the track was skipped and how much history to keep.'''
    stub = StubBroker()
    stub.answer(guild_queue_routes.FINISH_TRACK, {'status': 'ok'})
    async with _connected(stub) as client:
        assert await client.finish_track(GUILD, 'abc', skipped=True, history_cap=25) is None
    assert stub.last(guild_queue_routes.FINISH_TRACK)['body'] == {
        'uuid': 'abc', 'skipped': True, 'history_cap': 25}


# ---------------------------------------------------------------------------
# history / close / open
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_history_returns_the_stored_records():
    '''History records pass through as the dicts they were stored as.'''
    items = [{'uuid': 'a', 'title': 'One'}, {'uuid': 'b', 'title': 'Two'}]
    stub = StubBroker()
    stub.answer(guild_queue_routes.GET_GUILD_HISTORY, {'items': items})
    async with _connected(stub) as client:
        assert await client.get_guild_history(GUILD) == items


@pytest.mark.asyncio
async def test_close_releases_and_reports_how_many():
    '''Close reports how many entries it released.'''
    stub = StubBroker()
    stub.answer(guild_queue_routes.CLOSE_GUILD, {'released': 3})
    async with _connected(stub) as client:
        assert await client.close_guild(GUILD) == 3
    assert stub.last(guild_queue_routes.CLOSE_GUILD)['guild_id'] == str(GUILD)


@pytest.mark.asyncio
async def test_open_sends_the_text_channel_and_returns_what_was_recovered():
    '''Opening names the channel for the play-order message and reports a requeued track.'''
    stub = StubBroker()
    stub.answer(guild_queue_routes.OPEN_GUILD, {'status': 'ok', 'recovered': 'abc'})
    async with _connected(stub) as client:
        assert await client.open_guild(GUILD, 555) == 'abc'
    call = stub.last(guild_queue_routes.OPEN_GUILD)
    assert call['guild_id'] == str(GUILD)
    assert call['body'] == {'text_channel_id': 555}


@pytest.mark.asyncio
async def test_open_with_nothing_to_recover_returns_none():
    '''The common case, and what a broker that predates recovery answers.'''
    stub = StubBroker()
    stub.answer(guild_queue_routes.OPEN_GUILD, {'status': 'ok'})
    async with _connected(stub) as client:
        assert await client.open_guild(GUILD, 555) is None
