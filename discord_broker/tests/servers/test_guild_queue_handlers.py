'''Tests for the guild-queue route handlers: bad requests, an unconfigured server, and the defensive paths.'''
from pathlib import Path
import pytest
from aiohttp.test_utils import TestClient, TestServer

from discord_core.cogs.music_helpers.common import SearchType
from discord_core.routes import guild_queue as guild_queue_routes
from discord_core.types import broker_responses
from discord_core.types.media_download import MediaDownload
from discord_core.types.media_request import MediaRequest
from discord_core.types.search import SearchResult

from discord_broker.workers import guild_queue_registry as registry_module
from tests.fakes.asyncio_broker import AsyncioBroker
from tests.fakes.asyncio_queues import make_broker_http_server, make_guild_queue_broker

GUILD = 5


def _request(title: str = 'x') -> MediaRequest:
    return MediaRequest(
        guild_id=GUILD, channel_id=2, requester_name='tester', requester_id=9,
        search_result=SearchResult(search_type=SearchType.DIRECT,
                                   raw_search_string=f'https://example.com/{title}'),
    )


async def _with_download(broker, title: str) -> str:
    request = _request(title)
    await broker.register_request(request)
    await broker.register_download(MediaDownload(
        Path(f'{title}.mp3'),
        {'id': title, 'title': title, 'webpage_url': f'https://example.com/{title}',
         'uploader': 'u', 'duration': 1, 'extractor': 'youtube'}, request))
    return str(request.uuid)


async def _without_download(broker) -> str:
    '''An entry that exists but has no file: registered, never downloaded.'''
    request = _request('nofile')
    await broker.register_request(request)
    return str(request.uuid)


class _Env:
    def __init__(self):
        self.queue = make_guild_queue_broker()
        self.server = make_broker_http_server(self.queue.broker, guild_queue=self.queue)
        self.broker = self.queue.broker


@pytest.fixture(name='env')
def env_fixture():
    return _Env()


async def _client(server):
    client = TestClient(TestServer(server.build_app()))
    await client.start_server()
    return client


def _path(route, guild='5'):
    return route.template.replace('{guild_id}', guild)


# ---------------------------------------------------------------------------
# configuration
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_a_server_with_no_queue_serves_none_of_the_routes():
    '''No GuildQueueBroker, no routes: the registry has them, the router does not.'''
    server = make_broker_http_server(AsyncioBroker(), guild_queue=None)
    assert not server.guild_queue_handlers()
    client = await _client(server)
    try:
        for route in guild_queue_routes.ALL:
            resp = await client.request(route.method, _path(route), json={})
            assert resp.status == 404, route
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_every_guild_queue_route_has_a_handler(env):
    '''The handler map and the registry are the same set.'''
    assert set(env.server.guild_queue_handlers()) == set(guild_queue_routes.ALL)


def test_response_models_allow_exactly_the_values_the_broker_produces():
    '''The Literals in discord_core and the constants in the broker are two lists of one thing.'''
    enqueue = broker_responses.EnqueueTrackResponse.model_json_schema()['properties']['result']['enum']
    assert set(enqueue) == {registry_module.ENQUEUE_OK, registry_module.ENQUEUE_CLOSED,
                            registry_module.ENQUEUE_FULL, registry_module.ENQUEUE_DUPLICATE}
    skip = broker_responses.SkipTrackResponse.model_json_schema()['properties']['result']['enum']
    assert set(skip) == {registry_module.SKIP_OK, registry_module.SKIP_NO_PLAYER,
                         registry_module.SKIP_NOT_CURRENT}


# ---------------------------------------------------------------------------
# bad requests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize('route', guild_queue_routes.ALL, ids=lambda route: f'{route.method} {route.template}')
async def test_a_non_numeric_guild_is_unprocessable(env, route):
    '''The guild id is part of the path and must be an integer, on every route.'''
    client = await _client(env.server)
    try:
        resp = await client.request(route.method, _path(route, 'abc'),
                                    json={'uuid': 'u', 'gateway_id': 'g', 'skipped': False, 'history_cap': 1})
        assert resp.status == 422
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('route,body', [
    (guild_queue_routes.ENQUEUE_TRACK, {}),
    (guild_queue_routes.ENQUEUE_TRACK, {'uuid': 'u', 'max_size': 'many'}),
    (guild_queue_routes.REMOVE_QUEUED_TRACK, {}),
    (guild_queue_routes.BUMP_QUEUED_TRACK, {}),
    (guild_queue_routes.CLAIM_TRACK, {}),
    (guild_queue_routes.PLAYING_HEARTBEAT, {}),
    (guild_queue_routes.SKIP_TRACK, {}),
    (guild_queue_routes.OPEN_GUILD, {}),
    (guild_queue_routes.OPEN_GUILD, {'text_channel_id': 'general'}),
    (guild_queue_routes.FINISH_TRACK, {'uuid': 'u'}),
    (guild_queue_routes.FINISH_TRACK, {'uuid': 'u', 'skipped': True}),
    (guild_queue_routes.FINISH_TRACK, {'uuid': 'u', 'skipped': True, 'history_cap': 'lots'}),
    (guild_queue_routes.FINISH_TRACK, {'skipped': True, 'history_cap': 1}),
], ids=lambda value: getattr(value, 'template', None))
async def test_a_body_missing_or_mangling_a_field_is_unprocessable(env, route, body):
    '''A request the caller got wrong is a 422, not a server error.'''
    client = await _client(env.server)
    try:
        resp = await client.request(route.method, _path(route), json=body)
        assert resp.status == 422
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_a_body_that_is_not_json_is_unprocessable(env):
    '''Malformed JSON is the caller's fault too.'''
    client = await _client(env.server)
    try:
        resp = await client.post(_path(guild_queue_routes.ENQUEUE_TRACK), data='not json',
                                 headers={'Content-Type': 'application/json'})
        assert resp.status == 422
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_a_non_numeric_since_is_unprocessable(env):
    '''?since= must be the integer version the poller holds.'''
    client = await _client(env.server)
    try:
        resp = await client.get(_path(guild_queue_routes.POLL_GUILD_QUEUE) + '?since=soon')
        assert resp.status == 422
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_enqueue_max_size_defaults_to_unbounded(env):
    '''Leaving max_size out means no cap.'''
    uuid = await _with_download(env.broker, 'one')
    client = await _client(env.server)
    try:
        resp = await client.post(_path(guild_queue_routes.ENQUEUE_TRACK), json={'uuid': uuid})
        assert (resp.status, await resp.json()) == (200, {'result': 'ok'})
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# poll
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_poll_is_204_only_when_nothing_changed_and_no_skip_pending(env):
    '''since equal to the version is "no news"; a pending skip is news even then.'''
    uuid = await _with_download(env.broker, 'one')
    await env.queue.enqueue(GUILD, uuid)
    client = await _client(env.server)
    url = _path(guild_queue_routes.POLL_GUILD_QUEUE)
    try:
        assert (await client.get(url + '?since=1')).status == 204
        assert (await client.get(url + '?since=0')).status == 200
        assert (await client.get(url)).status == 200

        await env.queue.claim_next(GUILD, 'gw-1')
        await env.queue.skip(GUILD, uuid)
        body = await (await client.get(url + '?since=2')).json()
        assert body == {'version': 2, 'skip_for': uuid}
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# entries that cannot be played
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_a_queued_entry_without_a_download_is_left_out_of_the_read(env):
    '''It is dead weight in the list; the read does not pretend it is a track.'''
    good = await _with_download(env.broker, 'good')
    bad = await _without_download(env.broker)
    await env.queue.enqueue(GUILD, bad)
    await env.queue.enqueue(GUILD, good)
    client = await _client(env.server)
    try:
        body = await (await client.get(_path(guild_queue_routes.GET_GUILD_QUEUE))).json()
        assert [item['ytdl_data']['title'] for item in body['items']] == ['good']
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_playing_track_without_a_download_is_reported_without_one(env):
    '''A playing record whose entry has no file still reads; its download is null.'''
    bare = await _without_download(env.broker)
    await env.queue.enqueue(GUILD, bare)
    registry = env.queue._queues  # pylint: disable=protected-access
    assert await registry.queue_claim_next(GUILD) == bare
    assert await registry.confirm_claim(GUILD, bare, 'gw-1') is True
    client = await _client(env.server)
    try:
        body = await (await client.get(_path(guild_queue_routes.GET_GUILD_QUEUE))).json()
        assert body['playing']['uuid'] == bare
        assert body['playing']['download'] is None
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_playing_track_whose_entry_expired_is_reported_without_a_download(env):
    '''Same answer when the entry itself is gone.'''
    uuid = await _with_download(env.broker, 'one')
    await env.queue.enqueue(GUILD, uuid)
    await env.queue.claim_next(GUILD, 'gw-1')
    await env.broker._registry.delete_entry(uuid)  # pylint: disable=protected-access
    client = await _client(env.server)
    try:
        body = await (await client.get(_path(guild_queue_routes.GET_GUILD_QUEUE))).json()
        assert body['playing']['uuid'] == uuid
        assert body['playing']['download'] is None
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('route,flag', [
    (guild_queue_routes.REMOVE_QUEUED_TRACK, 'removed'),
    (guild_queue_routes.BUMP_QUEUED_TRACK, 'bumped'),
])
async def test_remove_and_bump_of_a_queued_entry_without_a_download_report_a_miss(env, route, flag):
    '''Nothing playable to hand back, so the answer is a miss with no download.'''
    bare = await _without_download(env.broker)
    await env.queue.enqueue(GUILD, bare)
    client = await _client(env.server)
    try:
        resp = await client.post(_path(route), json={'uuid': bare})
        assert (resp.status, await resp.json()) == (200, {flag: False, 'download': None})
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# text channel and recovery
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_open_records_the_channel_and_the_queue_read_reports_it(env):
    '''open takes the text channel; the read returns it (null for a guild nobody opened).'''
    client = await _client(env.server)
    try:
        before = await (await client.get(_path(guild_queue_routes.GET_GUILD_QUEUE))).json()
        assert before['text_channel_id'] is None

        resp = await client.post(_path(guild_queue_routes.OPEN_GUILD), json={'text_channel_id': 4242})
        assert (resp.status, await resp.json()) == (200, {'status': 'ok', 'recovered': None})

        after = await (await client.get(_path(guild_queue_routes.GET_GUILD_QUEUE))).json()
        assert after['text_channel_id'] == 4242
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_open_reports_the_track_it_recovered(env):
    '''A gateway that died mid-track: the next open puts the track back and says which.'''
    uuid = await _with_download(env.broker, 'interrupted')
    await env.queue.enqueue(GUILD, uuid)
    await env.queue.claim_next(GUILD, 'gw-old')
    await env.queue._queues._client.delete(  # pylint: disable=protected-access
        f'discord_bot:broker:gplaying:{GUILD}')
    client = await _client(env.server)
    try:
        resp = await client.post(_path(guild_queue_routes.OPEN_GUILD), json={'text_channel_id': 1})
        assert (await resp.json())['recovered'] == uuid
        body = await (await client.get(_path(guild_queue_routes.GET_GUILD_QUEUE))).json()
        assert [item['request']['uuid'] for item in body['items']] == [uuid]
    finally:
        await client.close()
