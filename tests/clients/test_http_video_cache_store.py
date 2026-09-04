'''Tests for HttpVideoCacheStore — against a real DatabaseHttpServer.

Same shape and reasoning as the three store groups before it: both halves go
through aiohttp's TestServer + TestClient, and the store behind the server is
the real VideoCacheClient on real postgres, so what is asserted is that the two
implementations of one Protocol are interchangeable rather than that a fake
agrees with itself.

What is worth the round trip here is different from the other groups. This is
the only Protocol whose signatures name a domain object, so the properties under
test are about what survives being taken apart and put back together: the file
path as a Path rather than a string, `cache_hit`, and — the one that would be
silently wrong — the caller's own MediaRequest instance coming back attached,
rather than an equal copy carrying a different uuid.
'''
# NOTE: test names here deliberately avoid being exactly 40 characters long.
# trufflehog's Lob detector matches `test_` + 35 chars, so a 40-char test
# function name is reported as a VERIFIED secret and fails pr-check:secrets.
from functools import partial
from pathlib import Path

import pytest
from aiohttp.test_utils import TestClient, TestServer
from sqlalchemy.exc import OperationalError

from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.cogs.music_helpers.video_cache_client import VideoCacheClient
from discord_bot.clients.http_video_cache_store import HttpVideoCacheStore
from discord_bot.exceptions import DatabaseUnavailable
from discord_bot.interfaces.database_protocols import VideoCacheStore
from discord_bot.servers.database_server import DatabaseHttpServer
from discord_bot.types.media_download import MediaDownload
from discord_bot.types.media_request import MediaRequest
from discord_bot.types.search import SearchResult
from discord_bot.types.video_cache import VideoCacheEntry

from tests.helpers import fake_engine #pylint:disable=unused-import
from tests.helpers import async_mock_session

GUILD_ID = 909
MAX_CACHE_FILES = 2


def _media_request(url: str = 'https://foo.example/one') -> MediaRequest:
    '''A DIRECT request whose resolved search string is the cache key.'''
    return MediaRequest(
        guild_id=GUILD_ID, channel_id=2, requester_id=3, requester_name='req',
        search_result=SearchResult(search_type=SearchType.DIRECT, raw_search_string=url),
    )


def _media_download(tmp_path: Path, media_request: MediaRequest,
                    title: str = 'a track') -> MediaDownload:
    '''A finished download pointing at a real file on disk.'''
    file_path = tmp_path / f'{title}.pcm'
    file_path.write_text('audio', encoding='utf-8')
    media_download = MediaDownload(file_path, {
        'id': f'id-{title}',
        'title': title,
        'webpage_url': media_request.search_result.resolved_search_string,
        'uploader': 'an uploader',
        'duration': 120,
        'extractor': 'youtube',
    }, media_request)
    media_download.file_size_bytes = file_path.stat().st_size
    return media_download


def _live_store(fake_engine, max_cache_files: int = MAX_CACHE_FILES) -> VideoCacheClient:  #pylint:disable=redefined-outer-name
    '''The in-process store the server delegates to, on real postgres.'''
    return VideoCacheClient(max_cache_files, partial(async_mock_session, fake_engine))


class _RecordingStore:
    '''VideoCacheStore stand-in that records calls and can fail on demand.'''

    def __init__(self, error=None):
        self.error = error
        self.calls = []

    def _record(self, name, *args):
        self.calls.append((name, *args))
        if self.error:
            raise self.error

    async def iterate_file(self, media_download):
        self._record('iterate_file', media_download)
        return True

    async def get_webpage_url_item(self, media_request):
        self._record('get_webpage_url_item', media_request)
        return None

    async def remove_video_cache(self, video_cache_ids):
        self._record('remove_video_cache', video_cache_ids)
        return True

    async def ready_remove(self):
        self._record('ready_remove')
        return True

    async def get_deletable_entries(self):
        self._record('get_deletable_entries')
        return []

    async def get_cache_count(self):
        self._record('get_cache_count')
        return 0


def test_http_video_cache_is_a_video_cache_store():
    '''The HTTP store is a VideoCacheStore, structurally.'''
    assert isinstance(HttpVideoCacheStore('http://localhost:9999'), VideoCacheStore)


@pytest.mark.asyncio
async def test_catalog_round_trip_over_the_wire(fake_engine, tmp_path):  #pylint:disable=redefined-outer-name
    '''A cataloged download comes back as an equivalent MediaDownload.'''
    server = DatabaseHttpServer(video_cache_store=_live_store(fake_engine))
    request = _media_request()
    download = _media_download(tmp_path, request)
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpVideoCacheStore(str(tc.make_url('')), session=tc.session)
        assert await client.iterate_file(download) is True
        assert await client.get_cache_count() == 1

        hit = await client.get_webpage_url_item(request)

    assert hit is not None
    assert hit.title == 'a track'
    assert hit.webpage_url == request.search_result.resolved_search_string
    assert hit.duration == 120
    assert hit.extractor == 'youtube'
    assert hit.file_size_bytes == download.file_size_bytes


@pytest.mark.asyncio
async def test_file_path_survives_as_a_path(fake_engine, tmp_path):  #pylint:disable=redefined-outer-name
    '''base_path crosses as a string and is rebuilt as a Path, not left one.

    The row column is text; a caller that got a str back would still pass
    truthiness checks and only fail later on a Path method.
    '''
    server = DatabaseHttpServer(video_cache_store=_live_store(fake_engine))
    request = _media_request()
    download = _media_download(tmp_path, request)
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpVideoCacheStore(str(tc.make_url('')), session=tc.session)
        await client.iterate_file(download)
        hit = await client.get_webpage_url_item(request)

    assert isinstance(hit.file_path, Path)
    assert hit.file_path == download.file_path


@pytest.mark.asyncio
async def test_the_hit_keeps_the_callers_request(fake_engine, tmp_path):  #pylint:disable=redefined-outer-name
    '''The returned download carries the caller's own MediaRequest instance.

    A rebuilt copy would be equal but carry a fresh uuid, and the cog tracks a
    download by the uuid of the request it submitted -- so the copy would look
    right in every assertion except the one that matters at the seam.
    '''
    server = DatabaseHttpServer(video_cache_store=_live_store(fake_engine))
    request = _media_request()
    download = _media_download(tmp_path, request)
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpVideoCacheStore(str(tc.make_url('')), session=tc.session)
        await client.iterate_file(download)
        hit = await client.get_webpage_url_item(request)

    assert hit.media_request is request
    assert hit.media_request.uuid == request.uuid


@pytest.mark.asyncio
async def test_a_hit_is_flagged_as_a_cache_hit(fake_engine, tmp_path):  #pylint:disable=redefined-outer-name
    '''cache_hit is what stops the caller re-downloading; it must survive.'''
    server = DatabaseHttpServer(video_cache_store=_live_store(fake_engine))
    request = _media_request()
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpVideoCacheStore(str(tc.make_url('')), session=tc.session)
        await client.iterate_file(_media_download(tmp_path, request))
        hit = await client.get_webpage_url_item(request)

    assert hit.cache_hit is True


@pytest.mark.asyncio
async def test_a_miss_is_none_and_not_an_error(fake_engine):  #pylint:disable=redefined-outer-name
    '''An uncached URL answers None over the wire, and does not raise.'''
    server = DatabaseHttpServer(video_cache_store=_live_store(fake_engine))
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpVideoCacheStore(str(tc.make_url('')), session=tc.session)
        assert await client.get_webpage_url_item(_media_request()) is None


@pytest.mark.asyncio
async def test_recataloging_bumps_rather_than_dupes(fake_engine, tmp_path):  #pylint:disable=redefined-outer-name
    '''The second iterate_file for a URL updates the row instead of adding one.'''
    server = DatabaseHttpServer(video_cache_store=_live_store(fake_engine))
    request = _media_request()
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpVideoCacheStore(str(tc.make_url('')), session=tc.session)
        await client.iterate_file(_media_download(tmp_path, request))
        await client.iterate_file(_media_download(tmp_path, request))

        assert await client.get_cache_count() == 1


@pytest.mark.asyncio
async def test_eviction_is_decided_remotely(fake_engine, tmp_path):  #pylint:disable=redefined-outer-name
    '''ready_remove applies the ceiling remotely and the flagged rows come back.

    The ceiling is the store's, not the caller's -- nothing about max_cache_files
    crosses the wire -- so this asserts the policy still ran and that the entries
    are readable after the loading session closed.
    '''
    server = DatabaseHttpServer(video_cache_store=_live_store(fake_engine))
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpVideoCacheStore(str(tc.make_url('')), session=tc.session)
        for index in range(MAX_CACHE_FILES + 1):
            request = _media_request(f'https://foo.example/{index}')
            await client.iterate_file(_media_download(tmp_path, request, title=f'track{index}'))

        assert await client.get_cache_count() == MAX_CACHE_FILES + 1
        assert await client.ready_remove() is True
        entries = await client.get_deletable_entries()

    assert entries, 'the ceiling was exceeded but nothing was flagged'
    assert all(isinstance(entry, VideoCacheEntry) for entry in entries)
    assert all(entry.base_path for entry in entries)


@pytest.mark.asyncio
async def test_removing_rows_is_one_request(fake_engine, tmp_path):  #pylint:disable=redefined-outer-name
    '''A batch of ids is deleted in a single call, and the rows are gone.'''
    server = DatabaseHttpServer(video_cache_store=_live_store(fake_engine))
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpVideoCacheStore(str(tc.make_url('')), session=tc.session)
        for index in range(MAX_CACHE_FILES + 1):
            request = _media_request(f'https://foo.example/{index}')
            await client.iterate_file(_media_download(tmp_path, request, title=f'track{index}'))
        await client.ready_remove()
        entries = await client.get_deletable_entries()

        assert await client.remove_video_cache([entry.id for entry in entries]) is True
        remaining = await client.get_cache_count()

    assert remaining == MAX_CACHE_FILES + 1 - len(entries)


@pytest.mark.asyncio
async def test_the_id_batch_crosses_in_one_call():
    '''remove_video_cache sends the whole list, not one request per id.'''
    store = _RecordingStore()
    async with TestClient(TestServer(DatabaseHttpServer(video_cache_store=store).build_app())) as tc:
        client = HttpVideoCacheStore(str(tc.make_url('')), session=tc.session)
        await client.remove_video_cache([4, 5, 6])

    assert store.calls == [('remove_video_cache', [4, 5, 6])]


@pytest.mark.asyncio
async def test_a_cache_failure_is_unavailable():
    '''A store failure crosses typed, and is not retried across the wire.

    Asserted per group because the envelope is applied per handler, and a route
    that forgot it would only show up here.
    '''
    store = _RecordingStore(error=OperationalError('SELECT 1', {}, Exception('gone')))
    async with TestClient(TestServer(DatabaseHttpServer(video_cache_store=store).build_app())) as tc:
        client = HttpVideoCacheStore(str(tc.make_url('')), session=tc.session)
        with pytest.raises(DatabaseUnavailable):
            await client.get_cache_count()

    assert len(store.calls) == 1


@pytest.mark.asyncio
async def test_a_malformed_request_is_rejected():
    '''A body that is not request-shaped is 422, propagated without a ladder.'''
    store = _RecordingStore()
    async with TestClient(TestServer(DatabaseHttpServer(video_cache_store=store).build_app())) as tc:
        resp = await tc.post('/database/video_cache/get_webpage_url_item',
                             json={'media_request': {'guild_id': 'not-an-int'}})

    assert resp.status == 422
    assert not store.calls, 'a malformed body reached the database'


@pytest.mark.asyncio
async def test_video_cache_routes_can_be_left_out():
    '''A server built without the store answers 404 for the group.'''
    async with TestClient(TestServer(DatabaseHttpServer().build_app())) as tc:
        resp = await tc.post('/database/video_cache/get_cache_count', json={})

    assert resp.status == 404
