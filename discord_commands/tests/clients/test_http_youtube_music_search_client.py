'''Tests for HttpYoutubeMusicSearchClient — exercised against a real
YoutubeMusicSearchHttpServer.

Both halves (the cog-side HttpYoutubeMusicSearchClient and the pod-side
YoutubeMusicSearchHttpServer) are covered together by routing requests through
aiohttp's TestServer + TestClient; the RedisYoutubeMusicSearchWorker is replaced
by a small fake that records calls + returns canned responses.
RedisYoutubeMusicSearchWorker.status_snapshot itself is covered against fakeredis
in tests/workers/test_redis_youtube_music_search_worker.py.
'''
# NOTE: test names here deliberately avoid being exactly 40 characters long.
# trufflehog's Lob detector matches `test_` + 35 chars — the exact shape of a
# Lob test API key — so a 40-char test function name is reported as a VERIFIED
# secret and fails pr-check:secrets. Renaming is cheaper than an allowlist.
from unittest.mock import AsyncMock

import pytest
from aiohttp.test_utils import TestClient, TestServer

from discord_core.cogs.music_helpers.common import SearchType
from discord_core.types.queue import PutsBlocked

from discord_commands.clients.http_youtube_music_search_client import HttpYoutubeMusicSearchClient
from discord_search.servers.youtube_music_search_server import YoutubeMusicSearchHttpServer

from tests.http_queue_client_suite import FakeQueueWorker, QueueClientSuite, QueueServerSuite, media_request

SEARCH_STRING = 'some song name'


def _make_server(worker=None):
    worker = worker or FakeQueueWorker()
    return worker, YoutubeMusicSearchHttpServer(worker)


class TestHttpYoutubeMusicSearchClient(QueueClientSuite, QueueServerSuite):
    '''The shared queue-client cases, against the search pod.

    The heartbeat job is the search pod's own label, NOT `youtube_music_search`, which the
    bot cog's search loop already publishes.
    '''
    client_cls = HttpYoutubeMusicSearchClient
    server_cls = YoutubeMusicSearchHttpServer
    prefix = '/search/ytmusic'
    request_search_type = SearchType.SEARCH
    request_search_string = SEARCH_STRING
    heartbeat_job = 'youtube_music_search_server'


# ---------------------------------------------------------------------------
# The seam this client does NOT carry
# ---------------------------------------------------------------------------

def test_client_omits_the_pop_and_resolve_surface():
    '''resolve / get_input_nowait / backoff_wait / set_wait_timestamp are absent by
    design: under HA the search loop runs in the search pod, and resolutions come
    back to the bot through the broker's search-result queue, not this client.  A
    call site that reaches for one is a wiring bug and should fail loudly.'''
    client = HttpYoutubeMusicSearchClient('http://localhost:9999')
    for attribute in ('resolve', 'get_input_nowait', 'backoff_wait',
                      'set_wait_timestamp', 'local_worker'):
        assert not hasattr(client, attribute)


def test_routes_sit_under_a_per_provider_segment():
    '''ytmusic owns /search/ytmusic, not a bare /search. The search tier is meant to
    co-host the media_search providers (Spotify, the YouTube Data API) in the same
    slim image and pod, so /search stays the pod namespace and /search/spotify +
    /search/youtube stay free. Renaming this route once the pod ships would be a
    cross-pod protocol change — the skew-sensitive kind that broke this seam twice.'''
    assert YoutubeMusicSearchHttpServer.ROUTE_PREFIX == '/search/ytmusic'


def test_client_and_server_share_one_route_group():
    '''The two halves are deployed as separate pods, so a prefix that drifts between
    them is a silent 404 across a version skew, not a test failure.

    Asserts the two read the SAME registry group object, not merely that their
    prefixes are equal. Since both now derive ROUTE_PREFIX from ROUTES, comparing
    the prefixes would be tautological -- it would pass by construction and read
    as coverage while checking nothing. Identity is the property that actually
    holds the seam together, and it fails if either side is repointed.
    '''
    assert HttpYoutubeMusicSearchClient.ROUTES is YoutubeMusicSearchHttpServer.ROUTES
    assert (HttpYoutubeMusicSearchClient.ROUTES.prefix
            == YoutubeMusicSearchHttpServer.ROUTE_PREFIX == '/search/ytmusic')


def test_default_port_is_8084():
    '''The search pod listens on 8084 (the downloader has 8083).'''
    _, server = _make_server()
    assert server._port == 8084  # pylint: disable=protected-access


def test_explicit_port_overrides_the_class_default():
    '''An explicit port wins over DEFAULT_PORT.'''
    server = YoutubeMusicSearchHttpServer(FakeQueueWorker(), port=9111)
    assert server._port == 9111  # pylint: disable=protected-access


# ---------------------------------------------------------------------------
# Submit rejections across the HTTP seam
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_blocked_guild_submit_maps_back():
    '''The regression this seam actually hit in production (trace fc7c6b0b,
    2026-08-17): a blocked guild made the search pod 500, the bot retried three
    times, and /play failed with a ClientResponseError the cog could not match
    against `except PutsBlocked`.'''
    worker, server = _make_server()
    worker.submit = AsyncMock(side_effect=PutsBlocked('Puts blocked for guild 7'))
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpYoutubeMusicSearchClient(str(tc.make_url('')), session=tc.session)
        with pytest.raises(PutsBlocked):
            await client.submit(7, media_request(guild_id=7))
    assert worker.submit.await_count == 1
