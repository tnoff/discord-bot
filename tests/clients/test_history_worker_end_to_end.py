'''
The history worker against the real db pod's HTTP server and a real SQLite database.

The worker's own tests use fake stores, which cannot notice the broker's two small store clients
disagreeing with the pod that serves them.  This does: a track plays out in the broker, the worker
drains the record over HTTP, and the rows are read back through the db pod's own store.
'''
import asyncio
from functools import partial
from pathlib import Path

import fakeredis.aioredis
import pytest
from aiohttp.test_utils import TestClient, TestServer

from discord_core.clients.redis_client import RedisManager
from discord_core.cogs.music_helpers.common import SearchType
from discord_core.types.media_download import MediaDownload
from discord_core.types.media_request import MediaRequest
from discord_core.types.search import SearchResult

from discord_broker.clients.http_history_stores import HttpHistoryPlaylistStore, HttpPlayAnalyticsStore
from discord_broker.workers.broker_registry import RedisBrokerRegistry
from discord_broker.workers.guild_queue import GuildQueueBroker
from discord_broker.workers.guild_queue_registry import GuildQueueRegistry
from discord_broker.workers.history_worker import HistoryWorker, Outcome
from discord_broker.workers.redis_broker import RedisBroker
from discord_db.clients.guild_analytics_client import GuildAnalyticsClient
from discord_db.clients.playlist_client import PlaylistClient
from discord_db.servers.database_server import DatabaseHttpServer

from tests.helpers import async_mock_session
from tests.helpers import fake_engine  # pylint:disable=unused-import

GUILD = 4242
MAX_SIZE = 3


class _Stack:
    '''The broker half plus a worker wired to a live db pod.'''

    def __init__(self, engine, test_client: TestClient):
        factory = partial(async_mock_session, engine)
        self.analytics_db = GuildAnalyticsClient(factory)
        self.playlist_db = PlaylistClient(factory)
        url = str(test_client.make_url(''))
        manager = RedisManager.from_client(fakeredis.aioredis.FakeRedis(decode_responses=True))
        self.broker = RedisBroker(RedisBrokerRegistry(manager), bucket_name='b')
        queues = GuildQueueRegistry(manager)
        self.queue = GuildQueueBroker(self.broker, queues, record_plays=True)
        self.worker = HistoryWorker(
            queues,
            HttpHistoryPlaylistStore(url, session=test_client.session),
            HttpPlayAnalyticsStore(url, session=test_client.session),
            max_size=MAX_SIZE)

    async def play(self, title: str, *, skipped=False, cache_hit=False, from_history=False, duration=90):
        """Play a track out through the broker, as the gateway would."""
        request = MediaRequest(
            guild_id=GUILD, channel_id=2, requester_name='Alice', requester_id=9,
            search_result=SearchResult(search_type=SearchType.DIRECT,
                                       raw_search_string=f'https://example.com/{title}'))
        request.added_from_history = from_history
        await self.broker.register_request(request)
        download = MediaDownload(
            Path(f'{title}.mp3'),
            {'id': title, 'title': title, 'webpage_url': f'https://example.com/{title}',
             'uploader': 'Someone', 'duration': duration, 'extractor': 'youtube'}, request)
        download.cache_hit = cache_hit
        await self.broker.register_download(download)
        await self.queue.enqueue(GUILD, str(request.uuid))
        await self.queue.claim_next(GUILD, 'gw-1')
        await self.queue.finish(GUILD, str(request.uuid), skipped=skipped, history_cap=10)

    async def drain(self) -> int:
        done = 0
        while await self.worker.process_one() is Outcome.DONE:
            done += 1
        return done

    async def history_titles(self) -> list[str]:
        playlist = await self.playlist_db.get_history_playlist(GUILD)
        return [item.title for item in await self.playlist_db.list_items(playlist.id)]


async def _stack(engine):
    server = DatabaseHttpServer(guild_analytics_store=GuildAnalyticsClient(partial(async_mock_session, engine)),
                                playlist_store=PlaylistClient(partial(async_mock_session, engine)))
    test_client = TestClient(TestServer(server.build_app()))
    await test_client.start_server()
    return _Stack(engine, test_client), test_client


@pytest.mark.asyncio
async def test_plays_reach_the_database_through_the_real_wire(fake_engine):  # pylint:disable=redefined-outer-name
    '''Counts and history rows land where the gateway's own loop used to put them.'''
    stack, client = await _stack(fake_engine)
    try:
        await stack.play('one', duration=7200)
        await stack.play('two', cache_hit=True, duration=3600)
        assert await stack.drain() == 2

        totals = await stack.analytics_db.get_analytics(GUILD)
        assert totals.total_plays == 2
        assert totals.cached_plays == 1
        assert totals.total_duration_seconds == 10800
        assert await stack.history_titles() == ['one', 'two']
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_skipped_and_replayed_tracks_follow_the_old_rules(fake_engine):  # pylint:disable=redefined-outer-name
    '''Skipped: not recorded at all. Replayed from history: counted, not added to history again.'''
    stack, client = await _stack(fake_engine)
    try:
        await stack.play('skipped', skipped=True)
        await stack.play('replayed', from_history=True)
        await stack.play('fresh')
        assert await stack.drain() == 2

        assert (await stack.analytics_db.get_analytics(GUILD)).total_plays == 2
        assert await stack.history_titles() == ['fresh']
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_the_history_ceiling_is_enforced_by_the_db(fake_engine):  # pylint:disable=redefined-outer-name
    '''The worker passes the configured size and the db evicts the oldest to honour it.'''
    stack, client = await _stack(fake_engine)
    try:
        for title in ('a', 'b', 'c', 'd', 'e'):
            await stack.play(title)
        await stack.drain()
        assert await stack.history_titles() == ['c', 'd', 'e']
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_the_loop_drains_to_the_database_and_stops(fake_engine):  # pylint:disable=redefined-outer-name
    '''The same through run(), as the broker pod drives it.'''
    stack, client = await _stack(fake_engine)
    try:
        await stack.play('one')
        stop = asyncio.Event()
        task = asyncio.create_task(stack.worker.run(stop))
        for _ in range(100):
            if (await stack.analytics_db.get_analytics(GUILD)).total_plays == 1:
                break
            await asyncio.sleep(0.02)
        stop.set()
        await asyncio.wait_for(task, timeout=2)
        assert await stack.history_titles() == ['one']
    finally:
        await client.close()
