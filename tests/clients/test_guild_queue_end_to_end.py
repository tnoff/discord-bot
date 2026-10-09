'''
The guild-queue contract, end to end: the real HttpBrokerClient against the real BrokerHttpServer.

discord_core's own tests check the client against a stub broker, and discord_broker's check the
server with raw requests.  Neither can notice the two disagreeing, so this does: one flow of a
guild's player through every route, over HTTP, with only Redis replaced (by fakeredis).
'''
import contextlib
from pathlib import Path

import aiohttp
import pytest
from aiohttp.test_utils import TestServer

from discord_core.clients.http_broker_client import HttpBrokerClient
from discord_core.cogs.music_helpers.common import SearchType
from discord_core.types.media_download import MediaDownload
from discord_core.types.media_request import MediaRequest
from discord_core.types.search import SearchResult

from tests.fakes.asyncio_queues import make_broker_http_server, make_guild_queue_broker

GUILD = 31
BUCKET = 'test-bucket'


@contextlib.asynccontextmanager
async def _stack():
    '''A client talking to a server that holds a real (fakeredis) guild queue.'''
    queue = make_guild_queue_broker(BUCKET)
    server = make_broker_http_server(queue.broker, guild_queue=queue)
    async with TestServer(server.build_app()) as running, aiohttp.ClientSession() as session:
        yield HttpBrokerClient(str(running.make_url('')), bucket_name=BUCKET, session=session), queue.broker


async def _track(broker, title: str, cache_hit: bool = False) -> str:
    '''Register a finished download with the broker; returns its uuid.'''
    request = MediaRequest(
        guild_id=GUILD, channel_id=2, requester_name='tester', requester_id=9,
        search_result=SearchResult(search_type=SearchType.DIRECT,
                                   raw_search_string=f'https://example.com/{title}'),
    )
    await broker.register_request(request)
    download = MediaDownload(
        Path(f'{title}.mp3'),
        {'id': title, 'title': title, 'webpage_url': f'https://example.com/{title}',
         'uploader': 'Someone', 'duration': 90, 'extractor': 'youtube'},
        request,
    )
    download.cache_hit = cache_hit
    await broker.register_download(download)
    return str(request.uuid)


@pytest.mark.asyncio
async def test_a_players_whole_life_over_http():
    '''Queue three tracks, reorder, play one, skip it, play on, then shut down.'''
    async with _stack() as (client, broker):
        one, two, three = [await _track(broker, name) for name in ('one', 'two', 'three')]

        # Enqueue, with the cap honoured and a repeat refused.
        for uuid in (one, two, three):
            assert await client.enqueue_track(GUILD, uuid, max_size=3) == 'ok'
        late = await _track(broker, 'late')
        assert await client.enqueue_track(GUILD, late, max_size=3) == 'full'
        assert await client.enqueue_track(GUILD, one, max_size=9) == 'duplicate'

        # Read it back as real downloads, in order.
        queue = await client.get_guild_queue(GUILD)
        assert [item.title for item in queue.items] == ['one', 'two', 'three']
        assert [str(item.media_request.uuid) for item in queue.items] == [one, two, three]
        assert queue.playing is None
        assert queue.closed is False

        # Bump and remove report the track they touched; a miss reports None.
        assert (await client.bump_queued_track(GUILD, three)).title == 'three'
        assert (await client.remove_queued_track(GUILD, two)).title == 'two'
        assert await client.remove_queued_track(GUILD, two) is None
        assert await client.bump_queued_track(GUILD, 'not-queued') is None
        assert [item.title for item in (await client.get_guild_queue(GUILD)).items] == ['three', 'one']

        # Shuffle keeps the tracks; the version moved with every change above.
        assert await client.shuffle_queue(GUILD) is True
        shuffled = await client.get_guild_queue(GUILD)
        assert sorted(item.title for item in shuffled.items) == ['one', 'three']

        # Claim plays the head: s3 location comes with the configured bucket.
        head = shuffled.items[0]
        claimed = await client.claim_next_track(GUILD, 'gw-1')
        assert claimed.download.title == head.title
        assert claimed.checkout.s3_key == f'{head.title}.mp3'
        assert claimed.checkout.bucket_name == BUCKET
        playing_uuid = str(claimed.download.media_request.uuid)
        assert await client.playing_heartbeat(GUILD, playing_uuid) is True
        assert await client.playing_heartbeat(GUILD, 'someone-else') is False
        playing = (await client.get_guild_queue(GUILD)).playing
        assert (playing.uuid, playing.gateway_id) == (playing_uuid, 'gw-1')
        assert playing.download.title == head.title

        # Skip names its track; the poller sees it; finish clears it and keeps no history.
        assert await client.skip_track(GUILD, 'someone-else') == 'not_current'
        assert await client.skip_track(GUILD, playing_uuid) == 'ok'
        _, skip_for = await client.poll_guild_queue(GUILD)
        assert skip_for == playing_uuid
        await client.finish_track(GUILD, playing_uuid, skipped=True, history_cap=10)
        assert await client.get_guild_history(GUILD) == []
        assert (await client.get_guild_queue(GUILD)).skip_for is None

        # The next track plays out and is remembered.
        nxt = await client.claim_next_track(GUILD, 'gw-1')
        next_uuid = str(nxt.download.media_request.uuid)
        await client.finish_track(GUILD, next_uuid, skipped=False, history_cap=10)
        [record] = await client.get_guild_history(GUILD)
        assert record['uuid'] == next_uuid
        assert record['title'] == nxt.download.title
        assert record['requester_name'] == 'tester'
        assert record['cache_hit'] is False

        # Nothing left to claim.
        assert await client.claim_next_track(GUILD, 'gw-1') is None


@pytest.mark.asyncio
async def test_polling_sees_only_what_changed():
    '''The poll is a 204 until the version moves or a skip is pending.'''
    async with _stack() as (client, broker):
        uuid = await _track(broker, 'one')
        version, skip_for = await client.poll_guild_queue(GUILD)
        assert (version, skip_for) == (0, None)
        assert await client.poll_guild_queue(GUILD, since=version) is None

        await client.enqueue_track(GUILD, uuid)
        moved, _ = await client.poll_guild_queue(GUILD, since=version)
        assert moved == version + 1
        assert await client.poll_guild_queue(GUILD, since=moved) is None

        await client.claim_next_track(GUILD, 'gw-1')
        await client.skip_track(GUILD, uuid)
        claimed_version, skip_for = await client.poll_guild_queue(GUILD)
        # A pending skip is news even when the version has not moved.
        assert await client.poll_guild_queue(GUILD, since=claimed_version) == (claimed_version, uuid)


@pytest.mark.asyncio
async def test_clear_close_and_reopen():
    '''Clear drops the queue, close refuses more, open takes tracks again.'''
    async with _stack() as (client, broker):
        uuids = [await _track(broker, name) for name in ('one', 'two', 'three')]
        for uuid in uuids:
            await client.enqueue_track(GUILD, uuid)
        assert await client.clear_queue(GUILD) == 3
        assert (await client.get_guild_queue(GUILD)).items == []

        again = await _track(broker, 'again')
        await client.enqueue_track(GUILD, again)
        assert await client.close_guild(GUILD) == 1
        assert (await client.get_guild_queue(GUILD)).closed is True
        assert await client.enqueue_track(GUILD, await _track(broker, 'refused')) == 'closed'

        await client.open_guild(GUILD)
        assert await client.enqueue_track(GUILD, await _track(broker, 'welcome')) == 'ok'


@pytest.mark.asyncio
async def test_history_keeps_cache_hit():
    '''cache_hit survives enqueue -> claim -> finish, which the history worker will need.'''
    async with _stack() as (client, broker):
        uuid = await _track(broker, 'cached', cache_hit=True)
        await client.enqueue_track(GUILD, uuid)
        claimed = await client.claim_next_track(GUILD, 'gw-1')
        assert claimed.download.cache_hit is True
        await client.finish_track(GUILD, uuid, skipped=False, history_cap=5)
        assert (await client.get_guild_history(GUILD))[0]['cache_hit'] is True


@pytest.mark.asyncio
async def test_guilds_do_not_see_each_other():
    '''Two guilds on one broker keep separate queues.'''
    async with _stack() as (client, broker):
        await client.enqueue_track(1, await _track(broker, 'a'))
        await client.enqueue_track(2, await _track(broker, 'b'))
        assert [i.title for i in (await client.get_guild_queue(1)).items] == ['a']
        assert [i.title for i in (await client.get_guild_queue(2)).items] == ['b']
