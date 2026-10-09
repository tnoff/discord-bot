'''Tests for the play-order message GuildQueueBroker keeps up to date through its dispatcher.'''
from pathlib import Path

import fakeredis.aioredis
import pytest

from discord_core.clients.redis_client import RedisManager
from discord_core.cogs.music_helpers.common import MultipleMutableType, SearchType
from discord_core.types.media_download import MediaDownload
from discord_core.types.media_request import MediaRequest
from discord_core.types.search import SearchResult

from discord_broker.workers.broker_registry import RedisBrokerRegistry
from discord_broker.workers.guild_queue import GuildQueueBroker
from discord_broker.workers.guild_queue_registry import GuildQueueRegistry
from discord_broker.workers.redis_broker import RedisBroker

GUILD = 21
CHANNEL = 555
KEY = f'{MultipleMutableType.PLAY_ORDER.value}-{GUILD}'


class FakeDispatcher:
    '''Records what the broker pushes, in the shape of the dispatch client.'''

    def __init__(self):
        self.updates = []
        self.moves = []

    def update_mutable(self, key, guild_id, content, channel_id, **_options):
        self.updates.append({'key': key, 'guild_id': guild_id, 'content': content,
                             'channel_id': channel_id})

    def update_mutable_channel(self, key, guild_id, new_channel_id):
        self.moves.append({'key': key, 'guild_id': guild_id, 'channel_id': new_channel_id})

    def remove_mutable(self, key):
        raise AssertionError('the broker clears the message by rendering empty content')

    def send_message(self, *args, **kwargs):
        raise AssertionError('the play-order message is a mutable, not a plain message')

    @property
    def last(self):
        return self.updates[-1]


def _make(dispatcher=None) -> tuple[RedisBroker, GuildQueueBroker]:
    manager = RedisManager.from_client(fakeredis.aioredis.FakeRedis(decode_responses=True))
    broker = RedisBroker(RedisBrokerRegistry(manager), bucket_name='b')
    return broker, GuildQueueBroker(broker, GuildQueueRegistry(manager), dispatcher)


async def _track(broker, title: str) -> str:
    request = MediaRequest(
        guild_id=GUILD, channel_id=2, requester_name='Alice', requester_id=9,
        search_result=SearchResult(search_type=SearchType.DIRECT,
                                   raw_search_string=f'https://example.com/{title}'))
    await broker.register_request(request)
    await broker.register_download(MediaDownload(
        Path(f'{title}.mp3'),
        {'id': title, 'title': title, 'webpage_url': f'https://example.com/{title}',
         'uploader': 'Someone', 'duration': 90, 'extractor': 'youtube'}, request))
    return str(request.uuid)


async def _opened(dispatcher):
    broker, queues = _make(dispatcher)
    await queues.open(GUILD, CHANNEL)
    return broker, queues


# ---------------------------------------------------------------------------
# what triggers a render
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_enqueue_shows_the_queue_in_the_guilds_channel():
    '''Queueing a track puts the table up, keyed per guild and aimed at the channel from open.'''
    dispatcher = FakeDispatcher()
    broker, queues = await _opened(dispatcher)
    await queues.enqueue(GUILD, await _track(broker, 'one'))
    assert dispatcher.last['key'] == KEY
    assert dispatcher.last['guild_id'] == GUILD
    assert dispatcher.last['channel_id'] == CHANNEL
    [table] = dispatcher.last['content']
    assert 'one' in table
    assert '1  || 00:00' in table


@pytest.mark.asyncio
@pytest.mark.parametrize('rejection', ['duplicate', 'full', 'closed'])
async def test_a_rejected_enqueue_changes_nothing_so_renders_nothing(rejection):
    '''Nothing about the queue changed, so there is nothing new to show.'''
    dispatcher = FakeDispatcher()
    broker, queues = await _opened(dispatcher)
    first = await _track(broker, 'one')
    second = await _track(broker, 'two')
    await queues.enqueue(GUILD, first, max_size=1)
    shown = len(dispatcher.updates)
    if rejection == 'duplicate':
        assert await queues.enqueue(GUILD, first, max_size=5) == 'duplicate'
    elif rejection == 'full':
        assert await queues.enqueue(GUILD, second, max_size=1) == 'full'
    else:
        await queues.close(GUILD)
        shown = len(dispatcher.updates)
        assert await queues.enqueue(GUILD, second) == 'closed'
    assert len(dispatcher.updates) == shown


@pytest.mark.asyncio
async def test_remove_bump_shuffle_and_clear_each_re_render():
    '''Every change to the queue's contents or order is reflected.'''
    dispatcher = FakeDispatcher()
    broker, queues = await _opened(dispatcher)
    uuids = [await _track(broker, name) for name in ('one', 'two', 'three')]
    for uuid in uuids:
        await queues.enqueue(GUILD, uuid)

    shown = len(dispatcher.updates)
    await queues.bump(GUILD, uuids[2])
    assert len(dispatcher.updates) == shown + 1
    assert dispatcher.last['content'][0].split('\n')[2].split('||')[2].strip() == 'three'

    await queues.remove(GUILD, uuids[1])
    assert len(dispatcher.updates) == shown + 2
    assert 'two' not in dispatcher.last['content'][0]

    await queues.shuffle(GUILD)
    assert len(dispatcher.updates) == shown + 3

    await queues.clear(GUILD)
    assert len(dispatcher.updates) == shown + 4
    assert dispatcher.last['content'] == []


@pytest.mark.asyncio
async def test_operations_that_find_nothing_to_do_render_nothing():
    '''A miss, or clearing an empty queue, is not a change.'''
    dispatcher = FakeDispatcher()
    broker, queues = await _opened(dispatcher)
    await queues.enqueue(GUILD, await _track(broker, 'one'))
    shown = len(dispatcher.updates)
    assert await queues.remove(GUILD, 'not-queued') is None
    assert await queues.bump(GUILD, 'not-queued') is None
    await queues.clear(GUILD)
    shown += 1  # the clear that emptied it
    assert await queues.clear(GUILD) == 0
    assert len(dispatcher.updates) == shown


@pytest.mark.asyncio
async def test_claiming_shows_now_playing_and_takes_the_track_out_of_the_table():
    '''Track start: a "Now playing" line, and the table is what is still waiting.'''
    dispatcher = FakeDispatcher()
    broker, queues = await _opened(dispatcher)
    await queues.enqueue(GUILD, await _track(broker, 'first'))
    await queues.enqueue(GUILD, await _track(broker, 'second'))

    assert await queues.claim_next(GUILD, 'gw-1') is not None

    playing, table = dispatcher.last['content']
    assert playing == 'Now playing https://example.com/first requested by Alice'
    assert 'second' in table
    assert 'first' not in table
    assert '1  || 01:30' in table  # waits for the 90s track that is playing


@pytest.mark.asyncio
async def test_claiming_from_an_empty_queue_renders_nothing():
    '''No track started, so the message is unchanged.'''
    dispatcher = FakeDispatcher()
    _, queues = await _opened(dispatcher)
    assert await queues.claim_next(GUILD, 'gw-1') is None
    assert not dispatcher.updates


@pytest.mark.asyncio
async def test_finishing_the_last_track_clears_the_message():
    '''Nothing playing and nothing queued renders as empty content: the message comes down.'''
    dispatcher = FakeDispatcher()
    broker, queues = await _opened(dispatcher)
    uuid = await _track(broker, 'one')
    await queues.enqueue(GUILD, uuid)
    await queues.claim_next(GUILD, 'gw-1')
    await queues.finish(GUILD, uuid, skipped=False, history_cap=5)
    assert dispatcher.last['content'] == []


@pytest.mark.asyncio
async def test_finishing_with_more_queued_drops_the_now_playing_line():
    '''Between tracks the message shows only what is waiting.'''
    dispatcher = FakeDispatcher()
    broker, queues = await _opened(dispatcher)
    first, second = await _track(broker, 'one'), await _track(broker, 'two')
    await queues.enqueue(GUILD, first)
    await queues.enqueue(GUILD, second)
    await queues.claim_next(GUILD, 'gw-1')
    await queues.finish(GUILD, first, skipped=True, history_cap=5)
    [table] = dispatcher.last['content']
    assert 'two' in table
    assert not table.startswith('Now playing')


@pytest.mark.asyncio
async def test_closing_takes_the_message_down():
    '''Shutting the player down renders empty, in the channel it was put up in.'''
    dispatcher = FakeDispatcher()
    broker, queues = await _opened(dispatcher)
    await queues.enqueue(GUILD, await _track(broker, 'one'))
    await queues.close(GUILD)
    assert dispatcher.last['content'] == []
    assert dispatcher.last['channel_id'] == CHANNEL


# ---------------------------------------------------------------------------
# opening
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_first_open_renders_nothing_and_moves_nothing():
    '''There is no message yet to show or move.'''
    dispatcher = FakeDispatcher()
    await _opened(dispatcher)
    assert not dispatcher.updates
    assert not dispatcher.moves


@pytest.mark.asyncio
async def test_opening_with_another_channel_moves_the_existing_message():
    '''move-messages is just an open with a new channel.'''
    dispatcher = FakeDispatcher()
    _, queues = await _opened(dispatcher)
    assert await queues.open(GUILD, 999) is None
    assert dispatcher.moves == [{'key': KEY, 'guild_id': GUILD, 'channel_id': 999}]


@pytest.mark.asyncio
async def test_opening_again_with_the_same_channel_moves_nothing():
    '''Re-asserting ownership of a guild is not a move.'''
    dispatcher = FakeDispatcher()
    _, queues = await _opened(dispatcher)
    await queues.open(GUILD, CHANNEL)
    assert not dispatcher.moves


@pytest.mark.asyncio
async def test_opening_a_guild_whose_gateway_died_recovers_and_shows_the_track():
    '''The interrupted track is back at the head, and the message says so.'''
    dispatcher = FakeDispatcher()
    broker, queues = await _opened(dispatcher)
    uuids = [await _track(broker, 'one'), await _track(broker, 'two')]
    for uuid in uuids:
        await queues.enqueue(GUILD, uuid)
    await queues.claim_next(GUILD, 'gw-old')
    await queues._queues._client.delete(  # pylint: disable=protected-access
        f'discord_bot:broker:gplaying:{GUILD}')  # the heartbeat TTL ran out

    assert await queues.open(GUILD, CHANNEL) == uuids[0]

    [table] = dispatcher.last['content']
    assert table.split('\n')[2].split('||')[2].strip() == 'one'
    queue = await queues.get_queue(GUILD)
    assert [str(entry.request.uuid) for entry in queue.items] == uuids
    claimed = await queues.claim_next(GUILD, 'gw-new')
    assert str(claimed.entry.request.uuid) == uuids[0]


@pytest.mark.asyncio
async def test_the_queue_read_carries_the_text_channel():
    '''The gateway and the move-messages check read it from here.'''
    _, queues = await _opened(None)
    assert (await queues.get_queue(GUILD)).text_channel_id == CHANNEL


# ---------------------------------------------------------------------------
# when there is nowhere to send it
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_without_a_dispatcher_the_queue_works_and_says_nothing():
    '''No dispatcher disables the message; it must not disable the queue.'''
    broker, queues = await _opened(None)
    uuid = await _track(broker, 'one')
    assert await queues.enqueue(GUILD, uuid) == 'ok'
    assert await queues.claim_next(GUILD, 'gw-1') is not None
    await queues.finish(GUILD, uuid, skipped=False, history_cap=5)
    await queues.open(GUILD, 999)
    assert await queues.close(GUILD) == 0


@pytest.mark.asyncio
async def test_a_guild_nobody_opened_has_no_channel_so_no_message():
    '''Without a channel there is nowhere to put it, and that is not an error.'''
    dispatcher = FakeDispatcher()
    broker, queues = _make(dispatcher)
    assert await queues.enqueue(GUILD, await _track(broker, 'one')) == 'ok'
    assert not dispatcher.updates
