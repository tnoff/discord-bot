'''Tests for GuildQueueBroker: the player queue joined to the broker entries behind it.'''
import logging
from pathlib import Path
from unittest.mock import AsyncMock

import fakeredis.aioredis
import pytest

from discord_core.clients.redis_client import RedisManager
from discord_core.cogs.music_helpers.common import SearchType
from discord_core.types.media_download import MediaDownload
from discord_core.types.media_request import MediaRequest
from discord_core.types.search import SearchResult

from discord_broker.interfaces.broker_protocols import Zone
from discord_broker.workers.broker_registry import RedisBrokerRegistry
from discord_broker.workers.guild_queue import GuildQueueBroker
from discord_broker.workers.guild_queue_registry import (
    ENQUEUE_CLOSED,
    ENQUEUE_DUPLICATE,
    ENQUEUE_FULL,
    ENQUEUE_OK,
    SKIP_NO_PLAYER,
    SKIP_NOT_CURRENT,
    SKIP_OK,
    GuildQueueRegistry,
)
from discord_broker.workers.redis_broker import RedisBroker, _download_from_dict, _download_to_dict

GUILD = 11
BUCKET = 'media-bucket'


def _make() -> tuple[RedisBroker, GuildQueueBroker]:
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    manager = RedisManager.from_client(client)
    broker = RedisBroker(RedisBrokerRegistry(manager), bucket_name=BUCKET)
    return broker, GuildQueueBroker(broker, GuildQueueRegistry(manager))


def _request(guild_id: int = GUILD, title_hint: str = 'x') -> MediaRequest:
    return MediaRequest(
        guild_id=guild_id,
        channel_id=2,
        requester_name='tester',
        requester_id=9,
        search_result=SearchResult(
            search_type=SearchType.DIRECT,
            raw_search_string=f'https://example.com/{title_hint}',
        ),
    )


def _download(request: MediaRequest, title: str = 'Video', file_path: str | None = 'key.mp3',
              cache_hit: bool = False) -> MediaDownload:
    download = MediaDownload(
        Path(file_path) if file_path else None,
        {
            'id': title,
            'title': title,
            'webpage_url': f'https://example.com/{title}',
            'uploader': 'Someone',
            'duration': 90,
            'extractor': 'youtube',
        },
        request,
    )
    download.cache_hit = cache_hit
    return download


async def _available(broker: RedisBroker, title: str, **kwargs) -> str:
    '''Register a finished download so it is AVAILABLE, and return its uuid.'''
    request = _request(title_hint=title)
    await broker.register_request(request)
    await broker.register_download(_download(request, title, **kwargs))
    return str(request.uuid)


async def _queued(broker: RedisBroker, queues: GuildQueueBroker, *titles: str) -> list[str]:
    uuids = []
    for title in titles:
        uuid = await _available(broker, title)
        assert await queues.enqueue(GUILD, uuid) == ENQUEUE_OK
        uuids.append(uuid)
    return uuids


async def _titles(queues: GuildQueueBroker) -> list[str]:
    return [entry.download.title for entry in (await queues.get_queue(GUILD)).items]


# ---------------------------------------------------------------------------
# cache_hit survives the registry round trip (history and analytics need it)
# ---------------------------------------------------------------------------

def test_cache_hit_round_trips_through_the_entry_dict():
    '''cache_hit used to be dropped on the way into Redis.'''
    request = _request()
    for flag in (True, False):
        stored = _download_to_dict(_download(request, cache_hit=flag))
        assert stored['cache_hit'] is flag
        assert _download_from_dict(stored, request).cache_hit is flag


@pytest.mark.asyncio
async def test_get_entries_batches_and_marks_missing():
    '''Several entries in one call, None for the ones that are gone.'''
    broker, queues = _make()
    [first] = await _queued(broker, queues, 'one')
    entries = await broker.get_entries([first, 'missing'])
    assert entries[0].download.title == 'one'
    assert entries[1] is None
    assert await broker.get_entries([]) == []


def test_entries_stored_before_cache_hit_existed_read_as_a_cache_miss():
    '''Entries already in Redis have no cache_hit key; they must still load.'''
    request = _request()
    stored = _download_to_dict(_download(request))
    del stored['cache_hit']
    assert _download_from_dict(stored, request).cache_hit is False


# ---------------------------------------------------------------------------
# enqueue / read
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_enqueue_reports_each_rejection():
    '''Closed, full and duplicate all come back as results, not exceptions.'''
    broker, queues = _make()
    first = await _available(broker, 'one')
    second = await _available(broker, 'two')
    assert await queues.enqueue(GUILD, first, max_size=1) == ENQUEUE_OK
    assert await queues.enqueue(GUILD, second, max_size=1) == ENQUEUE_FULL
    assert await queues.enqueue(GUILD, first, max_size=5) == ENQUEUE_DUPLICATE
    await queues.close(GUILD)
    assert await queues.enqueue(GUILD, second) == ENQUEUE_CLOSED


@pytest.mark.asyncio
async def test_get_guild_queue_returns_entries_in_play_order():
    '''Reading the queue loads the entry behind every uuid.'''
    broker, queues = _make()
    uuids = await _queued(broker, queues, 'one', 'two', 'three')
    queue = await queues.get_queue(GUILD)
    assert [str(entry.request.uuid) for entry in queue.items] == uuids
    assert [entry.download.title for entry in queue.items] == ['one', 'two', 'three']
    assert all(entry.zone is Zone.AVAILABLE for entry in queue.items)
    assert queue.version == 3
    assert queue.playing is None
    assert queue.skip_for is None
    assert queue.closed is False


@pytest.mark.asyncio
async def test_get_guild_queue_skips_entries_that_are_gone(caplog):
    '''A uuid whose entry expired is left out of the read, with a warning.'''
    broker, queues = _make()
    uuids = await _queued(broker, queues, 'one', 'two')
    await broker._registry.delete_entry(uuids[0])  # pylint: disable=protected-access
    with caplog.at_level(logging.WARNING):
        assert await _titles(queues) == ['two']
    assert uuids[0] in caplog.text


@pytest.mark.asyncio
async def test_poll_reports_version_and_skip():
    '''The gateway's cheap poll passes straight through.'''
    broker, queues = _make()
    assert await queues.poll(GUILD) == (0, None)
    await _queued(broker, queues, 'one')
    assert await queues.poll(GUILD) == (1, None)


# ---------------------------------------------------------------------------
# remove / bump / shuffle / clear
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_remove_returns_the_entry_and_drops_it():
    '''Removing a queued track tells the caller what it was and deletes the entry.'''
    broker, queues = _make()
    uuids = await _queued(broker, queues, 'one', 'two', 'three')
    removed = await queues.remove(GUILD, uuids[1])
    assert removed.download.title == 'two'
    assert await _titles(queues) == ['one', 'three']
    assert await broker.get_entry(uuids[1]) is None


@pytest.mark.asyncio
async def test_remove_of_a_track_not_queued_leaves_its_entry_alone():
    '''If the track already left the queue, its entry (now playing, say) is not touched.'''
    broker, queues = _make()
    uuid = await _available(broker, 'one')
    assert await queues.remove(GUILD, uuid) is None
    assert await broker.get_entry(uuid) is not None


@pytest.mark.asyncio
async def test_remove_when_the_entry_is_already_gone_still_dequeues():
    '''A dangling uuid can still be removed from the list; there is just no entry to report.'''
    broker, queues = _make()
    uuids = await _queued(broker, queues, 'one')
    await broker._registry.delete_entry(uuids[0])  # pylint: disable=protected-access
    assert await queues.remove(GUILD, uuids[0]) is None
    assert (await queues._queues.queue_state(GUILD)).queue == []  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_bump_returns_the_entry_and_moves_it_first():
    '''Bump reports the track and puts it at the head.'''
    broker, queues = _make()
    uuids = await _queued(broker, queues, 'one', 'two', 'three')
    bumped = await queues.bump(GUILD, uuids[2])
    assert bumped.download.title == 'three'
    assert await _titles(queues) == ['three', 'one', 'two']


@pytest.mark.asyncio
async def test_bump_of_a_track_not_queued_is_none():
    '''Bumping something that is not queued reports nothing and changes nothing.'''
    broker, queues = _make()
    await _queued(broker, queues, 'one')
    assert await queues.bump(GUILD, 'nope') is None
    assert await _titles(queues) == ['one']


@pytest.mark.asyncio
async def test_shuffle_keeps_every_entry():
    '''Shuffle reorders the queue without losing anything.'''
    broker, queues = _make()
    await _queued(broker, queues, *[f't{i}' for i in range(8)])
    assert await queues.shuffle(GUILD) is True
    assert sorted(await _titles(queues)) == sorted(f't{i}' for i in range(8))


@pytest.mark.asyncio
async def test_clear_drops_every_queued_entry():
    '''Clear removes the entries, not just the list, and says how many.'''
    broker, queues = _make()
    uuids = await _queued(broker, queues, 'one', 'two')
    assert await queues.clear(GUILD) == 2
    assert await _titles(queues) == []
    for uuid in uuids:
        assert await broker.get_entry(uuid) is None


@pytest.mark.asyncio
async def test_clear_of_an_empty_queue_is_zero():
    '''Nothing queued, nothing cleared.'''
    _, queues = _make()
    assert await queues.clear(GUILD) == 0


# ---------------------------------------------------------------------------
# claim
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_claim_of_an_empty_queue_is_none():
    '''Nothing to play.'''
    _, queues = _make()
    assert await queues.claim_next(GUILD, 'gw-1') is None


@pytest.mark.asyncio
async def test_claim_checks_out_and_marks_playing():
    '''The claimed track is checked out to the guild and recorded as now playing.'''
    broker, queues = _make()
    uuids = await _queued(broker, queues, 'one', 'two')
    claimed = await queues.claim_next(GUILD, 'gw-1')

    assert str(claimed.entry.request.uuid) == uuids[0]
    assert claimed.entry.zone is Zone.CHECKED_OUT
    assert claimed.entry.checked_out_by == GUILD
    assert claimed.checkout.s3_key == 'key.mp3'
    assert claimed.checkout.bucket_name == BUCKET

    queue = await queues.get_queue(GUILD)
    assert [entry.download.title for entry in queue.items] == ['two']
    assert queue.playing.uuid == uuids[0]
    assert queue.playing.gateway_id == 'gw-1'
    assert queue.playing.entry.download.title == 'one'


@pytest.mark.asyncio
async def test_claims_walk_the_queue_in_order():
    '''After one track finishes, the next claim is the next track.'''
    broker, queues = _make()
    uuids = await _queued(broker, queues, 'one', 'two')
    first = await queues.claim_next(GUILD, 'gw-1')
    await queues.finish(GUILD, str(first.entry.request.uuid), skipped=False, history_cap=5)
    second = await queues.claim_next(GUILD, 'gw-1')
    assert str(second.entry.request.uuid) == uuids[1]


@pytest.mark.asyncio
async def test_claim_skips_a_track_whose_entry_vanished(caplog):
    '''An expired entry is dropped and the next track is claimed instead.'''
    broker, queues = _make()
    uuids = await _queued(broker, queues, 'one', 'two')
    await broker._registry.delete_entry(uuids[0])  # pylint: disable=protected-access
    with caplog.at_level(logging.WARNING):
        claimed = await queues.claim_next(GUILD, 'gw-1')
    assert str(claimed.entry.request.uuid) == uuids[1]
    assert 'Dropping unplayable' in caplog.text


@pytest.mark.asyncio
async def test_claim_drops_and_releases_a_track_with_no_file():
    '''An entry with nothing to play is released and the queue moves on.'''
    broker, queues = _make()
    request = _request(title_hint='nofile')
    await broker.register_request(request)
    await broker.register_download(_download(request, 'nofile', file_path=None))
    assert await queues.enqueue(GUILD, str(request.uuid)) == ENQUEUE_OK
    good = await _queued(broker, queues, 'good')

    claimed = await queues.claim_next(GUILD, 'gw-1')

    assert str(claimed.entry.request.uuid) == good[0]
    assert await broker.get_entry(str(request.uuid)) is None


@pytest.mark.asyncio
async def test_claim_ends_when_only_unplayable_tracks_remain():
    '''A queue of nothing playable drains to None rather than looping.'''
    broker, queues = _make()
    uuids = await _queued(broker, queues, 'one')
    await broker._registry.delete_entry(uuids[0])  # pylint: disable=protected-access
    assert await queues.claim_next(GUILD, 'gw-1') is None


@pytest.mark.asyncio
async def test_claim_recovers_a_track_a_dead_gateway_left_half_started():
    '''Popped and checked out but never confirmed: the retry gets that same track back.'''
    broker, queues = _make()
    uuids = await _queued(broker, queues, 'one', 'two')
    assert await queues._queues.queue_claim_next(GUILD) == uuids[0]  # pylint: disable=protected-access
    assert await broker.checkout(uuids[0], GUILD) is not None  # ...then the gateway died

    claimed = await queues.claim_next(GUILD, 'gw-2')

    assert str(claimed.entry.request.uuid) == uuids[0]
    assert claimed.checkout.s3_key == 'key.mp3'
    assert (await queues.get_queue(GUILD)).playing.gateway_id == 'gw-2'


@pytest.mark.asyncio
async def test_claim_drops_a_half_started_track_that_has_no_file():
    '''Recovery only helps if there is something to play; a checked-out entry with no file is dropped.'''
    broker, queues = _make()
    request = _request(title_hint='nofile')
    await broker.register_request(request)
    await broker.register_download(_download(request, 'nofile', file_path=None))
    uuid = str(request.uuid)
    assert await queues.enqueue(GUILD, uuid) == ENQUEUE_OK
    assert await queues._queues.queue_claim_next(GUILD) == uuid  # pylint: disable=protected-access
    assert await broker._registry.atomic_checkout(uuid, GUILD) is True  # pylint: disable=protected-access

    assert await queues.claim_next(GUILD, 'gw-1') is None

    assert await broker.get_entry(uuid) is None


@pytest.mark.asyncio
async def test_claim_does_not_steal_a_track_checked_out_by_another_guild():
    '''An entry held by a different guild is not ours to play; it is dropped from this queue.'''
    broker, queues = _make()
    uuid = (await _queued(broker, queues, 'one'))[0]
    await queues._queues.queue_claim_next(GUILD)  # pylint: disable=protected-access
    assert await broker.checkout(uuid, GUILD + 1) is not None
    assert await queues.claim_next(GUILD, 'gw-1') is None


@pytest.mark.asyncio
async def test_claim_that_loses_the_guild_mid_flight_releases_the_entry():
    '''If the guild closes while a track is being checked out, nothing is left checked out.'''
    broker, queues = _make()
    uuid = (await _queued(broker, queues, 'one'))[0]
    queues._queues.confirm_claim = AsyncMock(return_value=False)  # pylint: disable=protected-access
    assert await queues.claim_next(GUILD, 'gw-1') is None
    assert await broker.get_entry(uuid) is None


# ---------------------------------------------------------------------------
# skip / heartbeat / finish
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_skip_follows_the_playing_track():
    '''A skip works for the playing track, not for anything else.'''
    broker, queues = _make()
    uuids = await _queued(broker, queues, 'one', 'two')
    assert await queues.skip(GUILD, uuids[0]) == SKIP_NO_PLAYER
    await queues.claim_next(GUILD, 'gw-1')
    assert await queues.skip(GUILD, uuids[1]) == SKIP_NOT_CURRENT
    assert await queues.skip(GUILD, uuids[0]) == SKIP_OK
    assert (await queues.get_queue(GUILD)).skip_for == uuids[0]
    # two enqueues and one claim
    assert await queues.poll(GUILD) == (3, uuids[0])


@pytest.mark.asyncio
async def test_heartbeat_follows_the_playing_track():
    '''The heartbeat only holds for the track that is actually playing.'''
    broker, queues = _make()
    uuids = await _queued(broker, queues, 'one')
    assert await queues.heartbeat(GUILD, uuids[0]) is False
    await queues.claim_next(GUILD, 'gw-1')
    assert await queues.heartbeat(GUILD, uuids[0]) is True


@pytest.mark.asyncio
async def test_finish_records_history_and_releases_the_entry():
    '''A track that played out is remembered in full, then its entry goes away.'''
    broker, queues = _make()
    request = _request(title_hint='hist')
    request.added_from_history = True
    await broker.register_request(request)
    await broker.register_download(_download(request, 'hist', cache_hit=True))
    await queues.enqueue(GUILD, str(request.uuid))
    await queues.claim_next(GUILD, 'gw-1')

    await queues.finish(GUILD, str(request.uuid), skipped=False, history_cap=5)

    [item] = await queues.get_history(GUILD)
    assert item['uuid'] == str(request.uuid)
    assert item['guild_id'] == GUILD
    assert item['requester_name'] == 'tester'
    assert item['requester_id'] == 9
    assert item['added_from_history'] is True
    assert item['title'] == 'hist'
    assert item['webpage_url'] == 'https://example.com/hist'
    assert item['uploader'] == 'Someone'
    assert item['duration'] == 90
    assert item['cache_hit'] is True
    assert await broker.get_entry(str(request.uuid)) is None
    assert (await queues.get_queue(GUILD)).playing is None


@pytest.mark.asyncio
async def test_finish_skipped_track_releases_without_history():
    '''A skipped track is cleaned up but not remembered.'''
    broker, queues = _make()
    uuid = (await _queued(broker, queues, 'one'))[0]
    await queues.claim_next(GUILD, 'gw-1')
    await queues.skip(GUILD, uuid)

    await queues.finish(GUILD, uuid, skipped=True, history_cap=5)

    assert await queues.get_history(GUILD) == []
    assert await broker.get_entry(uuid) is None
    assert (await queues.get_queue(GUILD)).skip_for is None


@pytest.mark.asyncio
async def test_finish_when_the_entry_is_already_gone_still_clears_playing():
    '''No entry means no history item, but the now-playing record must still clear.'''
    broker, queues = _make()
    uuid = (await _queued(broker, queues, 'one'))[0]
    await queues.claim_next(GUILD, 'gw-1')
    await broker._registry.delete_entry(uuid)  # pylint: disable=protected-access

    await queues.finish(GUILD, uuid, skipped=False, history_cap=5)

    assert await queues.get_history(GUILD) == []
    assert (await queues.get_queue(GUILD)).playing is None


@pytest.mark.asyncio
async def test_playing_entry_gone_is_reported_without_entry():
    '''If the playing entry expired, the playing record still reads, with no entry attached.'''
    broker, queues = _make()
    uuid = (await _queued(broker, queues, 'one'))[0]
    await queues.claim_next(GUILD, 'gw-1')
    await broker._registry.delete_entry(uuid)  # pylint: disable=protected-access
    playing = (await queues.get_queue(GUILD)).playing
    assert playing.uuid == uuid
    assert playing.entry is None


# ---------------------------------------------------------------------------
# close / open
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_close_releases_queued_claimed_and_playing_entries():
    '''Shutting a player down leaves no entry held on its behalf.'''
    broker, queues = _make()
    uuids = await _queued(broker, queues, 'playing', 'queued-1', 'queued-2')
    await queues.claim_next(GUILD, 'gw-1')
    late = await _available(broker, 'late')
    await queues.enqueue(GUILD, late)
    # Leave queued-1 popped but unconfirmed, as if the gateway died mid-claim.
    assert await queues._queues.queue_claim_next(GUILD) == uuids[1]  # pylint: disable=protected-access
    # Held: playing, the unconfirmed claim, and two still queued (queued-2, late).
    assert await queues.close(GUILD) == 4

    for uuid in uuids + [late]:
        assert await broker.get_entry(uuid) is None
    queue = await queues.get_queue(GUILD)
    assert queue.closed is True
    assert queue.items == []
    assert queue.playing is None


@pytest.mark.asyncio
async def test_close_of_an_idle_guild_releases_nothing():
    '''Nothing held, nothing released.'''
    _, queues = _make()
    assert await queues.close(GUILD) == 0


@pytest.mark.asyncio
async def test_open_lets_the_next_player_enqueue():
    '''A fresh player reopens the guild its predecessor closed.'''
    broker, queues = _make()
    await queues.close(GUILD)
    uuid = await _available(broker, 'one')
    assert await queues.enqueue(GUILD, uuid) == ENQUEUE_CLOSED
    await queues.open(GUILD)
    assert await queues.enqueue(GUILD, uuid) == ENQUEUE_OK
