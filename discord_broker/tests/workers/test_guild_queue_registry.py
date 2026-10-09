'''Tests for GuildQueueRegistry, the Redis side of the per-guild player queue.'''
import asyncio
import json
import random
from unittest.mock import AsyncMock

import fakeredis.aioredis
import pytest

from discord_core.clients.redis_client import RedisManager

from discord_broker.workers import guild_queue_registry
from discord_broker.workers.guild_queue_registry import (
    ENQUEUE_CLOSED,
    ENQUEUE_DUPLICATE,
    ENQUEUE_FULL,
    ENQUEUE_OK,
    GCLAIM_KEY_PREFIX,
    GCHANNEL_KEY_PREFIX,
    GCLOSED_KEY_PREFIX,
    GCURRENT_KEY_PREFIX,
    GPLAYING_KEY_PREFIX,
    GQUEUE_KEY_PREFIX,
    GSKIP_KEY_PREFIX,
    GVERSION_KEY_PREFIX,
    HISTORY_EVENTS_KEY,
    GuildQueueRegistry,
    GuildQueueState,
    SKIP_NO_PLAYER,
    SKIP_NOT_CURRENT,
    SKIP_OK,
)

GUILD = 7


def _make() -> tuple[GuildQueueRegistry, fakeredis.aioredis.FakeRedis]:
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    return GuildQueueRegistry(RedisManager.from_client(client)), client


async def _fill(reg: GuildQueueRegistry, *uuids: str, guild: int = GUILD) -> None:
    for uuid in uuids:
        assert await reg.queue_enqueue(guild, uuid) == ENQUEUE_OK


async def _version(client, guild: int = GUILD) -> int:
    raw = await client.get(f'{GVERSION_KEY_PREFIX}{guild}')
    return int(raw) if raw else 0


# ---------------------------------------------------------------------------
# enqueue
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_enqueue_appends_in_order_and_bumps_version():
    '''Items queue first-in first-out and each enqueue moves the version.'''
    reg, client = _make()
    await _fill(reg, 'a', 'b', 'c')
    assert await client.lrange(f'{GQUEUE_KEY_PREFIX}{GUILD}', 0, -1) == ['a', 'b', 'c']
    assert await _version(client) == 3


@pytest.mark.asyncio
async def test_enqueue_sets_ttl_on_queue_and_version():
    '''Both keys expire so an abandoned guild does not squat in Redis.'''
    reg, client = _make()
    await _fill(reg, 'a')
    assert 0 < await client.ttl(f'{GQUEUE_KEY_PREFIX}{GUILD}') <= guild_queue_registry.GUILD_QUEUE_TTL_SECONDS
    assert 0 < await client.ttl(f'{GVERSION_KEY_PREFIX}{GUILD}') <= guild_queue_registry.GUILD_QUEUE_TTL_SECONDS


@pytest.mark.asyncio
async def test_enqueue_rejects_when_full():
    '''max_size is a hard cap; a rejected enqueue changes nothing.'''
    reg, client = _make()
    assert await reg.queue_enqueue(GUILD, 'a', 2) == ENQUEUE_OK
    assert await reg.queue_enqueue(GUILD, 'b', 2) == ENQUEUE_OK
    version = await _version(client)
    assert await reg.queue_enqueue(GUILD, 'c', 2) == ENQUEUE_FULL
    assert await client.llen(f'{GQUEUE_KEY_PREFIX}{GUILD}') == 2
    assert await _version(client) == version


@pytest.mark.asyncio
async def test_enqueue_zero_max_size_is_unbounded():
    '''max_size 0 means no cap.'''
    reg, client = _make()
    for index in range(20):
        assert await reg.queue_enqueue(GUILD, f'u{index}', 0) == ENQUEUE_OK
    assert await client.llen(f'{GQUEUE_KEY_PREFIX}{GUILD}') == 20


@pytest.mark.asyncio
async def test_enqueue_rejects_duplicate_uuid():
    '''A uuid can be queued once; removal by uuid would otherwise be ambiguous.'''
    reg, client = _make()
    await _fill(reg, 'a')
    assert await reg.queue_enqueue(GUILD, 'a') == ENQUEUE_DUPLICATE
    assert await client.llen(f'{GQUEUE_KEY_PREFIX}{GUILD}') == 1


@pytest.mark.asyncio
async def test_enqueue_rejects_when_closed():
    '''A closed guild refuses new tracks (the old PutsBlocked).'''
    reg, client = _make()
    await reg.queue_close(GUILD)
    assert await reg.queue_enqueue(GUILD, 'a') == ENQUEUE_CLOSED
    assert await client.llen(f'{GQUEUE_KEY_PREFIX}{GUILD}') == 0


@pytest.mark.asyncio
async def test_queues_are_per_guild():
    '''One guild's queue and version never show up in another's.'''
    reg, _ = _make()
    await _fill(reg, 'a', guild=1)
    await _fill(reg, 'b', guild=2)
    assert (await reg.queue_state(1)).queue == ['a']
    assert (await reg.queue_state(2)).queue == ['b']


@pytest.mark.asyncio
async def test_concurrent_enqueues_respect_max_size():
    '''Racing enqueues cannot overshoot the cap: the check and the push are one step.'''
    reg, client = _make()
    results = await asyncio.gather(*[reg.queue_enqueue(GUILD, f'u{i}', 5) for i in range(30)])
    assert results.count(ENQUEUE_OK) == 5
    assert results.count(ENQUEUE_FULL) == 25
    assert await client.llen(f'{GQUEUE_KEY_PREFIX}{GUILD}') == 5


# ---------------------------------------------------------------------------
# remove / bump
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_remove_takes_item_out_and_bumps_version():
    '''Removing a queued uuid closes the gap and moves the version.'''
    reg, client = _make()
    await _fill(reg, 'a', 'b', 'c')
    before = await _version(client)
    assert await reg.queue_remove(GUILD, 'b') is True
    assert (await reg.queue_state(GUILD)).queue == ['a', 'c']
    assert await _version(client) == before + 1


@pytest.mark.asyncio
async def test_remove_missing_item_changes_nothing():
    '''A uuid that already left the queue is reported, not an error, and the version holds.'''
    reg, client = _make()
    await _fill(reg, 'a')
    before = await _version(client)
    assert await reg.queue_remove(GUILD, 'gone') is False
    assert await _version(client) == before


@pytest.mark.asyncio
async def test_bump_moves_item_to_head():
    '''Bump puts the item next to play, keeping the rest in order.'''
    reg, client = _make()
    await _fill(reg, 'a', 'b', 'c')
    before = await _version(client)
    assert await reg.queue_bump(GUILD, 'c') is True
    assert (await reg.queue_state(GUILD)).queue == ['c', 'a', 'b']
    assert await _version(client) == before + 1


@pytest.mark.asyncio
async def test_bump_missing_item_changes_nothing():
    '''Bumping something not queued leaves the queue and version alone.'''
    reg, client = _make()
    await _fill(reg, 'a', 'b')
    before = await _version(client)
    assert await reg.queue_bump(GUILD, 'gone') is False
    assert (await reg.queue_state(GUILD)).queue == ['a', 'b']
    assert await _version(client) == before


# ---------------------------------------------------------------------------
# shuffle / clear
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_shuffle_keeps_the_same_items():
    '''Shuffle reorders but never drops or duplicates, and moves the version.'''
    reg, client = _make()
    uuids = [f'u{i}' for i in range(10)]
    await _fill(reg, *uuids)
    before = await _version(client)
    assert await reg.queue_shuffle(GUILD, rng=random.Random(4)) is True
    shuffled = (await reg.queue_state(GUILD)).queue
    assert sorted(shuffled) == sorted(uuids)
    assert shuffled != uuids
    assert await _version(client) == before + 1


@pytest.mark.asyncio
async def test_shuffle_applies_the_drawn_permutation():
    '''The order is exactly what the rng drew, so it is testable and reproducible.'''
    reg, _ = _make()
    await _fill(reg, 'a', 'b', 'c', 'd')
    expected_permutation = random.Random(1).sample(range(1, 5), 4)
    await reg.queue_shuffle(GUILD, rng=random.Random(1))
    assert (await reg.queue_state(GUILD)).queue == [['a', 'b', 'c', 'd'][i - 1] for i in expected_permutation]


@pytest.mark.asyncio
async def test_shuffle_of_empty_and_single_queue_succeeds_without_a_version_bump():
    '''Nothing to reorder is still a successful shuffle, like the in-process queue.'''
    reg, client = _make()
    assert await reg.queue_shuffle(GUILD) is True
    await _fill(reg, 'a')
    before = await _version(client)
    assert await reg.queue_shuffle(GUILD) is True
    assert (await reg.queue_state(GUILD)).queue == ['a']
    assert await _version(client) == before


@pytest.mark.asyncio
async def test_shuffle_redraws_when_the_queue_changes_length():
    '''If the queue grew after the permutation was drawn, the script refuses and we draw again.'''
    reg, client = _make()
    await _fill(reg, 'a', 'b', 'c')
    real_llen = client.llen
    calls = {'count': 0}

    async def stale_then_real(key):
        calls['count'] += 1
        if calls['count'] == 1:
            return 2  # the length the first permutation is drawn for; the queue is really 3
        return await real_llen(key)

    client.llen = stale_then_real
    assert await reg.queue_shuffle(GUILD, rng=random.Random(2)) is True
    assert calls['count'] == 2
    assert sorted((await reg.queue_state(GUILD)).queue) == ['a', 'b', 'c']


@pytest.mark.asyncio
async def test_shuffle_gives_up_when_the_queue_never_settles():
    '''A queue that changes under every attempt is reported as not shuffled.'''
    reg, client = _make()
    await _fill(reg, 'a', 'b', 'c')
    client.llen = AsyncMock(return_value=2)
    assert await reg.queue_shuffle(GUILD) is False
    assert client.llen.await_count == guild_queue_registry.SHUFFLE_MAX_ATTEMPTS
    assert (await reg.queue_state(GUILD)).queue == ['a', 'b', 'c']


@pytest.mark.asyncio
async def test_clear_returns_items_in_play_order_and_empties_queue():
    '''Clear hands back what it removed so the caller can drop the entries.'''
    reg, client = _make()
    await _fill(reg, 'a', 'b', 'c')
    before = await _version(client)
    assert await reg.queue_clear(GUILD) == ['a', 'b', 'c']
    assert (await reg.queue_state(GUILD)).queue == []
    assert await _version(client) == before + 1


@pytest.mark.asyncio
async def test_clear_empty_queue_changes_nothing():
    '''Clearing nothing is not a change.'''
    reg, client = _make()
    assert await reg.queue_clear(GUILD) == []
    assert await _version(client) == 0


# ---------------------------------------------------------------------------
# claim / confirm / heartbeat
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_claim_returns_none_for_empty_queue():
    '''Nothing queued, nothing to claim.'''
    reg, _ = _make()
    assert await reg.queue_claim_next(GUILD) is None


@pytest.mark.asyncio
async def test_claim_pops_head_and_parks_it():
    '''The claimed uuid leaves the queue but is held until confirmed or dropped.'''
    reg, client = _make()
    await _fill(reg, 'a', 'b')
    assert await reg.queue_claim_next(GUILD) == 'a'
    assert (await reg.queue_state(GUILD)).queue == ['b']
    assert await client.get(f'{GCLAIM_KEY_PREFIX}{GUILD}') == 'a'


@pytest.mark.asyncio
async def test_claim_repeats_the_unconfirmed_claim():
    '''A caller that died before confirming gets the same track back, not the next one.'''
    reg, _ = _make()
    await _fill(reg, 'a', 'b')
    assert await reg.queue_claim_next(GUILD) == 'a'
    assert await reg.queue_claim_next(GUILD) == 'a'
    assert (await reg.queue_state(GUILD)).queue == ['b']


@pytest.mark.asyncio
async def test_claim_moves_on_after_confirm():
    '''Once confirmed, the next claim takes the next track.'''
    reg, _ = _make()
    await _fill(reg, 'a', 'b')
    await reg.queue_claim_next(GUILD)
    assert await reg.confirm_claim(GUILD, 'a', 'gw-1') is True
    assert await reg.queue_claim_next(GUILD) == 'b'


@pytest.mark.asyncio
async def test_drop_claim_lets_the_next_track_through():
    '''A track that cannot be played is dropped so it does not block the queue.'''
    reg, _ = _make()
    await _fill(reg, 'a', 'b')
    await reg.queue_claim_next(GUILD)
    await reg.drop_claim(GUILD)
    assert await reg.queue_claim_next(GUILD) == 'b'


@pytest.mark.asyncio
async def test_confirm_claim_records_now_playing():
    '''Confirming writes who is playing what, and since when, with a liveness TTL.'''
    reg, client = _make()
    await _fill(reg, 'a')
    await reg.queue_claim_next(GUILD)
    assert await reg.confirm_claim(GUILD, 'a', 'gw-1', started_at=1000.5) is True
    assert await client.hgetall(f'{GPLAYING_KEY_PREFIX}{GUILD}') == {
        'uuid': 'a', 'started_at': '1000.5', 'gateway_id': 'gw-1'}
    assert 0 < await client.ttl(f'{GPLAYING_KEY_PREFIX}{GUILD}') <= guild_queue_registry.PLAYING_TTL_SECONDS
    assert await client.get(f'{GCLAIM_KEY_PREFIX}{GUILD}') is None


@pytest.mark.asyncio
async def test_confirm_claim_defaults_started_at_to_now(monkeypatch):
    '''Without an explicit start time the registry stamps the current time.'''
    reg, client = _make()
    monkeypatch.setattr(guild_queue_registry.time, 'time', lambda: 2000.25)
    await _fill(reg, 'a')
    await reg.queue_claim_next(GUILD)
    await reg.confirm_claim(GUILD, 'a', 'gw-1')
    assert await client.hget(f'{GPLAYING_KEY_PREFIX}{GUILD}', 'started_at') == '2000.25'


@pytest.mark.asyncio
async def test_confirm_claim_refuses_a_uuid_that_is_not_claimed():
    '''Confirming something that was never claimed (or was dropped) does nothing.'''
    reg, client = _make()
    await _fill(reg, 'a')
    await reg.queue_claim_next(GUILD)
    assert await reg.confirm_claim(GUILD, 'other', 'gw-1') is False
    assert await client.exists(f'{GPLAYING_KEY_PREFIX}{GUILD}') == 0
    assert await client.get(f'{GCLAIM_KEY_PREFIX}{GUILD}') == 'a'


@pytest.mark.asyncio
async def test_heartbeat_extends_ttl_only_for_the_playing_track():
    '''The heartbeat keeps the playing record alive, and refuses a stale uuid.'''
    reg, client = _make()
    await _fill(reg, 'a')
    await reg.queue_claim_next(GUILD)
    await reg.confirm_claim(GUILD, 'a', 'gw-1')
    await client.expire(f'{GPLAYING_KEY_PREFIX}{GUILD}', 2)
    assert await reg.playing_heartbeat(GUILD, 'a') is True
    assert await client.ttl(f'{GPLAYING_KEY_PREFIX}{GUILD}') > 2
    assert await reg.playing_heartbeat(GUILD, 'stale') is False


@pytest.mark.asyncio
async def test_heartbeat_with_nothing_playing_is_false():
    '''No now-playing record, nothing to keep alive.'''
    reg, _ = _make()
    assert await reg.playing_heartbeat(GUILD, 'a') is False


# ---------------------------------------------------------------------------
# skip
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_skip_with_nothing_playing():
    '''No player, no skip.'''
    reg, client = _make()
    assert await reg.request_skip(GUILD, 'a') == SKIP_NO_PLAYER
    assert await client.exists(f'{GSKIP_KEY_PREFIX}{GUILD}') == 0


async def _play(reg: GuildQueueRegistry, uuid: str) -> None:
    await _fill(reg, uuid)
    assert await reg.queue_claim_next(GUILD) == uuid
    assert await reg.confirm_claim(GUILD, uuid, 'gw-1') is True


@pytest.mark.asyncio
async def test_skip_names_the_track_it_means():
    '''A skip aimed at a track that is no longer playing is refused, not applied to its successor.'''
    reg, client = _make()
    await _play(reg, 'a')
    assert await reg.request_skip(GUILD, 'earlier') == SKIP_NOT_CURRENT
    assert await client.exists(f'{GSKIP_KEY_PREFIX}{GUILD}') == 0


@pytest.mark.asyncio
async def test_skip_marks_the_playing_track_with_a_ttl():
    '''A good skip leaves a marker naming the track, which expires on its own.'''
    reg, client = _make()
    await _play(reg, 'a')
    assert await reg.request_skip(GUILD, 'a') == SKIP_OK
    assert await client.get(f'{GSKIP_KEY_PREFIX}{GUILD}') == 'a'
    assert 0 < await client.ttl(f'{GSKIP_KEY_PREFIX}{GUILD}') <= guild_queue_registry.SKIP_TTL_SECONDS


@pytest.mark.asyncio
async def test_skip_twice_is_idempotent():
    '''A second skip for the same track leaves the one marker in place.'''
    reg, _ = _make()
    await _play(reg, 'a')
    assert await reg.request_skip(GUILD, 'a') == SKIP_OK
    assert await reg.request_skip(GUILD, 'a') == SKIP_OK
    assert (await reg.queue_poll(GUILD))[1] == 'a'


# ---------------------------------------------------------------------------
# finish_track / history
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_finish_clears_playing_and_skip_and_records_history():
    '''A track that played out clears its markers and lands in history.'''
    reg, client = _make()
    await _play(reg, 'a')
    await reg.request_skip(GUILD, 'a')
    before = await _version(client)
    await reg.finish_track(GUILD, 'a', skipped=False, history_item={'title': 'T'}, history_cap=5)
    assert await client.exists(f'{GPLAYING_KEY_PREFIX}{GUILD}') == 0
    assert await client.exists(f'{GSKIP_KEY_PREFIX}{GUILD}') == 0
    assert await reg.history_items(GUILD) == [{'title': 'T'}]
    assert await _version(client) == before + 1


@pytest.mark.asyncio
async def test_finish_skipped_track_is_not_recorded():
    '''Skipped tracks stay out of history, matching the in-process player.'''
    reg, _ = _make()
    await _play(reg, 'a')
    await reg.finish_track(GUILD, 'a', skipped=True, history_item={'title': 'T'}, history_cap=5)
    assert await reg.history_items(GUILD) == []


@pytest.mark.asyncio
async def test_finish_without_history_item_or_cap_records_nothing():
    '''Nothing to record, or room for none, means no history write.'''
    reg, _ = _make()
    await _play(reg, 'a')
    await reg.finish_track(GUILD, 'a', skipped=False, history_item=None, history_cap=5)
    await reg.finish_track(GUILD, 'a', skipped=False, history_item={'title': 'T'}, history_cap=0)
    assert await reg.history_items(GUILD) == []


@pytest.mark.asyncio
async def test_finish_keeps_only_the_last_cap_items():
    '''History is bounded; the oldest entries fall off first.'''
    reg, _ = _make()
    for index in range(5):
        await reg.finish_track(GUILD, f'u{index}', skipped=False, history_item={'n': index}, history_cap=3)
    assert await reg.history_items(GUILD) == [{'n': 2}, {'n': 3}, {'n': 4}]


@pytest.mark.asyncio
async def test_finish_for_a_stale_uuid_leaves_the_current_track_alone():
    '''A late finish for track A must not erase the now-playing record of track B.'''
    reg, client = _make()
    await _play(reg, 'b')
    await reg.request_skip(GUILD, 'b')
    await reg.finish_track(GUILD, 'a', skipped=True, history_item=None, history_cap=5)
    assert await client.hget(f'{GPLAYING_KEY_PREFIX}{GUILD}', 'uuid') == 'b'
    assert await client.get(f'{GSKIP_KEY_PREFIX}{GUILD}') == 'b'


@pytest.mark.asyncio
async def test_history_is_per_guild():
    '''Guilds do not see each other's history.'''
    reg, _ = _make()
    await reg.finish_track(1, 'a', skipped=False, history_item={'g': 1}, history_cap=5)
    await reg.finish_track(2, 'b', skipped=False, history_item={'g': 2}, history_cap=5)
    assert await reg.history_items(1) == [{'g': 1}]
    assert await reg.history_items(2) == [{'g': 2}]


# ---------------------------------------------------------------------------
# state / poll
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_state_of_an_untouched_guild_is_empty():
    '''A guild nobody has used reads as the zero state, not an error.'''
    reg, _ = _make()
    assert await reg.queue_state(GUILD) == GuildQueueState()


@pytest.mark.asyncio
async def test_state_reports_everything_in_one_read():
    '''Queue, version, playing, pending skip and closed all come back together.'''
    reg, client = _make()
    await _fill(reg, 'a', 'b')
    await reg.queue_claim_next(GUILD)
    await reg.confirm_claim(GUILD, 'a', 'gw-1', started_at=5.0)
    await reg.request_skip(GUILD, 'a')
    await client.set(f'{GCLOSED_KEY_PREFIX}{GUILD}', '1')
    state = await reg.queue_state(GUILD)
    assert state.queue == ['b']
    assert state.version == 3
    assert state.playing == {'uuid': 'a', 'started_at': '5.0', 'gateway_id': 'gw-1'}
    assert state.skip_for == 'a'
    assert state.closed is True


@pytest.mark.asyncio
async def test_poll_returns_version_and_pending_skip():
    '''The cheap poll answers "has anything changed" and "was a skip asked for".'''
    reg, _ = _make()
    assert await reg.queue_poll(GUILD) == (0, None)
    await _play(reg, 'a')
    await reg.request_skip(GUILD, 'a')
    assert await reg.queue_poll(GUILD) == (2, 'a')


# ---------------------------------------------------------------------------
# close / open
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_close_returns_everything_held_and_wipes_it():
    '''Closing hands back the queue, the unconfirmed claim and the playing track.'''
    reg, client = _make()
    await _fill(reg, 'playing', 'claimed', 'q1', 'q2')
    await reg.queue_claim_next(GUILD)
    await reg.confirm_claim(GUILD, 'playing', 'gw-1')
    await reg.queue_claim_next(GUILD)
    await reg.request_skip(GUILD, 'playing')
    await reg.finish_track(GUILD, 'old', skipped=False, history_item={'x': 1}, history_cap=5)

    queued, claimed, playing = await reg.queue_close(GUILD)

    assert (queued, claimed, playing) == (['q1', 'q2'], 'claimed', 'playing')
    for prefix in (GQUEUE_KEY_PREFIX, GCLAIM_KEY_PREFIX, GPLAYING_KEY_PREFIX, GSKIP_KEY_PREFIX):
        assert await client.exists(f'{prefix}{GUILD}') == 0
    assert await reg.history_items(GUILD) == []
    assert (await reg.queue_state(GUILD)).closed is True


@pytest.mark.asyncio
async def test_close_of_an_idle_guild_returns_nothing():
    '''Closing a guild that holds nothing still closes it.'''
    reg, _ = _make()
    assert await reg.queue_close(GUILD) == ([], None, None)
    assert (await reg.queue_state(GUILD)).closed is True


@pytest.mark.asyncio
async def test_open_lets_a_closed_guild_enqueue_again():
    '''A new player reopens the guild its predecessor closed.'''
    reg, _ = _make()
    await reg.queue_close(GUILD)
    await reg.queue_open(GUILD, 555)
    assert await reg.queue_enqueue(GUILD, 'a') == ENQUEUE_OK


@pytest.mark.asyncio
async def test_closed_flag_expires():
    '''The closed flag carries a TTL, so a guild is not shut forever by a forgotten close.'''
    reg, client = _make()
    await reg.queue_close(GUILD)
    assert 0 < await client.ttl(f'{GCLOSED_KEY_PREFIX}{GUILD}') <= guild_queue_registry.GUILD_QUEUE_TTL_SECONDS


# ---------------------------------------------------------------------------
# text channel, durable current track, recovery on open
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_open_records_the_text_channel_and_reports_the_one_it_replaced():
    '''The first open has nothing to replace; a later one reports where the message was.'''
    reg, client = _make()
    assert await reg.queue_open(GUILD, 555) == (None, None)
    assert await client.get(f'{GCHANNEL_KEY_PREFIX}{GUILD}') == '555'
    assert 0 < await client.ttl(f'{GCHANNEL_KEY_PREFIX}{GUILD}') <= broker_registry_ttl()
    assert await reg.queue_open(GUILD, 777) == (555, None)
    assert (await reg.queue_state(GUILD)).text_channel_id == 777


def broker_registry_ttl() -> int:
    return guild_queue_registry.GUILD_QUEUE_TTL_SECONDS


@pytest.mark.asyncio
async def test_an_unopened_guild_has_no_text_channel():
    '''Nobody has said where its messages go.'''
    reg, _ = _make()
    assert (await reg.queue_state(GUILD)).text_channel_id is None


@pytest.mark.asyncio
async def test_confirming_a_claim_records_the_durable_current_track():
    '''gcurrent is not on the heartbeat TTL; it outlives a gateway that stops refreshing.'''
    reg, client = _make()
    await _play(reg, 'a')
    assert await client.get(f'{GCURRENT_KEY_PREFIX}{GUILD}') == 'a'
    assert await client.ttl(f'{GCURRENT_KEY_PREFIX}{GUILD}') > guild_queue_registry.PLAYING_TTL_SECONDS


@pytest.mark.asyncio
async def test_finishing_clears_the_current_track_only_for_that_track():
    '''A late finish for an earlier track must not erase the current one.'''
    reg, client = _make()
    await _play(reg, 'b')
    await reg.finish_track(GUILD, 'a', skipped=True, history_item=None, history_cap=5)
    assert await client.get(f'{GCURRENT_KEY_PREFIX}{GUILD}') == 'b'
    await reg.finish_track(GUILD, 'b', skipped=True, history_item=None, history_cap=5)
    assert await client.exists(f'{GCURRENT_KEY_PREFIX}{GUILD}') == 0


@pytest.mark.asyncio
async def test_open_recovers_a_track_nobody_is_heartbeating():
    '''The gateway died mid-track: its heartbeat lapsed, so the next owner gets the track back first.'''
    reg, client = _make()
    await _fill(reg, 'playing', 'next')
    assert await reg.queue_claim_next(GUILD) == 'playing'
    await reg.confirm_claim(GUILD, 'playing', 'gw-old')
    await client.delete(f'{GPLAYING_KEY_PREFIX}{GUILD}')  # the heartbeat TTL ran out
    before = await _version(client)

    assert await reg.queue_open(GUILD, 555) == (None, 'playing')

    assert (await reg.queue_state(GUILD)).queue == ['playing', 'next']
    assert await client.exists(f'{GCURRENT_KEY_PREFIX}{GUILD}') == 0
    assert await _version(client) == before + 1


@pytest.mark.asyncio
async def test_open_does_not_recover_while_the_old_gateway_is_still_heartbeating():
    '''A live playing record means this is the owner re-pointing the channel, or a restart that
    beat the heartbeat TTL; either way the track is not up for grabs.'''
    reg, _ = _make()
    await _fill(reg, 'playing', 'next')
    await reg.queue_claim_next(GUILD)
    await reg.confirm_claim(GUILD, 'playing', 'gw-old')
    assert await reg.queue_open(GUILD, 555) == (None, None)
    assert (await reg.queue_state(GUILD)).queue == ['next']


@pytest.mark.asyncio
async def test_open_with_no_current_track_recovers_nothing():
    '''Nothing was playing, so there is nothing to put back.'''
    reg, _ = _make()
    await _fill(reg, 'a')
    assert await reg.queue_open(GUILD, 555) == (None, None)
    assert (await reg.queue_state(GUILD)).queue == ['a']


@pytest.mark.asyncio
async def test_open_does_not_queue_a_recovered_track_twice():
    '''If the track is somehow already queued, recovery clears the marker without duplicating it.'''
    reg, client = _make()
    await _fill(reg, 'a')
    await client.set(f'{GCURRENT_KEY_PREFIX}{GUILD}', 'a')
    assert await reg.queue_open(GUILD, 555) == (None, 'a')
    assert (await reg.queue_state(GUILD)).queue == ['a']


@pytest.mark.asyncio
async def test_close_releases_a_track_whose_heartbeat_lapsed():
    '''The playing uuid falls back to the durable current track, so it is not left checked out.'''
    reg, client = _make()
    await _play(reg, 'dead')
    await client.delete(f'{GPLAYING_KEY_PREFIX}{GUILD}')
    queued, claimed, playing = await reg.queue_close(GUILD)
    assert (queued, claimed, playing) == ([], None, 'dead')
    assert await client.exists(f'{GCURRENT_KEY_PREFIX}{GUILD}') == 0


@pytest.mark.asyncio
async def test_close_keeps_the_text_channel():
    '''The message needs to be taken down in the same channel it was put up in.'''
    reg, _ = _make()
    await reg.queue_open(GUILD, 555)
    await reg.queue_close(GUILD)
    assert (await reg.queue_state(GUILD)).text_channel_id == 555


# ---------------------------------------------------------------------------
# play records for the history worker
# ---------------------------------------------------------------------------

async def _events(client) -> list[dict]:
    return [json.loads(raw) for raw in await client.lrange(HISTORY_EVENTS_KEY, 0, -1)]


@pytest.mark.asyncio
async def test_a_track_that_played_out_is_queued_for_the_history_worker():
    '''finish puts the play record where the worker will find it, only when asked to.'''
    reg, client = _make()
    await reg.finish_track(GUILD, 'a', skipped=False, history_item={'n': 1}, history_cap=5)
    assert await _events(client) == []
    await reg.finish_track(GUILD, 'b', skipped=False, history_item={'n': 2}, history_cap=5, emit_event=True)
    assert await _events(client) == [{'n': 2}]
    assert 0 < await client.ttl(HISTORY_EVENTS_KEY) <= guild_queue_registry.GUILD_QUEUE_TTL_SECONDS


@pytest.mark.asyncio
async def test_a_skipped_track_is_not_queued_for_the_history_worker():
    '''Skipped tracks are not recorded anywhere, as before.'''
    reg, client = _make()
    await reg.finish_track(GUILD, 'a', skipped=True, history_item={'n': 1}, history_cap=5, emit_event=True)
    assert await _events(client) == []


@pytest.mark.asyncio
async def test_finishing_without_a_record_queues_nothing():
    '''No item to describe the play, so nothing to record.'''
    reg, client = _make()
    await reg.finish_track(GUILD, 'a', skipped=False, history_item=None, history_cap=5, emit_event=True)
    assert await _events(client) == []


@pytest.mark.asyncio
async def test_plays_are_still_recorded_when_the_guilds_own_history_is_off():
    '''A cap of 0 switches off the guild's history list, not the analytics.'''
    reg, client = _make()
    await reg.finish_track(GUILD, 'a', skipped=False, history_item={'n': 1}, history_cap=0, emit_event=True)
    assert await reg.history_items(GUILD) == []
    assert await _events(client) == [{'n': 1}]


@pytest.mark.asyncio
async def test_records_come_out_oldest_first_and_a_retry_goes_to_the_front():
    '''FIFO for new records; a record put back is the next one taken.'''
    reg, _ = _make()
    for number in (1, 2, 3):
        await reg.finish_track(GUILD, f'u{number}', skipped=False, history_item={'n': number},
                               history_cap=5, emit_event=True)
    assert await reg.history_event_depth() == 3
    first = await reg.pop_history_event()
    assert first == {'n': 1}
    await reg.requeue_history_event({'n': 1, 'attempts': 1})
    assert await reg.pop_history_event() == {'n': 1, 'attempts': 1}
    assert await reg.pop_history_event() == {'n': 2}
    assert await reg.pop_history_event() == {'n': 3}
    assert await reg.pop_history_event() is None
    assert await reg.history_event_depth() == 0


@pytest.mark.asyncio
async def test_the_record_backlog_is_bounded_and_keeps_the_newest(monkeypatch):
    '''A db that stays down cannot grow Redis without limit; the oldest records go first.'''
    monkeypatch.setattr(guild_queue_registry, 'HISTORY_EVENTS_MAX', 3)
    reg, _ = _make()
    for number in range(6):
        await reg.finish_track(GUILD, f'u{number}', skipped=False, history_item={'n': number},
                               history_cap=5, emit_event=True)
    popped = [await reg.pop_history_event() for _ in range(3)]
    assert popped == [{'n': 3}, {'n': 4}, {'n': 5}]
    assert await reg.pop_history_event() is None
