import asyncio
from datetime import datetime, timezone
from functools import partial

import pytest
from sqlalchemy import select

from discord_bot.clients.guild_analytics_client import GuildAnalyticsClient
from discord_bot.database import Guild, GuildVideoAnalytics
from discord_bot.interfaces.database_protocols import GuildAnalyticsStore
from discord_bot.types.guild_analytics import GuildAnalyticsEntry

from tests.helpers import fake_engine #pylint:disable=unused-import
from tests.helpers import async_mock_session

GUILD_ID = 909
SECONDS_PER_DAY = 60 * 60 * 24


def build_store(fake_engine) -> GuildAnalyticsClient:  #pylint:disable=redefined-outer-name
    '''
    Build a GuildAnalyticsClient over the test engine.

    fake_engine : Async engine fixture, schema created and truncated
    '''
    return GuildAnalyticsClient(partial(async_mock_session, fake_engine))


@pytest.mark.asyncio
async def test_guild_analytics_client_satisfies_the_store_protocol(fake_engine):  #pylint:disable=redefined-outer-name
    '''GuildAnalyticsClient is a structural GuildAnalyticsStore'''
    assert isinstance(build_store(fake_engine), GuildAnalyticsStore)


@pytest.mark.asyncio
async def test_get_analytics_creates_rows_and_returns_zeroes(fake_engine):  #pylint:disable=redefined-outer-name
    '''A guild with no plays reads as zeroes, not as a missing row'''
    store = build_store(fake_engine)

    entry = await store.get_analytics(GUILD_ID)

    assert isinstance(entry, GuildAnalyticsEntry)
    assert entry.total_plays == 0
    assert entry.cached_plays == 0
    assert entry.total_duration_days == 0
    assert entry.total_duration_seconds == 0
    assert entry.created_at is not None

    async with async_mock_session(fake_engine) as db_session:
        guilds = (await db_session.execute(select(Guild))).scalars().all()
        rows = (await db_session.execute(select(GuildVideoAnalytics))).scalars().all()
    assert [guild.server_id for guild in guilds] == [GUILD_ID]
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_get_analytics_is_idempotent(fake_engine):  #pylint:disable=redefined-outer-name
    '''Calling twice does not mint a second guild or a second analytics row'''
    store = build_store(fake_engine)

    first = await store.get_analytics(GUILD_ID)
    second = await store.get_analytics(GUILD_ID)

    assert first.created_at == second.created_at
    async with async_mock_session(fake_engine) as db_session:
        assert len((await db_session.execute(select(Guild))).scalars().all()) == 1
        assert len((await db_session.execute(select(GuildVideoAnalytics))).scalars().all()) == 1


@pytest.mark.asyncio
async def test_record_play_on_a_guild_that_has_never_played(fake_engine):  #pylint:disable=redefined-outer-name
    '''The first play creates the rows itself rather than needing an ensure call first'''
    store = build_store(fake_engine)

    assert await store.record_play(GUILD_ID, 30, False) is True

    entry = await store.get_analytics(GUILD_ID)
    assert entry.total_plays == 1
    assert entry.cached_plays == 0
    assert entry.total_duration_seconds == 30


@pytest.mark.asyncio
async def test_record_play_counts_cache_hits_separately(fake_engine):  #pylint:disable=redefined-outer-name
    '''cached_plays counts only the plays served from cache, total_plays counts both'''
    store = build_store(fake_engine)

    await store.record_play(GUILD_ID, 10, True)
    await store.record_play(GUILD_ID, 10, False)
    await store.record_play(GUILD_ID, 10, True)

    entry = await store.get_analytics(GUILD_ID)
    assert entry.total_plays == 3
    assert entry.cached_plays == 2
    assert entry.total_duration_seconds == 30


@pytest.mark.asyncio
async def test_record_play_carries_whole_days_out_of_seconds(fake_engine):  #pylint:disable=redefined-outer-name
    '''Seconds roll into days rather than accumulating past a day's worth.

    The seconds column is a 32-bit integer and a busy guild passes a day of
    playback in a week, so the carry is what keeps it in range.
    '''
    store = build_store(fake_engine)

    await store.record_play(GUILD_ID, SECONDS_PER_DAY - 5, False)
    entry = await store.get_analytics(GUILD_ID)
    assert entry.total_duration_days == 0
    assert entry.total_duration_seconds == SECONDS_PER_DAY - 5

    await store.record_play(GUILD_ID, 10, False)
    entry = await store.get_analytics(GUILD_ID)
    assert entry.total_duration_days == 1
    assert entry.total_duration_seconds == 5


@pytest.mark.asyncio
async def test_record_play_carries_multiple_days_from_one_play(fake_engine):  #pylint:disable=redefined-outer-name
    '''A single long duration adds every whole day it contains, not one'''
    store = build_store(fake_engine)

    await store.record_play(GUILD_ID, (SECONDS_PER_DAY * 3) + 7, False)

    entry = await store.get_analytics(GUILD_ID)
    assert entry.total_duration_days == 3
    assert entry.total_duration_seconds == 7


@pytest.mark.asyncio
async def test_record_play_bumps_updated_at(fake_engine):  #pylint:disable=redefined-outer-name
    '''updated_at moves with the play; created_at does not'''
    store = build_store(fake_engine)
    created = await store.get_analytics(GUILD_ID)

    async with async_mock_session(fake_engine) as db_session:
        row = (await db_session.execute(select(GuildVideoAnalytics))).scalars().first()
        before = row.updated_at

    await store.record_play(GUILD_ID, 5, False)

    async with async_mock_session(fake_engine) as db_session:
        row = (await db_session.execute(select(GuildVideoAnalytics))).scalars().first()
        assert row.updated_at >= before
        assert row.created_at == created.created_at


@pytest.mark.asyncio
async def test_guilds_are_tracked_separately(fake_engine):  #pylint:disable=redefined-outer-name
    '''One guild's plays never land on another's totals'''
    store = build_store(fake_engine)

    await store.record_play(GUILD_ID, 60, True)
    await store.record_play(GUILD_ID + 1, 30, False)
    await store.record_play(GUILD_ID + 1, 30, False)

    first = await store.get_analytics(GUILD_ID)
    second = await store.get_analytics(GUILD_ID + 1)
    assert (first.total_plays, first.cached_plays, first.total_duration_seconds) == (1, 1, 60)
    assert (second.total_plays, second.cached_plays, second.total_duration_seconds) == (2, 0, 60)


@pytest.mark.asyncio
async def test_get_analytics_reads_survive_the_session_closing(fake_engine):  #pylint:disable=redefined-outer-name
    '''Every field is readable after the loading session is gone.

    This is the property the whole slice exists for. `!music-stats` formats
    `created_at` and does arithmetic on three counters, and the code it replaced
    did all of that inside the session block. A live row here raises
    DetachedInstanceError on the first attribute; an entry does not.
    '''
    store = build_store(fake_engine)
    await store.record_play(GUILD_ID, 90, True)

    entry = await store.get_analytics(GUILD_ID)

    assert entry.created_at.strftime('%Y-%m-%d') <= datetime.now(timezone.utc).strftime('%Y-%m-%d')
    assert entry.total_duration_seconds // 3600 == 0
    assert entry.model_dump_json()


@pytest.mark.asyncio
async def test_concurrent_plays_are_not_lost(fake_engine):  #pylint:disable=redefined-outer-name
    '''Two writers landing at once both count.

    A read-modify-write under postgres' default READ COMMITTED lets two
    transactions read the same totals and write back the same increment, and
    shortening the transaction narrows that window without closing it. The row
    lock closes it. Nothing writes concurrently today -- the bot is a singleton
    and one loop does all of it -- but a db pod exists so that more than one
    caller can, and this is the test that says what happens then.
    '''
    store = build_store(fake_engine)
    await store.get_analytics(GUILD_ID)

    await asyncio.gather(*[store.record_play(GUILD_ID, 60, False) for _ in range(8)])

    entry = await store.get_analytics(GUILD_ID)
    assert entry.total_plays == 8, 'an increment was lost; the row lock is not being taken'
    assert entry.total_duration_seconds == 480
