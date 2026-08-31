'''
In-process GuildAnalyticsStore: the guild analytics tables, over the local engine.

Satisfies interfaces.database_protocols.GuildAnalyticsStore, which the Music cog
annotates against. Same shape as the three clients before it -- a session
generator handed in by the caller.

The whole group is two rows deep, but it is where the read-modify-write lived.
`update_video_guild_analytics` loaded the analytics row, incremented four fields
in python and committed, and the post-play loop called it with a session it was
holding open across a Discord dispatch. The store opens the session, does the
work and closes it, so the connection is held for the query rather than for
however long the caller had other things to do.

`record_play` also takes a row lock, which the old code did not. Today that
changes nothing -- the bot is a singleton and one loop does all the writing, so
there is no second writer to lose an update to. It matters at the other end of
this project: the point of a db pod is that more than one caller can talk to it,
and read-modify-write under postgres' default READ COMMITTED lets two of them
read the same totals and write back the same increment. A shorter transaction
narrows that window; it does not close it. `FOR UPDATE` closes it.

`get_analytics` and `record_play` both create the rows when they are missing,
which is what `ensure_guild` / `ensure_guild_video_analytics` did -- except that
those ran as separate calls in the caller's session, so a first play in a new
guild was two ensures plus an update rather than one call.
'''
from datetime import datetime, timezone

from opentelemetry.trace import SpanKind
from sqlalchemy import select

from discord_bot.clients.session_store import SessionStoreBase
from discord_bot.database import Guild, GuildVideoAnalytics
from discord_bot.types.guild_analytics import GuildAnalyticsEntry
from discord_bot.utils.otel import async_otel_span_wrapper, DiscordContextNaming
from discord_bot.utils.sql_retry import async_retry_database_commands

OTEL_SPAN_PREFIX = 'music.guild_analytics_store'

# 1 day, in seconds. Durations are carried into `total_duration_days` rather
# than left to overflow a 32-bit seconds column.
_SECONDS_PER_DAY = 60 * 60 * 24


class GuildAnalyticsClient(SessionStoreBase):
    '''
    GuildAnalyticsStore backed by a session generator over the local engine.

    session_generator : Callable returning an async context manager yielding an
                        AsyncSession
    '''

    async def __ensure_rows(self, db_session, guild_id: int,
                            lock: bool = False) -> GuildVideoAnalytics:
        '''
        Return the analytics row for a guild, creating it and the guild row.

        Runs inside the caller's transaction so that the create and whatever
        follows it commit together.

        db_session : Open session to run against
        guild_id : Discord guild id
        lock : Take a row lock, for callers that read-modify-write
        '''
        guild = (await db_session.execute(
            select(Guild).where(Guild.server_id == guild_id)
        )).scalars().first()
        if not guild:
            guild = Guild(server_id=guild_id)
            db_session.add(guild)
            await db_session.flush()
        statement = select(GuildVideoAnalytics).where(GuildVideoAnalytics.guild_id == guild.id)
        if lock:
            statement = statement.with_for_update()
        existing = (await db_session.execute(statement)).scalars().first()
        if existing:
            return existing
        now_timestamp = datetime.now(timezone.utc)
        new_row = GuildVideoAnalytics(
            guild_id=guild.id,
            total_plays=0,
            cached_plays=0,
            total_duration_days=0,
            total_duration_seconds=0,
            created_at=now_timestamp,
            updated_at=now_timestamp,
        )
        db_session.add(new_row)
        await db_session.flush()
        return new_row

    async def get_analytics(self, guild_id: int) -> GuildAnalyticsEntry:
        '''
        Return a guild's play totals, creating the rows on first call.

        guild_id : Discord guild id
        '''
        attributes = {DiscordContextNaming.GUILD.value: guild_id}
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.get_analytics',
                                           kind=SpanKind.INTERNAL, attributes=attributes):
            async with self.session_generator() as db_session:
                async def read_totals():
                    row = await self.__ensure_rows(db_session, guild_id)
                    entry = GuildAnalyticsEntry.from_row(row)
                    await db_session.commit()
                    return entry

                return await async_retry_database_commands(db_session, read_totals)

    async def record_play(self, guild_id: int, duration_seconds: int,
                          cache_hit: bool) -> bool:
        '''
        Add one play to a guild's totals.

        guild_id : Discord guild id
        duration_seconds : Length of the track that just played
        cache_hit : True when the download was served from cache
        '''
        attributes = {DiscordContextNaming.GUILD.value: guild_id}
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.record_play',
                                           kind=SpanKind.INTERNAL, attributes=attributes):
            async with self.session_generator() as db_session:
                async def apply_play():
                    row = await self.__ensure_rows(db_session, guild_id, lock=True)
                    row.total_plays += 1
                    total_seconds = row.total_duration_seconds + duration_seconds
                    row.total_duration_days += total_seconds // _SECONDS_PER_DAY
                    row.total_duration_seconds = total_seconds % _SECONDS_PER_DAY
                    if cache_hit:
                        row.cached_plays += 1
                    row.updated_at = datetime.now(timezone.utc)
                    await db_session.commit()
                    return True

                return await async_retry_database_commands(db_session, apply_play)
