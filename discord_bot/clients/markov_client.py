'''
In-process MarkovStore: the markov tables, over the local engine.

Satisfies interfaces.database_protocols.MarkovStore, which the Markov cog
annotates against. Same shape as VideoCacheClient -- a session generator handed
in by the caller -- and the same reason for the plain name: an HTTP sibling does
not exist yet, and renaming this to InMemoryMarkovStore before there is
something to distinguish it from would be churn.

**This module owns every transaction boundary the cog used to hold open.** That
is the substance of the slice, not the type annotations. The cog previously
opened one session and then made Discord API calls inside it, mutated ORM rows
across awaits, and committed at points chosen by the loop it happened to be in.
None of that has a remote equivalent, and two of them were costs in-process too:
the producer loop held a connection for the whole dispatch fan-out, and
`!markov speak` would have taken a connection per word had the query been
per-word rather than per-sentence.

Each public method here opens exactly one session and returns values, so the
eventual HTTP implementation is one request per call with nothing session-bound
in the answer.
'''
from datetime import datetime
from typing import Callable, List

from opentelemetry.trace import SpanKind
from sqlalchemy import delete as sa_delete, select, update as sa_update
from sqlalchemy.sql.functions import random as sql_random

from discord_bot.database import MarkovChannel, MarkovRelation
from discord_bot.types.markov import MarkovChannelEntry, MarkovMessageWrite
from discord_bot.utils.otel import async_otel_span_wrapper, DiscordContextNaming
from discord_bot.utils.sql_retry import async_retry_database_commands

OTEL_SPAN_PREFIX = 'markov.store'


def _guild_relations(guild_id: int):
    '''
    Select over one guild's markov relations, joined through its channels.

    guild_id : Discord guild id
    '''
    return (
        select(MarkovRelation)
        .join(MarkovChannel, MarkovChannel.id == MarkovRelation.channel_id)
        .where(MarkovChannel.server_id == guild_id)
    )


def _channel_by_ids(guild_id: int, channel_id: int):
    '''
    Select the markov_channel row for a guild/channel pair.

    guild_id : Discord guild id
    channel_id : Discord channel id
    '''
    return (
        select(MarkovChannel)
        .where(MarkovChannel.channel_id == channel_id)
        .where(MarkovChannel.server_id == guild_id)
    )


class MarkovClient():
    '''
    The markov chain tables -- the in-process MarkovStore.

    Tracks which channels are gathered and the leader/follower word graph built
    from their messages.
    '''

    def __init__(self, session_generator: Callable):
        self.session_generator: Callable = session_generator

    async def list_channels(self) -> List[MarkovChannelEntry]:
        '''
        Return every tracked channel, across all guilds.
        '''
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.list_channels', kind=SpanKind.INTERNAL):
            async with self.session_generator() as db_session:
                rows = await async_retry_database_commands(
                    db_session,
                    lambda: db_session.execute(select(MarkovChannel)))
                return [MarkovChannelEntry.from_row(row) for row in rows.scalars().all()]

    async def list_guild_channel_ids(self, guild_id: int) -> List[int]:
        '''
        Return the Discord channel ids tracked in one guild.

        guild_id : Discord guild id
        '''
        attributes = {DiscordContextNaming.GUILD.value: guild_id}
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.list_guild_channel_ids',
                                           kind=SpanKind.INTERNAL, attributes=attributes):
            async with self.session_generator() as db_session:
                rows = await async_retry_database_commands(
                    db_session,
                    lambda: db_session.execute(
                        select(MarkovChannel.channel_id).where(MarkovChannel.server_id == guild_id)))
                return list(rows.scalars().all())

    async def get_channel(self, guild_id: int, channel_id: int) -> MarkovChannelEntry | None:
        '''
        Return the tracked channel, or None when markov is off for it.

        guild_id : Discord guild id
        channel_id : Discord channel id
        '''
        attributes = {
            DiscordContextNaming.GUILD.value: guild_id,
            DiscordContextNaming.CHANNEL.value: channel_id,
        }
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.get_channel',
                                           kind=SpanKind.INTERNAL, attributes=attributes):
            async with self.session_generator() as db_session:
                row = await self.__fetch_channel(db_session, guild_id, channel_id)
                return MarkovChannelEntry.from_row(row) if row else None

    async def add_channel(self, guild_id: int, channel_id: int) -> MarkovChannelEntry:
        '''
        Start tracking a channel and return its new row.

        guild_id : Discord guild id
        channel_id : Discord channel id
        '''
        attributes = {
            DiscordContextNaming.GUILD.value: guild_id,
            DiscordContextNaming.CHANNEL.value: channel_id,
        }
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.add_channel',
                                           kind=SpanKind.INTERNAL, attributes=attributes):
            async with self.session_generator() as db_session:
                new_channel = MarkovChannel(channel_id=channel_id,
                                            server_id=guild_id,
                                            last_message_id=None)
                db_session.add(new_channel)
                await async_retry_database_commands(db_session, db_session.commit)
                return MarkovChannelEntry.from_row(new_channel)

    async def remove_channel(self, guild_id: int, channel_id: int) -> bool:
        '''
        Stop tracking a channel, dropping its relations with it.

        guild_id : Discord guild id
        channel_id : Discord channel id
        '''
        attributes = {
            DiscordContextNaming.GUILD.value: guild_id,
            DiscordContextNaming.CHANNEL.value: channel_id,
        }
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.remove_channel',
                                           kind=SpanKind.INTERNAL, attributes=attributes):
            async with self.session_generator() as db_session:
                row = await self.__fetch_channel(db_session, guild_id, channel_id)
                if not row:
                    return False

                async def delete_records():
                    await db_session.execute(
                        sa_delete(MarkovRelation).where(MarkovRelation.channel_id == row.id))
                    await db_session.delete(row)
                    await db_session.commit()

                await async_retry_database_commands(db_session, delete_records)
                return True

    async def reset_channel(self, guild_id: int, channel_id: int) -> bool:
        '''
        Clear a channel's relations and its last_message_id together.

        guild_id : Discord guild id
        channel_id : Discord channel id
        '''
        attributes = {
            DiscordContextNaming.GUILD.value: guild_id,
            DiscordContextNaming.CHANNEL.value: channel_id,
        }
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.reset_channel',
                                           kind=SpanKind.INTERNAL, attributes=attributes):
            async with self.session_generator() as db_session:
                row = await self.__fetch_channel(db_session, guild_id, channel_id)
                if not row:
                    return False

                async def reset_records():
                    await db_session.execute(
                        sa_delete(MarkovRelation).where(MarkovRelation.channel_id == row.id))
                    await db_session.execute(
                        sa_update(MarkovChannel)
                        .where(MarkovChannel.id == row.id)
                        .values(last_message_id=None))
                    await db_session.commit()

                await async_retry_database_commands(db_session, reset_records)
                return True

    async def save_messages(self, guild_id: int, channel_id: int,
                            messages: List[MarkovMessageWrite]) -> int | None:
        '''
        Persist a batch of gathered messages, committing one message at a time.

        One session for the batch, one commit per message. The commit boundary
        is what makes a message atomic -- its relations and the channel's new
        last_message_id land together, so a failure part-way re-fetches that
        whole message next cycle rather than double-counting words already
        saved. Batching is what keeps that boundary from costing a connection
        per message under NullPool.

        guild_id : Discord guild id
        channel_id : Discord channel id
        messages : Word pairs and message id, oldest first
        '''
        attributes = {
            DiscordContextNaming.GUILD.value: guild_id,
            DiscordContextNaming.CHANNEL.value: channel_id,
        }
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.save_messages',
                                           kind=SpanKind.INTERNAL, attributes=attributes):
            async with self.session_generator() as db_session:
                row = await self.__fetch_channel(db_session, guild_id, channel_id)
                if not row:
                    return None
                channel_row_id = row.id
                written = 0
                for message in messages:
                    async def write_message(message=message):
                        for (leader_word, follower_word) in message.word_pairs:
                            db_session.add(MarkovRelation(channel_id=channel_row_id,
                                                          leader_word=leader_word,
                                                          follower_word=follower_word,
                                                          created_at=message.message_timestamp))
                        await db_session.execute(
                            sa_update(MarkovChannel)
                            .where(MarkovChannel.id == channel_row_id)
                            .values(last_message_id=message.last_message_id))
                        await db_session.commit()

                    await async_retry_database_commands(db_session, write_message)
                    written += 1
                return written

    async def generate_words(self, guild_id: int, count: int,
                             first_word: str | None = None) -> List[str]:
        '''
        Walk the chain and return up to `count` words.

        guild_id : Discord guild id
        count : Maximum words to return
        first_word : Constrain the opening word, if given
        '''
        attributes = {DiscordContextNaming.GUILD.value: guild_id}
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.generate_words',
                                           kind=SpanKind.INTERNAL, attributes=attributes):
            if count < 1:
                return []
            async with self.session_generator() as db_session:
                word = await async_retry_database_commands(
                    db_session,
                    lambda: self.__random_leader_word(db_session, guild_id, first_word))
                if word is None:
                    return []
                words = [word]
                while len(words) < count:
                    word = await async_retry_database_commands(
                        db_session,
                        lambda w=word: self.__random_follower_word(db_session, guild_id, w))
                    if word is None:
                        # Dead end, not an error: retention can delete every
                        # relation in which a word leads while keeping one where
                        # it follows. A short sentence beats a traceback.
                        break
                    words.append(word)
                return words

    async def prune_relations_before(self, cutoff: datetime) -> bool:
        '''
        Delete relations older than the retention cutoff.

        cutoff : Relations created before this are dropped
        '''
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.prune_relations_before', kind=SpanKind.INTERNAL):
            async with self.session_generator() as db_session:
                await async_retry_database_commands(
                    db_session,
                    lambda: db_session.execute(
                        sa_delete(MarkovRelation).where(MarkovRelation.created_at < cutoff)))
                await db_session.commit()
                return True

    async def __fetch_channel(self, db_session, guild_id: int, channel_id: int):
        '''
        Return the live MarkovChannel row for a guild/channel pair, or None.

        Private on purpose: a live row never leaves this class.

        db_session : Sqlalchemy async db session
        guild_id : Discord guild id
        channel_id : Discord channel id
        '''
        result = await async_retry_database_commands(
            db_session,
            lambda: db_session.execute(_channel_by_ids(guild_id, channel_id)))
        return result.scalars().first()

    async def __random_leader_word(self, db_session, guild_id: int, first_word: str | None):
        '''
        Pick one random leader word for the guild, optionally constrained.

        db_session : Sqlalchemy async db session
        guild_id : Discord guild id
        first_word : Constrain the opening word, if given
        '''
        stmt = _guild_relations(guild_id).with_only_columns(MarkovRelation.leader_word)
        if first_word:
            stmt = stmt.where(MarkovRelation.leader_word == first_word)
        # bandit B311 does not apply: the randomness is postgres' random(), and
        # word selection is not security-sensitive either way.
        return (await db_session.execute(stmt.order_by(sql_random()).limit(1))).scalar_one_or_none()

    async def __random_follower_word(self, db_session, guild_id: int, leader_word: str):
        '''
        Pick one random word that follows leader_word in this guild, or None.

        db_session : Sqlalchemy async db session
        guild_id : Discord guild id
        leader_word : Word to follow
        '''
        stmt = (_guild_relations(guild_id)
                .with_only_columns(MarkovRelation.follower_word)
                .where(MarkovRelation.leader_word == leader_word))
        return (await db_session.execute(stmt.order_by(sql_random()).limit(1))).scalar_one_or_none()
