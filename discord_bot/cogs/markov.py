from asyncio import sleep
from datetime import datetime, timedelta, timezone
from functools import partial
from re import match, sub, MULTILINE
from typing import Optional, List

from dappertable import DapperTable, Columns, Column, PaginationLength
from discord import ChannelType
from discord.ext.commands import Bot, Context, group
from discord.errors import DiscordServerError
from opentelemetry.trace import SpanKind
from opentelemetry.metrics import Observation
from pydantic import BaseModel, Field
from sqlalchemy import delete as sa_delete, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.sql.functions import random as sql_random

from discord_bot.common import DISCORD_MAX_MESSAGE_LENGTH
from discord_bot.cogs.cog_helper import CogHelper
from discord_bot.database import MarkovChannel, MarkovRelation
from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.types.dispatch_result import ChannelHistoryResult, GuildEmojisResult, is_not_found_error
from discord_bot.utils.common import return_loop_runner
from discord_bot.utils.loop_health import LOOP_HEALTH, health_aware_queue_get
from discord_bot.utils.sql_retry import async_retry_database_commands
from discord_bot.utils.otel import async_otel_span_wrapper, AttributeNaming, DiscordContextNaming, MetricNaming, METER_PROVIDER, create_observable_gauge, loop_heartbeat_observations, span_links_from_context
from discord_bot.utils.otel_command import command_wrapper
from discord_bot.clients.dispatch_client_base import DispatchClientBase

# Default for how many days to keep messages around
MARKOV_HISTORY_RETENTION_DAYS_DEFAULT = 365

# Default for how to wait between each loop
LOOP_SLEEP_INTERVAL_DEFAULT = 300

# Limit for how many messages we grab on each history check
MESSAGE_CHECK_LIMIT = 16

# Background-loop names: LoopHealth registry keys and heartbeat background_job values
LOOP_MARKOV_CHECK = 'markov_check'
LOOP_MARKOV_RESULT = 'markov_result'

# Pydantic config model
class MarkovConfig(BaseModel):
    '''Markov chain configuration'''
    loop_sleep_interval: float = 300.0
    message_check_limit: int = 16
    history_retention_days: int = 365
    server_reject_list: list[int] = Field(default_factory=list)

def clean_message(content: str, emojis: List[dict]):
    '''
    Clean channel message
    content :   Full message content to clean
    emojis  :   List of server emoji dicts ({'id', 'name', 'animated'}, as the
                dispatcher serialises them), so we can remove any not from server

    Returns "corpus", list of cleaned words
    '''
    # Remove web links and mentions from text
    message_text = sub(r'(https?\://|\<\@)\S+|\<\#\S+', '',
                       content, flags=MULTILINE)
    # Doesnt remove @here or @everyone
    message_text = message_text.replace('@here', '')
    message_text = message_text.replace('@everyone', '')
    # Strip blank ends
    message_text = message_text.strip()
    corpus = []
    emoji_ids = [emoji['id'] for emoji in emojis]
    for word in message_text.split(' '):
        if word in ('', ' '):
            continue
        # Check for commands again
        if word[0] == '!':
            continue
        # Check for emojis in message
        # If emoji, check if belongs to list, if not, disregard it
        # Emojis can be case sensitive so do not lower them
        # Custom emojis usually have <:emoji:id> format
        # Ex: <:fail:1231031923091032910390>
        match_result = match(r'^\ *<(?P<emoji>:\w+:)(?P<id>\d+)>\ *$', word)
        if match_result:
            if int(match_result.group('id')) in emoji_ids:
                corpus.append(word)
            continue
        corpus.append(word.lower())
    return corpus

async def get_matching_markov_channel(db_session: AsyncSession, ctx: Context):
    '''
    Get channel that matches original context
    '''
    return (await db_session.execute(
        select(MarkovChannel)
        .where(MarkovChannel.channel_id == ctx.channel.id)
        .where(MarkovChannel.server_id == ctx.guild.id)
    )).scalars().first()

async def list_guild_channels(db_session: AsyncSession, ctx: Context):
    '''
    List guild channels
    '''
    return (await db_session.execute(
        select(MarkovChannel.channel_id)
        .where(MarkovChannel.server_id == ctx.guild.id)
    )).all()

async def get_markov_channel_by_ids(db_session: AsyncSession, guild_id: int, channel_id: int):
    '''Get markov channel matching guild_id and channel_id.'''
    return (await db_session.execute(
        select(MarkovChannel)
        .where(MarkovChannel.channel_id == channel_id)
        .where(MarkovChannel.server_id == guild_id)
    )).scalars().first()

def _guild_relations(guild_id: int):
    '''Select over one guild's markov relations, joined through its channels.'''
    return (
        select(MarkovRelation)
        .join(MarkovChannel, MarkovChannel.id == MarkovRelation.channel_id)
        .where(MarkovChannel.server_id == guild_id)
    )

async def random_leader_word(db_session: AsyncSession, guild_id: int, first_word: str = None):
    '''
    Pick one random leader word for the guild, optionally constrained to first_word.

    Returns None when the guild has no relations at all (or none matching
    first_word), which is the caller's "nothing to say" signal.
    '''
    stmt = _guild_relations(guild_id).with_only_columns(MarkovRelation.leader_word)
    if first_word:
        stmt = stmt.where(MarkovRelation.leader_word == first_word)
    # bandit B311 does not apply: the randomness is postgres' random(), and word
    # selection is not security-sensitive either way.
    return (await db_session.execute(stmt.order_by(sql_random()).limit(1))).scalar_one_or_none()

async def random_follower_word(db_session: AsyncSession, guild_id: int, leader_word: str):
    '''
    Pick one random word that follows leader_word in this guild, or None.

    None means the chain dead-ends: retention can delete every relation in which
    a word leads while leaving one where it follows.
    '''
    stmt = (_guild_relations(guild_id)
            .with_only_columns(MarkovRelation.follower_word)
            .where(MarkovRelation.leader_word == leader_word))
    return (await db_session.execute(stmt.order_by(sql_random()).limit(1))).scalar_one_or_none()

class Markov(CogHelper):
    '''
    Save markov relations to a database periodically
    '''
    def __init__(self, bot: Bot, settings: dict, dispatcher: DispatchClientBase,
                 db_engine: AsyncEngine = None, redis_manager=None):
        if not db_engine:
            raise CogMissingRequiredArg('No db engine passed, cannot start markov')
        if not settings.get('general', {}).get('include', {}).get('markov', False):
            raise CogMissingRequiredArg('Markov cog not enabled')

        super().__init__(bot, settings, dispatcher, db_engine,
                         settings_prefix='markov', config_model=MarkovConfig,
                         redis_manager=redis_manager)

        # Access config values through self.config (Pydantic model)
        self.loop_sleep_interval = self.config.loop_sleep_interval
        self.message_check_limit = self.config.message_check_limit
        self.history_retention_days = self.config.history_retention_days
        self.server_reject_list = self.config.server_reject_list

        self._task = None
        self._result_task = None
        self._emoji_cache: dict[int, list] = {}
        self._init_task = None
        # Heartbeats read LoopHealth (successful iterations), the same bit the
        # health server's probe uses — see utils/loop_health.
        create_observable_gauge(METER_PROVIDER, MetricNaming.HEARTBEAT.value,
                                partial(loop_heartbeat_observations, LOOP_MARKOV_CHECK), 'Markov check loop heartbeat')
        create_observable_gauge(METER_PROVIDER, MetricNaming.HEARTBEAT.value,
                                partial(loop_heartbeat_observations, LOOP_MARKOV_RESULT), 'Markov result loop heartbeat')
        create_observable_gauge(METER_PROVIDER, MetricNaming.DISPATCH_RESULT_QUEUE_DEPTH.value, self.__result_queue_depth_callback, 'Markov dispatch result queue depth')

    def __result_queue_depth_callback(self, _options):
        '''
        Depth of the dispatch result queue — climbs if the consumer stalls or dies.
        '''
        depth = self._result_queue.qsize() if self._result_queue else 0
        return [
            Observation(depth, attributes={
                AttributeNaming.BACKGROUND_JOB.value: 'markov_result'
            })
        ]

    async def cog_load(self):
        '''Start background tasks.'''
        self._start_tasks()

    def _start_tasks(self):
        '''Start the producer and consumer tasks.'''
        self.register_result_queue()
        self._emoji_cache = {}
        # The producer sleeps loop_sleep_interval (default 300 s) per iteration,
        # so its staleness window is sized from that cadence rather than the
        # process default — otherwise it would read as stalled between runs.
        self._task = self.bot.loop.create_task(
            return_loop_runner(self._markov_request_loop, self.bot, self.logger,
                               continue_exceptions=(DiscordServerError, TimeoutError),
                               health=LOOP_HEALTH.register_for_interval(LOOP_MARKOV_CHECK, self.loop_sleep_interval))()
        )
        self._result_task = self.bot.loop.create_task(self._markov_result_loop())

    async def cog_unload(self):
        '''Cancel all running tasks.'''
        # Cancellation is a deliberate stop, not a wedge — see Music.cog_unload.
        LOOP_HEALTH.mark_stopped(LOOP_MARKOV_CHECK, LOOP_MARKOV_RESULT)
        if self._init_task:
            self._init_task.cancel()
        if self._task:
            self._task.cancel()
        if self._result_task:
            self._result_task.cancel()

    # https://srome.github.io/Making-A-Markov-Chain-Twitter-Bot-In-Python/
    async def build_and_save_relations(self, db_session: AsyncSession, corpus: List[str],
                                       markov_channel_id: str, message_timestamp: datetime):
        '''
        Stage relations for one message on the caller's session.

        db_session : Sqlalchemy async db session, owned and committed by the caller
        corpus : List of strings from message, after cleaning
        markov_channel_id : Markov Channel ID (ID from DB)
        message_timestamp: Timestamp for db

        Does NOT commit. This used to open its own session and commit once per
        word pair, which cost a fresh connection per pair -- the engine is built
        with NullPool, so nothing is reused and a twenty-word message opened
        twenty connections.

        Staging on the caller's session also makes a message atomic. The caller
        commits the relations and the channel's last_message_id together, so a
        failure part-way through re-fetches the whole message next cycle. Under
        the old split the relations committed first and last_message_id second,
        and a failure between them re-fetched a message whose relations were
        already saved -- silently doubling them.
        '''
        def ensure_word(word):
            if len(word) >= 255:
                self.logger.debug(f'Markov :: Cannot add word "{word}", is too long')
                return None
            return word

        for (k, word) in enumerate(corpus):
            if k != len(corpus) - 1: # Deal with last word
                next_word = corpus[k+1]
            else:
                next_word = corpus[0] # To loop back to the beginning
            leader_word = ensure_word(word)
            if leader_word is None:
                continue
            follower_word = ensure_word(next_word)
            if follower_word is None:
                continue
            db_session.add(MarkovRelation(channel_id=markov_channel_id,
                                          leader_word=leader_word,
                                          follower_word=follower_word,
                                          created_at=message_timestamp))

    async def delete_channel_relations(self, db_session: AsyncSession, channel_id: str):
        '''
        Delete all relations related to channel

        db_session : Sqlalchemy async db_session
        channel_id: Markov Channel ID (DB ID)
        '''
        async def delete_records():
            await db_session.execute(
                sa_delete(MarkovRelation).where(MarkovRelation.channel_id == channel_id)
            )
            await db_session.commit()

        await async_retry_database_commands(db_session, delete_records)

    async def _markov_request_loop(self):
        '''
        Producer loop: submit Discord fetch requests for each tracked channel.
        '''
        await sleep(self.loop_sleep_interval)
        retention_cutoff = datetime.now(timezone.utc) - timedelta(days=self.history_retention_days)
        self.logger.debug(f'Entering message gather loop, using cutoff {retention_cutoff}')

        async with self.with_db_session() as db_session:
            markov_channels = (await db_session.execute(select(MarkovChannel))).scalars().all()
            for markov_channel in markov_channels:
                guild_id = markov_channel.server_id
                async with async_otel_span_wrapper('markov.channel_check', kind=SpanKind.INTERNAL,
                                                   attributes={DiscordContextNaming.CHANNEL.value: markov_channel.channel_id,
                                                               DiscordContextNaming.GUILD.value: markov_channel.server_id}):
                    self.logger.debug(f'Checking channel id: {markov_channel.channel_id}, server id: {markov_channel.server_id}')
                    await self.dispatch_guild_emojis(guild_id, max_retries=5)
                    self.logger.info('Gathering markov messages for '
                                    f'channel {markov_channel.channel_id}')
                    if not markov_channel.last_message_id:
                        await self.dispatch_channel_history(
                            guild_id, markov_channel.channel_id,
                            limit=self.message_check_limit,
                            after=retention_cutoff,
                        )
                    else:
                        await self.dispatch_channel_history(
                            guild_id, markov_channel.channel_id,
                            limit=self.message_check_limit,
                            after_message_id=markov_channel.last_message_id,
                        )

        # Delete old records
        async with async_otel_span_wrapper('markov.message_delete', kind=SpanKind.INTERNAL):
            async with self.with_db_session() as db_session:
                await async_retry_database_commands(
                    db_session,
                    lambda: db_session.execute(
                        sa_delete(MarkovRelation).where(MarkovRelation.created_at < retention_cutoff)
                    )
                )
                await db_session.commit()
            self.logger.debug('Deleted expired/old markov relations')

    async def _markov_result_loop(self):
        '''
        Consumer loop: process results from the dispatcher result queue.
        '''
        health = LOOP_HEALTH.register(LOOP_MARKOV_RESULT)
        while True:
            result = await health_aware_queue_get(self._result_queue, health)
            try:
                if isinstance(result, GuildEmojisResult):
                    await self._process_emojis_result(result)
                elif isinstance(result, ChannelHistoryResult):
                    await self._process_history_result(result)
                health.record_success()
            except Exception:  # pylint: disable=broad-except
                health.record_error()
                # A single bad result must NOT kill the consumer: the producer keeps
                # filling the queue, so a dead consumer leaks memory unboundedly
                # (docs findings/2026-07-19 OOM root cause). Log and drain the next.
                self.logger.exception('Markov :: error processing dispatch result')

    async def _process_emojis_result(self, result: GuildEmojisResult):
        '''
        Process a guild emoji result, caching the emoji list on success.

        A result carrying an error is still a handled iteration — the fetch failed
        upstream, the consumer did its job. An early return before the caller's
        record_success() would let a run of upstream errors read as a wedged
        consumer, so this never raises on result.error.
        '''
        async with async_otel_span_wrapper('markov.emojis_result', kind=SpanKind.CONSUMER,
                                           attributes={DiscordContextNaming.GUILD.value: result.guild_id},
                                           links=span_links_from_context(result.span_context)):
            if result.error:
                self.logger.error(f'Markov :: Failed to fetch emojis for server {result.guild_id}: {result.error}')
                return
            self._emoji_cache[result.guild_id] = result.emojis

    async def _process_history_result(self, result: ChannelHistoryResult):
        '''
        Process a channel history result: filter messages and save to the Markov chain.

        Runs in the result-consumer task, long after every span that produced the
        request has closed, so it opens its own span linked back to the requesting
        one. Without that link these logs carry no trace at all.
        '''
        guild_id = result.guild_id
        channel_id = result.channel_id

        async with async_otel_span_wrapper('markov.history_result', kind=SpanKind.CONSUMER,
                                           attributes={DiscordContextNaming.CHANNEL.value: channel_id,
                                                       DiscordContextNaming.GUILD.value: guild_id},
                                           links=span_links_from_context(result.span_context)):
            return await self._apply_history_result(result, guild_id, channel_id)

    async def _apply_history_result(self, result: ChannelHistoryResult,
                                            guild_id: int, channel_id: int):
        '''Body of _process_history_result, run inside the consumer span.'''
        if result.error:
            # Matched on status rather than isinstance(result.error, NotFound):
            # results arriving through the dispatcher carry a DispatchRemoteError
            # rebuilt from JSON, so an isinstance check never matches in the split
            # deployment and the channel stays pinned to a dead message forever.
            if is_not_found_error(result.error) and result.after_message_id:
                self.logger.info(f'Unable to find message {result.after_message_id}'
                                 f' in channel {channel_id} in server {guild_id}, '
                                 'clearing relations and restarting from retention cutoff')
                async with self.with_db_session() as db_session:
                    markov_channel = await async_retry_database_commands(
                        db_session,
                        lambda: get_markov_channel_by_ids(db_session, guild_id, channel_id)
                    )
                    if markov_channel:
                        # Clearing last_message_id makes the next check loop
                        # re-request with after=retention_cutoff, so the channel
                        # rebuilds as far back as retention allows rather than
                        # re-fetching a message that no longer exists.
                        await self.delete_channel_relations(db_session, markov_channel.id)
                        markov_channel.last_message_id = None
                        await self.retry_commit(db_session)
            else:
                self.logger.error(
                    f'Markov :: Failed to fetch history for channel {channel_id} '
                    f'in server {guild_id}: {result.error}'
                )
            return

        if not result.messages:
            self.logger.debug(f'No new messages for channel {channel_id}')
            return

        emojis = self._emoji_cache.get(guild_id, [])
        async with self.with_db_session() as db_session:
            markov_channel = await async_retry_database_commands(
                db_session,
                lambda: get_markov_channel_by_ids(db_session, guild_id, channel_id)
            )
            if not markov_channel:
                self.logger.debug(f'Markov channel {channel_id} not found in DB, skipping')
                return

            for message in result.messages:
                self.logger.debug(f'Gathering message {message.id} '
                                  f'for channel {channel_id}')
                add_message = True
                if not message.content or message.author_bot:
                    add_message = False
                elif message.content[0] == '!':
                    add_message = False
                corpus = None
                if add_message:
                    corpus = clean_message(message.content, emojis)
                if corpus:
                    self.logger.info(f'Attempting to add corpus "{corpus}" '
                                     f'to channel {channel_id}')
                    await self.build_and_save_relations(db_session, corpus, markov_channel.id, message.created_at)
                markov_channel.last_message_id = message.id
                await self.retry_commit(db_session)
            self.logger.debug(f'Done with channel {channel_id}')

    @group(name='markov', invoke_without_command=False)
    async def markov(self, ctx: Context):
        '''
        Markov functions. Use '!help markov'
        '''
        if ctx.invoked_subcommand is None:
            await ctx.send('Invalid sub command passed...')

    @markov.command(name='on')
    @command_wrapper
    async def on(self, ctx: Context):
        '''
        Turn markov on for channel
        '''
        if ctx.guild.id in self.server_reject_list:
            return await self.dispatch_message(ctx.guild.id, ctx.channel.id,'Unable to turn on markov for server, in reject list')

        async with self.with_db_session() as db_session:
            # Ensure channel not already on
            markov = await async_retry_database_commands(db_session, lambda: get_matching_markov_channel(db_session, ctx))

            if markov:
                return await self.dispatch_message(ctx.guild.id, ctx.channel.id,'Channel already has markov turned on')
            channel = await self.bot.fetch_channel(ctx.channel.id)
            if channel.type not in [ChannelType.text, ChannelType.voice]:
                return await self.dispatch_message(ctx.guild.id, ctx.channel.id,'Not a valid markov channel, cannot turn on markov')

            new_markov = MarkovChannel(channel_id=ctx.channel.id,
                                       server_id=ctx.guild.id,
                                       last_message_id=None)
            db_session.add(new_markov)
            await async_retry_database_commands(db_session, db_session.commit)
            self.logger.info(f'Adding new markov channel {ctx.channel.id} from server {ctx.guild.id}')
            return await self.dispatch_message(ctx.guild.id, ctx.channel.id,'Markov turned on for channel')

    @markov.command(name='off')
    @command_wrapper
    async def off(self, ctx: Context):
        '''
        Turn markov off for channel
        '''
        async with self.with_db_session() as db_session:
            # Ensure channel not already on
            markov_channel = await async_retry_database_commands(db_session, lambda: get_matching_markov_channel(db_session, ctx))

            if not markov_channel:
                return await self.dispatch_message(ctx.guild.id, ctx.channel.id,'Channel does not have markov turned on')
            self.logger.info(f'Turning off markov channel {ctx.channel.id} from server {ctx.guild.id}')

            await self.delete_channel_relations(db_session, markov_channel.id)
            await db_session.delete(markov_channel)
            await async_retry_database_commands(db_session, db_session.commit)
            return await self.dispatch_message(ctx.guild.id, ctx.channel.id,'Markov turned off for channel')

    @markov.command(name='list-channels')
    @command_wrapper
    async def list_channels(self, ctx: Context):
        '''
        List channels markov is enabled for in this server
        '''
        async with self.with_db_session() as db_session:
            markov_channels = await async_retry_database_commands(db_session, lambda: list_guild_channels(db_session, ctx))

            if not markov_channels:
                return await self.dispatch_message(ctx.guild.id, ctx.channel.id,'Markov not enabled for any channels in server')

            headers = [
                Column('Channel', 64),
            ]

            table = DapperTable(columns=Columns(headers), pagination_options=PaginationLength(DISCORD_MAX_MESSAGE_LENGTH),
                                prefix='Channel List \n')
            for row in markov_channels:
                table.add_row([f'<#{row[0]}>'])
            for output in table.render():
                await self.dispatch_message(ctx.guild.id, ctx.channel.id,output)
            return True

    @markov.command(name='speak')
    @command_wrapper
    async def speak(self, ctx: Context, #pylint:disable=too-many-locals
                    first_word: Optional[str] = '',
                    sentence_length: Optional[int] = 32):
        '''
        Say a random sentence generated by markov

        Note that this uses all markov channels setup for the server

        first_word  :   First word for markov string, if not given will be random.
        sentence_length :   Length of sentence

        Note that for first_word, multiple words can be given, but they must be in quotes
        Ex: !markov speak "hey whats up", or !markov speak "hey whats up" 64
        '''
        if ctx.guild.id in self.server_reject_list:
            return await self.dispatch_message(ctx.guild.id, ctx.channel.id,'Unable to use markov for server, in reject list')

        self.logger.info(f'Calling speak on server {ctx.guild.id}')
        all_words = []
        first = None
        if first_word:
            # Allow for multiple words to be given
            # If so, just grab last word
            starting_words = first_word.split(' ')
            # Make sure to add to all words here
            for start_words in starting_words[:-1]:
                all_words.append(start_words.lower())
            first = starting_words[-1].lower()

        # One row per word, chosen by postgres. This used to select EVERY
        # relation id for the guild into python, choice() one, then fetch that
        # row by id -- and then repeat the whole select once per word of the
        # sentence. A 32-word sentence was ~64 queries, half of them returning
        # the guild's entire relation-id set, which grows without bound as
        # channels are gathered.
        async with self.with_db_session() as db_session:
            word = await async_retry_database_commands(
                db_session, lambda: random_leader_word(db_session, ctx.guild.id, first))

            if word is None:
                if first_word:
                    return await self.dispatch_message(ctx.guild.id, ctx.channel.id,f'No markov word matching "{first_word}"')
                return await self.dispatch_message(ctx.guild.id, ctx.channel.id,'No markov words to pick from')

            all_words.append(word)

            remaining_word_num = sentence_length - len(all_words)
            for _ in range(remaining_word_num):
                word = await async_retry_database_commands(
                    db_session, lambda w=word: random_follower_word(db_session, ctx.guild.id, w))
                if word is None:
                    # Dead end rather than a crash. The old code passed the empty
                    # id list straight to choice(), which raises IndexError -- and
                    # retention makes that reachable, since it can delete every
                    # relation in which a word leads while keeping one where it
                    # follows. A short sentence beats a traceback.
                    break
                all_words.append(word)
            return await self.dispatch_message(ctx.guild.id, ctx.channel.id,' '.join(markov_word for markov_word in all_words))
