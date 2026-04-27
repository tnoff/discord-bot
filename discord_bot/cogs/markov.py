from asyncio import sleep
from datetime import datetime, timedelta, timezone
from random import choice
from re import match, sub, MULTILINE
from typing import Optional, List

from dappertable import DapperTable, Columns, Column, PaginationLength
from discord import ChannelType
from discord.ext.commands import Bot, Context, group
from discord.errors import NotFound, DiscordServerError
from opentelemetry.trace import SpanKind
from opentelemetry.metrics import Observation
from pydantic import BaseModel, Field
from sqlalchemy import delete as sa_delete, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from discord_bot.common import DISCORD_MAX_MESSAGE_LENGTH
from discord_bot.cogs.cog_helper import CogHelper
from discord_bot.database import MarkovChannel, MarkovRelation
from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.types.dispatch_result import ChannelHistoryResult, GuildEmojisResult
from discord_bot.utils.common import return_loop_runner
from discord_bot.utils.sql_retry import async_retry_database_commands
from discord_bot.utils.otel import async_otel_span_wrapper, command_wrapper, AttributeNaming, DiscordContextNaming, MetricNaming, METER_PROVIDER, create_observable_gauge
from discord_bot.clients.dispatch_client_base import DispatchClientBase

# Default for how many days to keep messages around
MARKOV_HISTORY_RETENTION_DAYS_DEFAULT = 365

# Default for how to wait between each loop
LOOP_SLEEP_INTERVAL_DEFAULT = 300

# Limit for how many messages we grab on each history check
MESSAGE_CHECK_LIMIT = 16

# Pydantic config model
class MarkovConfig(BaseModel):
    '''Markov chain configuration'''
    loop_sleep_interval: float = 300.0
    message_check_limit: int = 16
    history_retention_days: int = 365
    server_reject_list: list[int] = Field(default_factory=list)

def clean_message(content: str, emojis: List[str]):
    '''
    Clean channel message
    content :   Full message content to clean
    emojis  :   List of server emoji ids, so we can remove any not from server

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
    emoji_ids = [emoji.id for emoji in emojis]
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

class Markov(CogHelper):
    '''
    Save markov relations to a database periodically
    '''
    REQUIRED_TABLES = ['markov_channel', 'markov_relation']

    def __init__(self, bot: Bot, settings: dict, dispatcher: DispatchClientBase, db_engine: AsyncEngine = None):
        if not db_engine:
            raise CogMissingRequiredArg('No db engine passed, cannot start markov')
        if not settings.get('general', {}).get('include', {}).get('markov', False):
            raise CogMissingRequiredArg('Markov cog not enabled')

        super().__init__(bot, settings, dispatcher, db_engine, settings_prefix='markov', config_model=MarkovConfig)

        # Access config values through self.config (Pydantic model)
        self.loop_sleep_interval = self.config.loop_sleep_interval
        self.message_check_limit = self.config.message_check_limit
        self.history_retention_days = self.config.history_retention_days
        self.server_reject_list = self.config.server_reject_list

        self._task = None
        self._result_task = None
        self._emoji_cache: dict[int, list] = {}
        self._init_task = None
        create_observable_gauge(METER_PROVIDER, MetricNaming.HEARTBEAT.value, self.__loop_active_callback, 'Markov check loop heartbeat')

    def __loop_active_callback(self, _options):
        '''
        Loop active callback check
        '''
        value = 1 if (self._task and not self._task.done()) else 0
        return [
            Observation(value, attributes={
                AttributeNaming.BACKGROUND_JOB.value: 'markov_check'
            })
        ]

    async def cog_load(self):
        '''Start background tasks.'''
        self._start_tasks()

    def _start_tasks(self):
        '''Start the producer and consumer tasks.'''
        self.register_result_queue()
        self._emoji_cache = {}
        self._task = self.bot.loop.create_task(
            return_loop_runner(self._markov_request_loop, self.bot, self.logger,
                               continue_exceptions=(DiscordServerError, TimeoutError))()
        )
        self._result_task = self.bot.loop.create_task(self._markov_result_loop())

    async def cog_unload(self):
        '''Cancel all running tasks.'''
        if self._init_task:
            self._init_task.cancel()
        if self._task:
            self._task.cancel()
        if self._result_task:
            self._result_task.cancel()

    # https://srome.github.io/Making-A-Markov-Chain-Twitter-Bot-In-Python/
    async def build_and_save_relations(self, corpus: List[str], markov_channel_id: str, message_timestamp: datetime):
        '''
        Build and save relations to db
        corpus : List of strings from message, after cleaning
        markov_channel_id : Markov Channel ID (ID from DB)
        message_timestamp: Timestamp for db
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
            new_relation = MarkovRelation(channel_id=markov_channel_id,
                                          leader_word=leader_word,
                                          follower_word=follower_word,
                                          created_at=message_timestamp)
            async with self.with_db_session() as db_session:
                db_session.add(new_relation)
                await async_retry_database_commands(db_session, db_session.commit)

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
        while True:
            result = await self._result_queue.get()
            if isinstance(result, GuildEmojisResult):
                if result.error:
                    self.logger.error(f'Markov :: Failed to fetch emojis for guild {result.guild_id}: {result.error}')
                    continue
                self._emoji_cache[result.guild_id] = result.emojis
            elif isinstance(result, ChannelHistoryResult):
                await self._process_history_result(result)

    async def _process_history_result(self, result: ChannelHistoryResult):
        '''
        Process a channel history result: filter messages and save to the Markov chain.
        '''
        guild_id = result.guild_id
        channel_id = result.channel_id

        if result.error:
            if isinstance(result.error, NotFound) and result.after_message_id:
                self.logger.info(f'Unable to find message {result.after_message_id}'
                                 f' in channel {channel_id}')
                async with self.with_db_session() as db_session:
                    markov_channel = await async_retry_database_commands(
                        db_session,
                        lambda: get_markov_channel_by_ids(db_session, guild_id, channel_id)
                    )
                    if markov_channel:
                        await self.delete_channel_relations(db_session, markov_channel.id)
                        markov_channel.last_message_id = None
                        await self.retry_commit(db_session)
            else:
                self.logger.error(
                    f'Markov :: Failed to fetch history for channel {channel_id}: {result.error}'
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
                    await self.build_and_save_relations(corpus, markov_channel.id, message.created_at)
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

        async with self.with_db_session() as db_session:
            async def get_possible_words(first=None):
                stmt = (
                    select(MarkovRelation.id)
                    .join(MarkovChannel, MarkovChannel.id == MarkovRelation.channel_id)
                    .where(MarkovChannel.server_id == ctx.guild.id)
                )
                if first:
                    stmt = stmt.where(MarkovRelation.leader_word == first)
                return (await db_session.execute(stmt)).scalars().all()

            possible_words = await async_retry_database_commands(db_session, lambda: get_possible_words(first))

            if len(possible_words) == 0:
                if first_word:
                    return await self.dispatch_message(ctx.guild.id, ctx.channel.id,f'No markov word matching "{first_word}"')
                return await self.dispatch_message(ctx.guild.id, ctx.channel.id,'No markov words to pick from')

            async def get_leader_word():
                return (await db_session.get(MarkovRelation, choice(possible_words))).leader_word

            async def get_follower_word(word_ids):
                return (await db_session.get(MarkovRelation, choice(word_ids))).follower_word

            word = await async_retry_database_commands(db_session, get_leader_word)
            all_words.append(word)

            remaining_word_num = sentence_length - len(all_words)
            for _ in range(remaining_word_num):
                relation_ids = await async_retry_database_commands(db_session, lambda w=word: get_possible_words(w))
                word = await async_retry_database_commands(db_session, lambda r=relation_ids: get_follower_word(r))
                all_words.append(word)
            return await self.dispatch_message(ctx.guild.id, ctx.channel.id,' '.join(markov_word for markov_word in all_words))
