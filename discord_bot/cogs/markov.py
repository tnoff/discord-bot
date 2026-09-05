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

from discord_bot.common import DISCORD_MAX_MESSAGE_LENGTH
from discord_bot.cogs.common import CogHelperBase
from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.interfaces.database_protocols import MarkovStore
from discord_bot.types.dispatch_result import ChannelHistoryResult, GuildEmojisResult, is_not_found_error
from discord_bot.types.markov import MarkovMessageWrite
from discord_bot.utils.common import return_loop_runner
from discord_bot.utils.loop_health import LOOP_HEALTH, health_aware_queue_get
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

class Markov(CogHelperBase):
    '''
    Save markov relations to a database periodically
    '''
    def __init__(self, bot: Bot, settings: dict, dispatcher: DispatchClientBase,
                 stores: object = None, redis_manager=None):
        if not stores or not stores.markov:
            raise CogMissingRequiredArg('No markov store passed, cannot start markov')
        if not settings.get('general', {}).get('include', {}).get('markov', False):
            raise CogMissingRequiredArg('Markov cog not enabled')

        super().__init__(bot, settings, dispatcher, stores,
                         settings_prefix='markov', config_model=MarkovConfig,
                         redis_manager=redis_manager)

        # Access config values through self.config (Pydantic model)
        self.loop_sleep_interval = self.config.loop_sleep_interval
        self.message_check_limit = self.config.message_check_limit
        self.history_retention_days = self.config.history_retention_days
        self.server_reject_list = self.config.server_reject_list

        # The cog talks to persistence only through this. It was annotated against
        # the Protocol rather than MarkovClient so the HTTP store could drop in
        # without touching a line below -- which is exactly what happened in MR 4b,
        # and not one line below this changed.
        # Injected rather than constructed. The cog has never cared which
        # implementation this is -- it was annotated against the Protocol from the
        # day MarkovStore existed, precisely so this line could change without any
        # line below it changing.
        self.markov_store: MarkovStore = stores.markov

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
    def build_word_pairs(self, corpus: List[str]) -> List[tuple]:
        '''
        Turn one message's corpus into leader/follower pairs.

        corpus : List of strings from message, after cleaning

        Pure: it builds the pairs and hands them back for the store to persist.
        It used to open a session and commit once per pair, which cost a fresh
        connection per pair under NullPool; `!267` moved the write onto the
        caller's session, and the store owns it now. Pair-building is the markov
        algorithm and stays here -- what left is every decision about sessions
        and commits.

        Words at or over the column width are dropped rather than truncated, the
        same as before, because a truncated word is a real word that was never
        said.
        '''
        def ensure_word(word):
            if len(word) >= 255:
                self.logger.debug(f'Markov :: Cannot add word "{word}", is too long')
                return None
            return word

        pairs = []
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
            pairs.append((leader_word, follower_word))
        return pairs

    async def _markov_request_loop(self):
        '''
        Producer loop: submit Discord fetch requests for each tracked channel.
        '''
        await sleep(self.loop_sleep_interval)
        retention_cutoff = datetime.now(timezone.utc) - timedelta(days=self.history_retention_days)
        self.logger.debug(f'Entering message gather loop, using cutoff {retention_cutoff}')

        # Entries, and the session is closed before the first dispatch. This used
        # to iterate live rows inside `async with self.with_db_session()`, which
        # held one postgres connection open across every emoji fetch and history
        # request in the sweep -- under NullPool, for the whole fan-out.
        markov_channels = await self.markov_store.list_channels()
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
            await self.markov_store.prune_relations_before(retention_cutoff)
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
                # Clearing last_message_id makes the next check loop re-request
                # with after=retention_cutoff, so the channel rebuilds as far
                # back as retention allows rather than re-fetching a message
                # that no longer exists. Both halves are one transaction in the
                # store: a clear that dropped the id but kept the relations
                # would double every word it re-gathered.
                await self.markov_store.reset_channel(guild_id, channel_id)
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
        writes = []
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
            word_pairs = []
            if corpus:
                self.logger.info(f'Attempting to add corpus "{corpus}" '
                                 f'to channel {channel_id}')
                word_pairs = self.build_word_pairs(corpus)
            # A message that contributed nothing still advances last_message_id.
            # Skipping it here would re-fetch it every cycle forever.
            writes.append(MarkovMessageWrite(word_pairs=word_pairs,
                                             message_timestamp=message.created_at,
                                             last_message_id=message.id))

        # One call for the batch. The store still commits per message, so the
        # atomicity `!267` established survives -- what does not survive the
        # seam is a session held open across the loop.
        written = await self.markov_store.save_messages(guild_id, channel_id, writes)
        if written is None:
            self.logger.debug(f'Markov channel {channel_id} not found in DB, skipping')
            return
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

        # Ensure channel not already on
        markov = await self.markov_store.get_channel(ctx.guild.id, ctx.channel.id)

        if markov:
            return await self.dispatch_message(ctx.guild.id, ctx.channel.id,'Channel already has markov turned on')
        # Deliberately outside any store call: this awaits Discord, and the old
        # shape held an open postgres session across it.
        channel = await self.bot.fetch_channel(ctx.channel.id)
        if channel.type not in [ChannelType.text, ChannelType.voice]:
            return await self.dispatch_message(ctx.guild.id, ctx.channel.id,'Not a valid markov channel, cannot turn on markov')

        await self.markov_store.add_channel(ctx.guild.id, ctx.channel.id)
        self.logger.info(f'Adding new markov channel {ctx.channel.id} from server {ctx.guild.id}')
        return await self.dispatch_message(ctx.guild.id, ctx.channel.id,'Markov turned on for channel')

    @markov.command(name='off')
    @command_wrapper
    async def off(self, ctx: Context):
        '''
        Turn markov off for channel
        '''
        # False is "was not tracked", not a failure -- the store drops the
        # relations and the channel row in one transaction.
        removed = await self.markov_store.remove_channel(ctx.guild.id, ctx.channel.id)

        if not removed:
            return await self.dispatch_message(ctx.guild.id, ctx.channel.id,'Channel does not have markov turned on')
        self.logger.info(f'Turning off markov channel {ctx.channel.id} from server {ctx.guild.id}')
        return await self.dispatch_message(ctx.guild.id, ctx.channel.id,'Markov turned off for channel')

    @markov.command(name='list-channels')
    @command_wrapper
    async def list_channels(self, ctx: Context):
        '''
        List channels markov is enabled for in this server
        '''
        # Ids, not one-column Row tuples -- this used to index `row[0]`, which
        # is a driver artifact and not something an HTTP store would ever send.
        markov_channels = await self.markov_store.list_guild_channel_ids(ctx.guild.id)

        if not markov_channels:
            return await self.dispatch_message(ctx.guild.id, ctx.channel.id,'Markov not enabled for any channels in server')

        headers = [
            Column('Channel', 64),
        ]

        table = DapperTable(columns=Columns(headers), pagination_options=PaginationLength(DISCORD_MAX_MESSAGE_LENGTH),
                            prefix='Channel List \n')
        for channel_id in markov_channels:
            table.add_row([f'<#{channel_id}>'])
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

        # The whole walk in one call. Each word is chosen from the previous one
        # by postgres; `!268` made that a single query per word instead of
        # selecting the guild's entire relation-id set and choosing in python.
        # Asking the store per word would have put a round trip where that query
        # is, which is the same mistake one layer up.
        # At least one: a leader word was always fetched before, even when the
        # given prefix already met sentence_length, so `!markov speak "a b c" 2`
        # still answers with a word rather than "nothing to say".
        generated = await self.markov_store.generate_words(
            ctx.guild.id, max(1, sentence_length - len(all_words)), first_word=first)

        # Empty is the "nothing to say" answer, and a short list is a dead end:
        # retention can delete every relation in which a word leads while
        # keeping one where it follows.
        if not generated:
            if first_word:
                return await self.dispatch_message(ctx.guild.id, ctx.channel.id,f'No markov word matching "{first_word}"')
            return await self.dispatch_message(ctx.guild.id, ctx.channel.id,'No markov words to pick from')

        all_words.extend(generated)
        return await self.dispatch_message(ctx.guild.id, ctx.channel.id,' '.join(markov_word for markov_word in all_words))
