from functools import partial
from typing import Optional

from discord import ChannelType
from discord.ext.commands import Bot, Context, group
from discord.errors import DiscordServerError
from opentelemetry.metrics import Observation
from pydantic import BaseModel, Field

from discord_core.exceptions import CogMissingRequiredArg
from discord_core.utils.common import return_loop_runner
from discord_core.utils.loop_health import LOOP_HEALTH, health_aware_queue_get
from discord_core.utils.otel import AttributeNaming, MetricNaming, METER_PROVIDER, create_observable_gauge, loop_heartbeat_observations
from discord_core.clients.dispatch_client_base import DispatchClientBase
from discord_core.types.dispatch_result import ChannelHistoryResult, GuildEmojisResult

from discord_core.interfaces.database_protocols import MarkovStore

from discord_gateway.cogs.common import CogHelperBase, envelope_from_ctx
from discord_gateway.command_logic.markov import MarkovCommands
from discord_gateway.loop_logic.markov import MarkovLoop
from discord_gateway.utils.otel_command import command_wrapper

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
        create_observable_gauge(METER_PROVIDER, MetricNaming.RESULT_QUEUE_DEPTH.value, self.__result_queue_depth_callback, 'Markov dispatch result queue depth')

    def __result_queue_depth_callback(self, _options):
        '''
        Depth of the dispatch result queue — climbs if the consumer stalls or dies.
        '''
        depth = self._result_queue.qsize() if self._result_queue else 0
        return [
            Observation(depth, attributes={
                AttributeNaming.BACKGROUND_JOB.value: 'markov_result',
                AttributeNaming.RESULT_TYPE.value: 'dispatch'
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

    @property
    def loop_logic(self) -> MarkovLoop:
        '''One iteration of each loop, over this cog's store and dispatcher.'''
        return MarkovLoop(self.logger, self.markov_store, self.loop_sleep_interval,
                          self.message_check_limit, self.history_retention_days, self._emoji_cache,
                          self.dispatch_guild_emojis, self.dispatch_channel_history)

    async def _markov_request_loop(self):
        '''
        Producer loop: submit Discord fetch requests for each tracked channel.
        '''
        await self.loop_logic.request()

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
        '''Cache a guild's emojis from a result.'''
        return await self.loop_logic.process_emojis_result(result)

    async def _process_history_result(self, result: ChannelHistoryResult):
        '''Save the messages of a channel history result to the markov chain.'''
        return await self.loop_logic.process_history_result(result)

    @group(name='markov', invoke_without_command=False)
    async def markov(self, ctx: Context):
        '''
        Markov functions. Use '!help markov'
        '''
        if ctx.invoked_subcommand is None:
            await ctx.send('Invalid sub command passed...')

    @property
    def markov_logic(self) -> MarkovCommands:
        '''
        The ``!markov`` subcommands, built from this cog's current store and reject list.
        Stateless, so rebuilt per use rather than cached: replacing the store on the cog
        (as the tests do) is picked up with nothing to keep in sync.
        '''
        return MarkovCommands(self.dispatch_message, self.logger, self.markov_store,
                              self.server_reject_list, self._is_markov_channel)

    async def _is_markov_channel(self, channel_id: int) -> bool:
        '''Whether markov can run in this channel: it has to be a text or voice channel.'''
        channel = await self.bot.fetch_channel(channel_id)
        return channel.type in [ChannelType.text, ChannelType.voice]

    @markov.command(name='on')
    @command_wrapper
    async def on(self, ctx: Context):
        '''
        Turn markov on for channel
        '''
        return await self.markov_logic.on(envelope_from_ctx(ctx))

    @markov.command(name='off')
    @command_wrapper
    async def off(self, ctx: Context):
        '''
        Turn markov off for channel
        '''
        return await self.markov_logic.off(envelope_from_ctx(ctx))

    @markov.command(name='list-channels')
    @command_wrapper
    async def list_channels(self, ctx: Context):
        '''
        List channels markov is enabled for in this server
        '''
        return await self.markov_logic.list_channels(envelope_from_ctx(ctx))

    @markov.command(name='speak')
    @command_wrapper
    async def speak(self, ctx: Context,
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
        return await self.markov_logic.speak(envelope_from_ctx(ctx), first_word, sentence_length)
