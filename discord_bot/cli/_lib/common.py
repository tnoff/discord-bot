import asyncio
import contextlib
from asyncio import get_running_loop
from contextlib import asynccontextmanager
import logging
import signal
import sys
from typing import Callable, Iterator, TYPE_CHECKING

from pyaml_env import parse_config
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.metrics import set_meter_provider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import get_aggregated_resources, OTELResourceDetector
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.instrumentation.logging.handler import LoggingHandler
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
from pydantic import ValidationError as PydanticValidationError

from discord_bot.clients.dispatch_client_base import DispatchClientBase
from discord_bot.exceptions import DiscordBotException, CogMissingRequiredArg
from discord_bot.utils.common import get_logger, GeneralConfig
from discord_bot.utils.loop_health import LOOP_HEALTH
# HealthServer is intentionally NOT imported here — it transitively pulls in
# sqlalchemy, which the dispatcher image (base extras only) does not install.
# cli.bot and cli.full import it via discord_bot.cli.health instead.
from discord_bot.utils.memory_profiler import MemoryProfiler
from discord_bot.utils.process_metrics import ProcessMetricsProfiler
from discord_bot.utils.gc_census import GcCensusProfiler

if TYPE_CHECKING:  # pragma: no cover - typing only
    from discord.ext.commands import Bot


def read_config(config_file: str) -> dict:
    '''
    Get values from config file with environment variable substitution
    Uses pyaml-env for env var parsing and Pydantic for validation
    '''
    if config_file is None:
        return {}

    settings = parse_config(config_file) or {}

    if 'general' not in settings:
        raise DiscordBotException('General config section required')
    return settings


def setup_otlp(general_config: GeneralConfig):
    '''
    Configure OpenTelemetry tracing, metrics, and logging.
    Returns the logger_provider (or None if OTLP is disabled).
    '''
    if not (general_config.monitoring and general_config.monitoring.otlp.enabled):
        return None

    tracer_provider = TracerProvider()
    trace.set_tracer_provider(tracer_provider)
    RequestsInstrumentor().instrument(tracer_provider=tracer_provider)
    RedisInstrumentor().instrument(tracer_provider=tracer_provider)
    span_exporter = OTLPSpanExporter()
    trace_provider = trace.get_tracer_provider()
    # Span filtering is the otel-collector's job now — see the
    # filter/drop-ok-high-volume-spans processor in
    # monitoring/collector/config.yaml in docker-apps. Doing it here meant a
    # regex list per service in the ConfigMap, only applied on a pod roll, and
    # it could never reach the redis auto-instrumentation spans that turned out
    # to be 99.5% of the volume (they are created by the redis instrumentor,
    # not by this process's wrappers, but more to the point the filter was
    # name-based and redis names its spans after the bare command, which
    # collides with the HTTP client spans).
    trace_provider.add_span_processor(BatchSpanProcessor(span_exporter))
    resource = get_aggregated_resources(detectors=[OTELResourceDetector()])
    exporter = OTLPMetricExporter()
    reader = PeriodicExportingMetricReader(exporter)
    provider = MeterProvider(resource=resource, metric_readers=[reader])
    set_meter_provider(provider)
    logger_provider = LoggerProvider()
    set_logger_provider(logger_provider)
    log_exporter = OTLPLogExporter()
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
    handler = LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)
    logging.getLogger().addHandler(handler)
    return logger_provider


def setup_logging(general_config: GeneralConfig, logger_provider=None):
    '''Configure application loggers and return the main logger.'''
    print('Starting logging', file=sys.stderr)
    logger = get_logger('main', general_config.logging, otlp_logger=logger_provider)
    discord_bot_logger = get_logger('discord_bot', general_config.logging, otlp_logger=logger_provider)
    # propagate=False so WARNING+ records aren't double-exported via root's OTLP handler
    logger.propagate = False
    discord_bot_logger.propagate = False
    root_logger = logging.getLogger()
    third_party_level = general_config.logging.third_party_log_level if general_config.logging else 30
    root_logger.setLevel(third_party_level)
    discord_logger = get_logger('discord', general_config.logging, otlp_logger=logger_provider)
    discord_logger.setLevel(third_party_level)
    # propagate=False so discord records export once via this logger's own OTLP
    # handler instead of also bubbling up to root's handler (matches main/discord_bot).
    discord_logger.propagate = False
    # discord.py logs voice/gateway lifecycle — websocket close codes (e.g. 4014
    # kicked, 4006 session invalid), reconnect attempts, "voice connection is now
    # closed" — at INFO, which the third-party WARNING gate above drops. Lower just
    # the gateway/voice sub-loggers so those diagnostics reach OTLP without pulling
    # INFO noise from every other third-party library. They propagate up to the
    # discord logger's OTLP handler, so no extra handler is needed.
    gateway_level = general_config.logging.discord_gateway_log_level if general_config.logging else 20
    for sub_logger in ('discord.gateway', 'discord.voice_state', 'discord.voice_client', 'discord.client'):
        logging.getLogger(sub_logger).setLevel(gateway_level)
    return logger


def setup_profiling(general_config: GeneralConfig, logger):
    '''Start memory and process metrics profilers if enabled.'''
    if general_config.monitoring and general_config.monitoring.memory_profiling \
            and general_config.monitoring.memory_profiling.enabled:
        logger.info('Main :: Starting memory profiler')
        interval_seconds = general_config.monitoring.memory_profiling.interval_seconds
        top_n_lines = general_config.monitoring.memory_profiling.top_n_lines
        MemoryProfiler(interval_seconds=interval_seconds, top_n_lines=top_n_lines).start()

    if general_config.monitoring and general_config.monitoring.process_metrics \
            and general_config.monitoring.process_metrics.enabled:
        logger.info('Main :: Starting process metrics profiler')
        interval_seconds = general_config.monitoring.process_metrics.interval_seconds
        ProcessMetricsProfiler(interval_seconds=interval_seconds).start()

    if general_config.monitoring and general_config.monitoring.gc_census \
            and general_config.monitoring.gc_census.enabled:
        logger.info('Main :: Starting GC census profiler')
        interval_seconds = general_config.monitoring.gc_census.interval_seconds
        top_n = general_config.monitoring.gc_census.top_n
        GcCensusProfiler(interval_seconds=interval_seconds, top_n=top_n).start()


class ShutdownState:
    '''Mutable flag shared between the signal handler and the main loop.'''
    def __init__(self):
        self.triggered: bool = False

    def __bool__(self) -> bool:
        return self.triggered


@contextlib.contextmanager
def handle_shutdown_signals(bot: 'Bot') -> Iterator[ShutdownState]:
    '''
    Register SIGTERM/SIGINT handlers for the duration of the with-block.

    Yields a ShutdownState whose .triggered is set to True when a signal arrives.
    Callers may also set .triggered = True directly (e.g. on KeyboardInterrupt)
    so shutdown detection is unified.
    '''
    state = ShutdownState()
    loop = get_running_loop()
    logger = logging.getLogger('main')

    def signal_handler(signum, _frame):
        if state.triggered:
            return
        state.triggered = True
        logger.info(f'Main :: Received {signal.Signals(signum).name}, triggering graceful shutdown...')
        if not bot.is_closed():
            loop.create_task(bot.close())

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    yield state


async def unload_cogs(cog_list: list) -> None:
    '''Call cog_unload() on every cog that exposes it, logging any errors.'''
    logger = logging.getLogger('main')
    for cog in cog_list:
        if hasattr(cog, 'cog_unload'):
            try:
                logger.debug(f'Main :: Calling cog_unload on {cog.__class__.__name__}')
                await cog.cog_unload()
            except Exception as e:
                logger.exception(f'Main :: Error during cog_unload for {cog.__class__.__name__}: {str(e)}')


@asynccontextmanager
async def bot_lifecycle(bot: 'Bot', cog_list: list, health_server=None,
                        on_shutdown: Callable | None = None):
    '''
    Async context manager encapsulating the shared bot try/except/finally pattern.

    Registers shutdown signal handlers, loads cogs, starts the optional health
    server, then yields control so the caller can run bot.start() or bot.login().
    On exit (normal or signal), unloads cogs, closes the bot, and calls on_shutdown
    if provided.

    Usage::

        async with bot_lifecycle(bot, cog_list, health_server=hs,
                                  on_shutdown=dispatcher.stop):
            logger.info('Starting…')
            await bot.start(token)
    '''
    logger = logging.getLogger('main')
    with handle_shutdown_signals(bot) as shutdown:
        async with bot:
            for cog in cog_list:
                await bot.add_cog(cog)
            if health_server:
                asyncio.create_task(health_server.serve())
            try:
                yield shutdown
            except KeyboardInterrupt:
                logger.info('Main :: Received keyboard interrupt, shutting down gracefully...')
                shutdown.triggered = True
            except Exception as exc:
                logger.debug('Main :: Shutting down main loop: %s', str(exc))
            finally:
                if shutdown:
                    await unload_cogs(cog_list)
                    if not bot.is_closed():
                        await bot.close()
                    if on_shutdown is not None:
                        await on_shutdown()
                    logger.info('Main :: Graceful shutdown complete')


def run_loop(coro) -> None:
    '''Schedule *coro* on the running event loop or start a new one.'''
    logger = logging.getLogger('main')
    try:
        loop = get_running_loop()
        logger.debug('Main :: Found existing running loop, re-using')
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        logger.debug('Main :: Async event loop already running. Adding coroutine to the event loop.')
        loop.create_task(coro)
    else:
        logger.debug('Main :: Starting new discord bot instance')
        asyncio.run(coro)


@contextlib.contextmanager
def shutdown_event_signals() -> Iterator[asyncio.Event]:
    '''
    Register SIGTERM/SIGINT handlers that set an asyncio.Event, for the no-bot
    worker pods (broker/downloader) whose main_loop awaits an Event rather than a
    Bot's closed state.

    Yields the stop_event; a signal (thread-safely) sets it so the caller's
    ``await stop_event.wait()`` returns and shutdown drains.  Must be entered from
    inside a running event loop.
    '''
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    logger = logging.getLogger('main')

    def _handle_signal(signum, _frame):
        logger.info('Main :: Received %s, triggering graceful shutdown...',
                    signal.Signals(signum).name)
        loop.call_soon_threadsafe(stop_event.set)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    yield stop_event


def setup_loop_health(general_config: GeneralConfig) -> None:
    '''Apply the configured staleness window to this process's loop-health registry.

    Every entrypoint calls this before starting loops, so the heartbeat gauge and
    the health server's probe agree on how long a loop may go without a
    successful iteration. Left at the default when unconfigured.
    '''
    if general_config.monitoring and general_config.monitoring.loop_health:
        LOOP_HEALTH.configure(general_config.monitoring.loop_health.stale_after_seconds)


def setup_observability(general_config: GeneralConfig) -> logging.Logger:
    '''Configure OTLP, logging, and profiling. Returns the main logger.'''
    logger_provider = setup_otlp(general_config)
    logger = setup_logging(general_config, logger_provider=logger_provider)
    setup_profiling(general_config, logger)
    setup_loop_health(general_config)
    return logger


def register_on_ready(bot: 'Bot', general_config: GeneralConfig, logger) -> None:
    '''Register an on_ready event that logs guild membership and enforces the rejectlist.'''
    rejectlist_guilds = list(general_config.rejectlist_guilds)
    logger.info(f'Main :: Gathered guild reject list {rejectlist_guilds}')

    @bot.event
    async def on_ready():
        logger.info(f'Main :: Starting bot, logged in as {bot.user} (ID: {bot.user.id})')
        guilds = [guild async for guild in bot.fetch_guilds(limit=150)]
        for guild in guilds:
            if guild.id in rejectlist_guilds:
                logger.info(f'Main :: Bot currently in guild {guild.id} thats within reject list, leaving server')
                await guild.leave()
                continue
            logger.info(f'Main :: Bot associated with guild {guild.id} with name "{guild.name}"')



def load_cogs(bot: 'Bot', cog_classes: list, settings: dict, stores,
              dispatcher: DispatchClientBase, redis_manager=None) -> list:
    '''Attempt to instantiate each cog class; skip those missing required args.

    stores is the DatabaseStores bundle, in the slot db_engine occupied before
    MR 4b. Cogs that need no persistence still take the argument and ignore it,
    which is what lets this loop call every constructor the same way.
    '''
    logger = logging.getLogger('main')
    cogs = []
    for cog_cls in cog_classes:
        try:
            cogs.append(cog_cls(bot, settings, dispatcher, stores,
                                redis_manager=redis_manager))
        except CogMissingRequiredArg as e:
            logger.debug(f'Main :: Cannot add cog {str(cog_cls)}, {str(e)}')
    return cogs


def parse_and_validate_config(config_file: str) -> tuple[dict, GeneralConfig]:
    '''Read config file and return (raw settings dict, validated GeneralConfig).'''
    settings = read_config(config_file)
    try:
        general_config = GeneralConfig(**settings['general'])
    except PydanticValidationError as exc:
        print(f'Invalid config, general section does not match schema: {str(exc)}', file=sys.stderr)
        raise DiscordBotException('Invalid general config') from exc
    return settings, general_config


def require_discord_token(general_config: GeneralConfig) -> str:
    '''
    Return the Discord token, raising for gateway processes that must have one.

    discord_token is optional in GeneralConfig (broker/downloader never connect to
    Discord); the gateway entrypoints (bot/dispatcher/full) call this so a missing
    token fails with a clear message instead of a cryptic discord.py login error.
    '''
    if not general_config.discord_token:
        raise DiscordBotException('discord_token is required to connect to Discord')
    return general_config.discord_token
