import asyncio
import contextlib
from asyncio import get_running_loop
from contextlib import asynccontextmanager
import logging
import re
import signal
import sys
from typing import Callable, Iterator, List

from pyaml_env import parse_config
from sqlalchemy.engine.url import make_url
from discord import Intents
from discord.ext.commands import Bot, when_mentioned_or
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider, ReadableSpan, SpanProcessor
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
from pydantic import ValidationError as PydanticValidationError

from discord_bot.clients.dispatch_client_base import DispatchClientBase
from discord_bot.exceptions import DiscordBotException, CogMissingRequiredArg
from discord_bot.servers.health_server import HealthServer
from discord_bot.utils.common import get_logger, GeneralConfig
from discord_bot.utils.memory_profiler import MemoryProfiler
from discord_bot.utils.process_metrics import ProcessMetricsProfiler


class FilterOKRetrySpans(SpanProcessor):
    '''
    Filter spammy spans for the retry clients.
    Wraps another SpanProcessor and only forwards spans that should not be filtered.
    Accepts a list of regex patterns to match span names against.
    '''
    def __init__(self, next_processor: SpanProcessor, patterns: List[str]):
        self._next = next_processor
        self._patterns = [re.compile(p) for p in patterns]

    def on_start(self, span, parent_context=None):
        self._next.on_start(span, parent_context)

    def on_end(self, span: ReadableSpan):
        '''
        Overrides on_end, filter OK spans matching configured patterns
        '''
        if span.status.is_ok:
            for pattern in self._patterns:
                if pattern.match(span.name):
                    return  # Don't forward to next processor
        # Forward to next processor
        self._next.on_end(span)

    def shutdown(self):
        self._next.shutdown()

    def force_flush(self, timeout_millis=30000):
        return self._next.force_flush(timeout_millis)


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
    span_exporter = OTLPSpanExporter()
    trace_provider = trace.get_tracer_provider()
    batch_processor = BatchSpanProcessor(span_exporter)
    if general_config.monitoring.otlp.filter_high_volume_spans:
        patterns = general_config.monitoring.otlp.high_volume_span_patterns
        trace_provider.add_span_processor(FilterOKRetrySpans(batch_processor, patterns))
    else:
        trace_provider.add_span_processor(batch_processor)
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


def setup_logging(general_config: GeneralConfig):
    '''Configure application loggers and return the main logger.'''
    print('Starting logging', file=sys.stderr)
    logger = get_logger('main', general_config.logging)
    get_logger('discord_bot', general_config.logging)
    root_logger = logging.getLogger()
    third_party_level = general_config.logging.third_party_log_level if general_config.logging else 30
    root_logger.setLevel(third_party_level)
    discord_logger = get_logger('discord', general_config.logging)
    discord_logger.setLevel(third_party_level)
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


class ShutdownState:
    '''Mutable flag shared between the signal handler and the main loop.'''
    def __init__(self):
        self.triggered: bool = False

    def __bool__(self) -> bool:
        return self.triggered


@contextlib.contextmanager
def handle_shutdown_signals(bot: Bot) -> Iterator[ShutdownState]:
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
async def bot_lifecycle(bot: Bot, cog_list: list, health_server=None,
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


def setup_observability(general_config: GeneralConfig) -> logging.Logger:
    '''Configure OTLP, logging, and profiling. Returns the main logger.'''
    setup_otlp(general_config)
    logger = setup_logging(general_config)
    setup_profiling(general_config, logger)
    return logger


def setup_health_server(bot: Bot, general_config: GeneralConfig):
    '''Return a HealthServer if monitoring.health_server.enabled, else None.'''
    if (general_config.monitoring and general_config.monitoring.health_server
            and general_config.monitoring.health_server.enabled):
        return HealthServer(bot, port=general_config.monitoring.health_server.port)
    return None


def register_on_ready(bot: Bot, general_config: GeneralConfig, logger) -> None:
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


def build_bot(general_config: GeneralConfig) -> Bot:
    '''Construct and return the Bot instance.'''
    logger = logging.getLogger('main')
    logger.debug('Main :: Generating Intents')
    intents = Intents.default()
    for intent in list(general_config.intents):
        logger.debug(f'Main :: Adding extra intents: {intent}')
        setattr(intents, intent, True)

    return Bot(
        command_prefix=when_mentioned_or('!'),
        description='Discord bot',
        intents=intents,
    )


def load_cogs(bot: Bot, cog_classes: list, settings: dict, db_engine,
              dispatcher: DispatchClientBase) -> list:
    '''Attempt to instantiate each cog class; skip those missing required args.'''
    logger = logging.getLogger('main')
    cogs = []
    for cog_cls in cog_classes:
        try:
            cogs.append(cog_cls(bot, settings, dispatcher, db_engine))
        except CogMissingRequiredArg as e:
            logger.debug(f'Main :: Cannot add cog {str(cog_cls)}, {str(e)}')
    return cogs


def make_async_db_url(connection_string: str):
    '''Convert a plain SQLAlchemy URL to its async-driver equivalent.'''
    url = make_url(connection_string)
    if url.drivername.startswith('postgresql'):
        return url.set(drivername='postgresql+asyncpg')
    if url.drivername == 'sqlite':
        return url.set(drivername='sqlite+aiosqlite')
    return url


def parse_and_validate_config(config_file: str) -> tuple[dict, GeneralConfig]:
    '''Read config file and return (raw settings dict, validated GeneralConfig).'''
    settings = read_config(config_file)
    try:
        general_config = GeneralConfig(**settings['general'])
    except PydanticValidationError as exc:
        print(f'Invalid config, general section does not match schema: {str(exc)}', file=sys.stderr)
        raise DiscordBotException('Invalid general config') from exc
    return settings, general_config
