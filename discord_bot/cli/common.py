import asyncio
from asyncio import run, get_running_loop
import logging
import re
import signal
import sys
from typing import List

from pyaml_env import parse_config
from sqlalchemy.engine.url import make_url
from discord import Intents
from discord.ext.commands import Bot, when_mentioned_or
from pydantic import ValidationError as PydanticValidationError
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

from discord_bot.cogs.common import CogHelperBase
from discord_bot.exceptions import DiscordBotException, CogMissingRequiredArg
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


async def main_loop(bot: Bot, cog_list: List[CogHelperBase], token: str, health_server=None,
                    dispatch_gateway: bool = True):
    '''
    Main loop for starting bot
    Includes logic to handle stops and cog removals
    '''
    logger = logging.getLogger('main')
    loop = get_running_loop()
    shutdown_triggered = False

    def signal_handler(signum, frame): #pylint:disable=unused-argument
        '''Handle SIGTERM and SIGINT for graceful shutdown'''
        nonlocal shutdown_triggered
        if shutdown_triggered:
            return
        shutdown_triggered = True
        sig_name = signal.Signals(signum).name
        logger.info(f'Main :: Received {sig_name}, triggering graceful shutdown...')
        if not bot.is_closed():
            loop.create_task(bot.close())

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        async with bot:
            for cog in cog_list:
                await bot.add_cog(cog)
            if health_server:
                asyncio.create_task(health_server.serve())
            if dispatch_gateway:
                logger.info('Main :: Starting bot in gateway mode')
                await bot.start(token)
            else:
                logger.info('Main :: Starting bot in HTTP-only mode (no gateway connection)')
                await bot.login(token)
                while not bot.is_closed():
                    await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info('Main :: Received keyboard interrupt, shutting down gracefully...')
        shutdown_triggered = True
    except Exception as e:
        logger.debug('Main :: Shutting down main loop: %s', str(e))
        return
    finally:
        if shutdown_triggered:
            for cog in cog_list:
                if hasattr(cog, 'cog_unload'):
                    try:
                        logger.debug(f'Main :: Calling cog_unload on {cog.__class__.__name__}')
                        await cog.cog_unload()
                    except Exception as e:
                        logger.exception(f'Main :: Error during cog_unload for {cog.__class__.__name__}: {str(e)}')
            if not bot.is_closed():
                await bot.close()
            logger.info('Main :: Graceful shutdown complete')


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


def run_bot(general_config: GeneralConfig, bot: Bot, cog_list: list, health_server=None,
            dispatch_gateway: bool = True):
    '''Schedule main_loop on an existing event loop or start a new one.'''
    logger = logging.getLogger('main')
    token = general_config.discord_token
    try:
        loop = get_running_loop()
        logger.debug('Main :: Found existing running loop, re-using')
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        logger.debug('Main :: Async event loop already running. Adding coroutine to the event loop.')
        loop.create_task(main_loop(bot, cog_list, token, health_server=health_server,
                                   dispatch_gateway=dispatch_gateway))
    else:
        logger.debug('Main :: Starting new discord bot instance')
        run(main_loop(bot, cog_list, token, health_server=health_server,
                      dispatch_gateway=dispatch_gateway))


def build_bot(general_config: GeneralConfig, settings: dict = None) -> tuple[Bot, list]:
    '''
    Construct the Bot instance and base cog list (error handler only).
    Callers add their own cogs on top.
    '''
    logger = logging.getLogger('main')
    logger.debug('Main :: Generating Intents')
    intents = Intents.default()
    for intent in list(general_config.intents):
        logger.debug(f'Main :: Adding extra intents: {intent}')
        setattr(intents, intent, True)

    bot = Bot(
        command_prefix=when_mentioned_or('!'),
        description='Discord bot',
        intents=intents,
    )

    from discord_bot.cogs.error import CommandErrorHandler  #pylint:disable=import-outside-toplevel
    cog_list = [CommandErrorHandler(bot, settings or {})]
    return bot, cog_list


def load_cogs(bot: Bot, cog_classes: list, settings: dict, db_engine) -> list:
    '''Attempt to instantiate each cog class; skip those missing required args.'''
    logger = logging.getLogger('main')
    cogs = []
    for cog_cls in cog_classes:
        try:
            cogs.append(cog_cls(bot, settings, db_engine))
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
