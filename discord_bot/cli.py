import asyncio
from asyncio import run, get_running_loop
import logging
import re
import signal
from sys import stderr
from typing import List

import click
from pyaml_env import parse_config
from discord import Intents
from discord.ext.commands import Bot, when_mentioned_or
from pydantic import ValidationError as PydanticValidationError
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.metrics import set_meter_provider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace import ReadableSpan, SpanProcessor
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import get_aggregated_resources, OTELResourceDetector
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

from discord_bot.cogs.error import CommandErrorHandler
from discord_bot.cogs.delete_messages import DeleteMessages
from discord_bot.cogs.database_backup import DatabaseBackup
from discord_bot.cogs.common import CogHelper
from discord_bot.cogs.general import General
from discord_bot.cogs.markov import Markov
from discord_bot.cogs.music import Music
from discord_bot.cogs.role import RoleAssignment
from discord_bot.cogs.urban import UrbanDictionary
from discord_bot.database import BASE
from discord_bot.exceptions import DiscordBotException, CogMissingRequiredArg
from discord_bot.utils.common import get_logger, GeneralConfig
from discord_bot.utils.health_server import HealthServer
from discord_bot.utils.memory_profiler import MemoryProfiler
from discord_bot.utils.process_metrics import ProcessMetricsProfiler

POSSIBLE_COGS = [
    DeleteMessages,
    DatabaseBackup,
    Markov,
    Music,
    RoleAssignment,
    UrbanDictionary,
    General,
]

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

async def main_loop(bot: Bot, cog_list: List[CogHelper], token: str, health_server=None):
    '''
    Main loop for starting bot
    Includes logic to handle stops and cog removals
    '''
    logger = logging.getLogger('main')
    # Set up signal handlers for graceful shutdown
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
        # Schedule the bot to close
        if not bot.is_closed():
            loop.create_task(bot.close())

    # Register signal handlers for both SIGTERM (Docker stop) and SIGINT (Ctrl+C)
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        async with bot:
            for cog in cog_list:
                await bot.add_cog(cog)
            if health_server:
                asyncio.create_task(health_server.serve())
            await bot.start(token)
    except KeyboardInterrupt:
        logger.info('Main :: Received keyboard interrupt, shutting down gracefully...')
        shutdown_triggered = True
    except Exception as e:
        logger.debug('Main :: Shutdown down main loop', str(e))
        return
    finally:
        if shutdown_triggered:
            # Call cog_unload on all cogs to allow graceful shutdown
            for cog in cog_list:
                if hasattr(cog, 'cog_unload'):
                    try:
                        logger.debug(f'Main :: Calling cog_unload on {cog.__class__.__name__}')
                        await cog.cog_unload()
                    except Exception as e:
                        logger.exception(f'Main :: Error during cog_unload for {cog.__class__.__name__}: {str(e)}')
            # Ensure bot connection is closed
            if not bot.is_closed():
                await bot.close()
            logger.info('Main :: Graceful shutdown complete')

@click.command()
@click.argument('config_file', type=click.Path(dir_okay=False))
def main(config_file): #pylint:disable=too-many-statements
    '''
    Main loop
    '''

    # First generate settings
    settings = read_config(config_file)
    try:
        # Validate using Pydantic
        general_config = GeneralConfig(**settings['general'])
    except PydanticValidationError as exc:
        print(f'Invalid config, general section does not match schema: {str(exc)}', file=stderr)
        raise DiscordBotException('Invalid general config') from exc

    # Grab db engine for possible dump or load commands
    if general_config.sql_connection_statement:
        db_engine = create_engine(general_config.sql_connection_statement, pool_pre_ping=True)
        BASE.metadata.create_all(db_engine)
        BASE.metadata.bind = db_engine
    else:
        print('Unable to find sql statement in settings, assuming no db', file=stderr)
        db_engine = None

    try:
        logger_provider = None
        if general_config.monitoring and general_config.monitoring.otlp.enabled:
            tracer_provider = TracerProvider()
            trace.set_tracer_provider(tracer_provider)
            # Add some tracing instrumentation
            # https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/sqlalchemy/sqlalchemy.html
            RequestsInstrumentor().instrument(tracer_provider=tracer_provider)
            SQLAlchemyInstrumentor().instrument(tracer_provider=tracer_provider, enable_commenter=True, commenter_options={})
            # Set span exporters
            span_exporter = OTLPSpanExporter()
            trace_provider = trace.get_tracer_provider()
            batch_processor = BatchSpanProcessor(span_exporter)
            if general_config.monitoring.otlp.filter_high_volume_spans:
                # Wrap batch processor to filter out OK spans matching configured patterns
                patterns = general_config.monitoring.otlp.high_volume_span_patterns
                trace_provider.add_span_processor(FilterOKRetrySpans(batch_processor, patterns))
            else:
                trace_provider.add_span_processor(batch_processor)
            # Set metrics
            # Need to grab this directly for one reason or another with metrics
            resource = get_aggregated_resources(detectors=[OTELResourceDetector()])
            exporter = OTLPMetricExporter()
            reader = PeriodicExportingMetricReader(exporter)
            provider = MeterProvider(resource=resource, metric_readers=[reader])
            set_meter_provider(provider)
            # Set logging
            logger_provider = LoggerProvider()
            set_logger_provider(logger_provider)
            log_exporter = OTLPLogExporter()
            logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
            handler = LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)
            logging.getLogger().addHandler(handler)

        # Grab logger
        print('Starting logging', file=stderr)
        logger = get_logger('main', general_config.logging)



        # Set root logger level to suppress verbose third-party library logs
        # (discord.py, yt-dlp, etc.) while keeping our application loggers at configured levels
        # Default to WARNING (30) if not configured
        root_logger = logging.getLogger()
        third_party_level = general_config.logging.third_party_log_level if general_config.logging else 30
        root_logger.setLevel(third_party_level)

        # Add loggers for discord.py
        discord_logger = get_logger('discord', general_config.logging, otlp_logger=logger_provider)
        discord_logger.setLevel(third_party_level)


        # Start memory profiling if enabled
        if general_config.monitoring and general_config.monitoring.memory_profiling and general_config.monitoring.memory_profiling.enabled:
            logger.info('Main :: Starting memory profiler')
            get_logger('memory_profiler', general_config.logging, otlp_logger=logger_provider).setLevel(logging.INFO)
            interval_seconds = general_config.monitoring.memory_profiling.interval_seconds
            top_n_lines = general_config.monitoring.memory_profiling.top_n_lines
            memory_profiler = MemoryProfiler(interval_seconds=interval_seconds, top_n_lines=top_n_lines)
            memory_profiler.start()

        # Start process metrics profiling if enabled
        if general_config.monitoring and general_config.monitoring.process_metrics and general_config.monitoring.process_metrics.enabled:
            logger.info('Main :: Starting process metrics profiler')
            get_logger('process_metrics', general_config.logging, otlp_logger=logger_provider).setLevel(logging.INFO)
            interval_seconds = general_config.monitoring.process_metrics.interval_seconds
            process_metrics_profiler = ProcessMetricsProfiler(interval_seconds=interval_seconds)
            process_metrics_profiler.start()

        # Run main bot
        main_runner(general_config, settings, db_engine)
    finally:
        # Ensure database engine is properly disposed
        if db_engine:
            db_engine.dispose()

def main_runner(general_config: GeneralConfig, settings: dict, db_engine: Engine):
    '''
    Main runner logic
    '''
    logger = logging.getLogger('main')
    token = general_config.discord_token

    logger.debug('Main :: Generating Intents')
    intents = Intents.default()
    intent_list = list(general_config.intents)
    if intent_list:
        logger.debug(f'Main :: Adding extra intents: {intent_list}')
        for intent in intent_list:
            setattr(intents, intent, True)

    bot = Bot(
        command_prefix=when_mentioned_or("!"),
        description='Discord bot',
        intents=intents,
    )

    cog_list = [
        CommandErrorHandler(bot, general_config),
    ]
    for cog in POSSIBLE_COGS:
        try:
            new_cog = cog(bot, settings, db_engine)
            cog_list.append(new_cog)
        except CogMissingRequiredArg as e:
            logger.debug(f'Main :: Cannot add cog {str(cog)}, {str(e)}')

    health_server = None
    if general_config.monitoring and general_config.monitoring.health_server \
            and general_config.monitoring.health_server.enabled:
        get_logger('health_server', general_config.logging)
        health_server = HealthServer(bot, port=general_config.monitoring.health_server.port)

    # Make sure we cast to string here just to keep it consistent
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


    try:
        loop = get_running_loop()
        logger.debug('Main :: Found existing running loop, re-using')
    except RuntimeError:  # 'RuntimeError: There is no current event loop...'
        loop = None

    if loop and loop.is_running():
        logger.debug('Main :: Async event loop already running. Adding coroutine to the event loop.')
        loop.create_task(main_loop(bot, cog_list, token, health_server=health_server))
    else:
        logger.debug('Main :: Starting new discord bot instance')
        run(main_loop(bot, cog_list, token, health_server=health_server))



if __name__ == '__main__':
    main() #pylint: disable=no-value-for-parameter
