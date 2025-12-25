from asyncio import run, get_running_loop
import logging
from logging import RootLogger
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

async def main_loop(bot: Bot, cog_list: List[CogHelper], token: str, logger: RootLogger):
    '''
    Main loop for starting bot
    Includes logic to handle stops and cog removals
    '''
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
        GeneralConfig(**settings['general'])
    except PydanticValidationError as exc:
        print(f'Invalid config, general section does not match schema: {str(exc)}', file=stderr)
        raise DiscordBotException('Invalid general config') from exc

    # Grab db engine for possible dump or load commands
    try:
        db_engine = create_engine(settings['general']['sql_connection_statement'], pool_pre_ping=True)
        BASE.metadata.create_all(db_engine)
        BASE.metadata.bind = db_engine
    except KeyError:
        print('Unable to find sql statement in settings, assuming no db', file=stderr)
        db_engine = None

    try:
        # Instrument otlp if enabled
        monitoring_settings = settings['general'].get('monitoring', {})
        otlp_settings = monitoring_settings.get('otlp', {})

        logger_provider = None
        if otlp_settings.get('enabled', False):
            tracer_provider = TracerProvider()
            trace.set_tracer_provider(tracer_provider)
            # Add some tracing instrumentation
            # https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/sqlalchemy/sqlalchemy.html
            RequestsInstrumentor().instrument(tracer_provider=tracer_provider)
            SQLAlchemyInstrumentor().instrument(tracer_provider=tracer_provider, enable_commenter=True, commenter_options={})
            # Set span exporters
            span_exporter = OTLPSpanExporter()
            trace.get_tracer_provider().add_span_processor(
                BatchSpanProcessor(span_exporter)
            )
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
        logger = get_logger('main', settings['general'].get('logging', {}))



        # Set root logger level to suppress verbose third-party library logs
        # (discord.py, yt-dlp, etc.) while keeping our application loggers at configured levels
        # Default to WARNING (30) if not configured
        root_logger = logging.getLogger()
        third_party_level = settings['general'].get('logging', {}).get('third_party_log_level', 30)
        root_logger.setLevel(third_party_level)

        # Add loggers for discord.py
        discord_logger = get_logger('discord', settings['general'].get('logging', {}), otlp_logger=logger_provider)
        discord_logger.setLevel(third_party_level)


        # Start memory profiling if enabled
        memory_profiling_settings = monitoring_settings.get('memory_profiling', {})
        if memory_profiling_settings.get('enabled', False):
            logger.info('Main :: Starting memory profiler')
            memory_profiler_logger = get_logger('memory_profiler', settings['general'].get('logging', {}), otlp_logger=logger_provider)
            memory_profiler_logger.setLevel(logging.INFO)
            interval_seconds = memory_profiling_settings.get('interval_seconds', 60)
            top_n_lines = memory_profiling_settings.get('top_n_lines', 25)
            memory_profiler = MemoryProfiler(memory_profiler_logger, interval_seconds=interval_seconds, top_n_lines=top_n_lines)
            memory_profiler.start()

        # Start process metrics profiling if enabled
        process_metrics_settings = monitoring_settings.get('process_metrics', {})
        if process_metrics_settings.get('enabled', False):
            logger.info('Main :: Starting process metrics profiler')
            process_metrics_logger = get_logger('process_metrics', settings['general'].get('logging', {}), otlp_logger=logger_provider)
            process_metrics_logger.setLevel(logging.INFO)
            interval_seconds = process_metrics_settings.get('interval_seconds', 15)
            process_metrics_profiler = ProcessMetricsProfiler(process_metrics_logger, interval_seconds=interval_seconds)
            process_metrics_profiler.start()

        # Run main bot
        main_runner(settings, logger, db_engine)
    finally:
        # Ensure database engine is properly disposed
        if db_engine:
            db_engine.dispose()

def main_runner(settings: dict, logger: RootLogger, db_engine: Engine):
    '''
    Main runner logic
    '''
    try:
        token = settings['general']['discord_token']
    except KeyError as exc:
        raise DiscordBotException('Unable to run bot without token') from exc

    logger.debug('Main :: Generating Intents')
    intents = Intents.default()
    try:
        intent_list = list(settings['general']['intents'])
        logger.debug(f'Main :: Adding extra intents: {intent_list}')
        for intent in intent_list:
            setattr(intents, intent, True)
    except KeyError:
        pass

    bot = Bot(
        command_prefix=when_mentioned_or("!"),
        description='Discord bot',
        intents=intents,
    )

    cog_list = [
        CommandErrorHandler(bot, settings),
    ]
    for cog in POSSIBLE_COGS:
        try:
            new_cog = cog(bot, settings, db_engine)
            cog_list.append(new_cog)
        except CogMissingRequiredArg as e:
            logger.debug(f'Main :: Cannot add cog {str(cog)}, {str(e)}')

    # Make sure we cast to string here just to keep it consistent
    rejectlist_guilds = []
    for guild in settings['general'].get('rejectlist_guilds', []):
        rejectlist_guilds.append(guild)
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
        loop.create_task(main_loop(bot, cog_list, token, logger))
    else:
        logger.debug('Main :: Starting new discord bot instance')
        run(main_loop(bot, cog_list, token, logger))



if __name__ == '__main__':
    main() #pylint: disable=no-value-for-parameter
