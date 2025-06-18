from asyncio import run, get_running_loop
from enum import Enum
import logging
from logging import RootLogger
from sys import stderr
from typing import List

import click
from discord import Intents
from discord.ext.commands import Bot, when_mentioned_or
from jsonschema import ValidationError
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

from pyaml_env import parse_config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.base import Engine

from discord_bot.cogs.error import CommandErrorHandler
from discord_bot.cogs.delete_messages import DeleteMessages
from discord_bot.cogs.common import CogHelper
from discord_bot.cogs.general import General
from discord_bot.cogs.markov import Markov
from discord_bot.cogs.music import Music
from discord_bot.cogs.role import RoleAssignment
from discord_bot.cogs.urban import UrbanDictionary
from discord_bot.database import BASE, MarkovRelation, MarkovChannel
from discord_bot.exceptions import DiscordBotException, CogMissingRequiredArg
from discord_bot.utils.common import get_logger, validate_config, GENERAL_SECTION_SCHEMA

POSSIBLE_COGS = [
    DeleteMessages,
    Markov,
    Music,
    RoleAssignment,
    UrbanDictionary,
    General,
]

def read_config(config_file: str) -> dict:
    '''
    Get values from config file
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
    try:
        async with bot:
            for cog in cog_list:
                await bot.add_cog(cog)
            await bot.start(token)
    except Exception as e:
        logger.debug('Main :: Shuttdown down main loop', str(e))
        return

class CLIRunners(Enum):
    '''
    Enum for CLI Runner Commands
    '''
    CLEAR_MARKOV = 'clear-markov-relations'

@click.command()
@click.option('-e', '--execute', type=click.Choice([CLIRunners.CLEAR_MARKOV.value]))
@click.argument('config_file', type=click.Path(dir_okay=False))
def main(execute, config_file): #pylint:disable=too-many-statements
    '''
    Main loop
    '''

    # First generate settings
    settings = read_config(config_file)
    try:
        validate_config(settings['general'], GENERAL_SECTION_SCHEMA)
    except ValidationError as exc:
        print(f'Invalid config, general section does not match schema: {str(exc)}', file=stderr)

    # Grab db engine for possible dump or load commands
    try:
        db_engine = create_engine(settings['general']['sql_connection_statement'], pool_pre_ping=True)
        BASE.metadata.create_all(db_engine)
        BASE.metadata.bind = db_engine
    except KeyError:
        print('Unable to find sql statement in settings, assuming no db', file=stderr)
        db_engine = None

    # Instrument otlp if enabled
    otlp_settings = settings['general'].get('otlp', {})

    if otlp_settings:
        tracer_provider = TracerProvider()
        trace.set_tracer_provider(tracer_provider)
        # Add some tracing instrumentation
        # https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/sqlalchemy/sqlalchemy.html
        RequestsInstrumentor().instrument(tracer_provider=tracer_provider)
        SQLAlchemyInstrumentor().instrument(tracer_provider=tracer_provider, enable_commenter=True, commenter_options={})
        # Set span exporters
        span_exporter = OTLPSpanExporter(endpoint=otlp_settings["trace_endpoint"])
        trace.get_tracer_provider().add_span_processor(
            BatchSpanProcessor(span_exporter)
        )
        # Set metrics
        # Need to grab this directly for one reason or another with metrics
        resource = get_aggregated_resources(detectors=[OTELResourceDetector()])
        exporter = OTLPMetricExporter(endpoint=otlp_settings['metric_endpoint'], insecure=True)
        reader = PeriodicExportingMetricReader(exporter)
        provider = MeterProvider(resource=resource, metric_readers=[reader])
        set_meter_provider(provider)
        # Set logging
        logger_provider = LoggerProvider()
        set_logger_provider(logger_provider)
        log_exporter = OTLPLogExporter(endpoint=otlp_settings['log_endpoint'], insecure=True)
        logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
        handler = LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)
        logging.getLogger().addHandler(handler)

    # Grab logger
    print('Starting logging', file=stderr)
    logger = get_logger('main', settings['general'].get('logging', {}))

    if execute == CLIRunners.CLEAR_MARKOV.value:
        clear_markov_relations(db_engine)
        return

    # Default to run function
    main_runner(settings, logger, db_engine)

def clear_markov_relations(db_engine: Engine):
    '''
    Clear markov relations from db
    '''
    if not db_engine:
        click.echo('Unable to run markov clear relations, no db given')
        return False
    db_session = sessionmaker(bind=db_engine)()
    click.echo('Running clear on all MarkovRelation rows')
    db_session.query(MarkovRelation).delete()
    db_session.commit()
    for channel in db_session.query(MarkovChannel).all():
        channel.last_message_id = None
        db_session.commit()
    return True

def main_runner(settings: dict, logger: RootLogger, db_engine: Engine):
    '''
    Main runner logic
    '''
    try:
        token = settings['general']['discord_token']
    except KeyError as exc:
        raise ValidationError('Unable to run bot without token') from exc

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
        rejectlist_guilds.append(str(guild))
    logger.info(f'Main :: Gathered guild reject list {rejectlist_guilds}')

    @bot.event
    async def on_ready():
        logger.info(f'Main :: Starting bot, logged in as {bot.user} (ID: {bot.user.id})')
        guilds = [guild async for guild in bot.fetch_guilds(limit=150)]
        for guild in guilds:
            if str(guild.id) in rejectlist_guilds:
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
