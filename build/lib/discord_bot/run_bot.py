import argparse
from asyncio import run
from datetime import datetime
from json import dumps, load
import pathlib

from discord import Intents
from discord.ext import commands
from jsonschema import ValidationError
from pyaml_env import parse_config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from discord_bot.cogs.common import CogHelper
from discord_bot.cogs.error import CommandErrorHandler
from discord_bot.cogs.delete_messages import DeleteMessages
from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.cogs.general import General
from discord_bot.cogs.markov import Markov
from discord_bot.cogs.music import Music
from discord_bot.cogs.role import RoleAssignment
from discord_bot.cogs.urban import UrbanDictionary
from discord_bot.database import BASE, AlchemyEncoder, RetryingQuery
from discord_bot.exceptions import DiscordBotException
from discord_bot.utils import get_logger, validate_config, GENERAL_SECTION_SCHEMA


DB_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'

def parse_args():
    '''
    Basic cli arg parser
    '''
    parser = argparse.ArgumentParser(description='Discord Bot Runner')
    parser.add_argument('config_file', help='Config file')

    subparser = parser.add_subparsers(dest='command', help='command')
    subparser.add_parser('run', help='Run bot')
    subparser.add_parser('db_dump', help='Dump database contents to json')

    db_load_parser = subparser.add_parser('db_load')
    db_load_parser.add_argument('json_file', help='JSON file to load from')
    return parser.parse_args()

def read_config(config_file):
    '''
    Get values from config file
    '''
    if config_file is None:
        return {}
    settings = parse_config(config_file)

    sections = list(settings.keys())
    if 'general' not in sections:
        raise DiscordBotException('General config section required')
    return settings


def db_dump(db_engine):
    '''
    Dump contents of DB to json
    '''
    if db_engine is None:
        print('Unable to dump database, no engine')
        return
    db_session = sessionmaker(bind=db_engine, query_cls=RetryingQuery)()
    tables = BASE.__subclasses__()
    table_data = {}
    for t in tables:
        rows = db_session.query(t).all()
        data = []
        for r in rows:
            data.append(r)
        table_data[t.__tablename__] = data
    print(dumps(table_data, cls=AlchemyEncoder, indent=4))

def db_load(db_engine, json_data):
    '''
    Load database attrs from json data
    '''
    db_session = sessionmaker(bind=db_engine)()
    tables = BASE.__subclasses__()
    # Get mapping of table name to table obj
    table_mapping = {}
    for t in tables:
        table_mapping[t.__tablename__] = t

    for table_name, values in json_data.items():
        table = table_mapping[table_name]
        for row in values:
            new_row = {}
            for key, value in row.items():
                try:
                    value = datetime.strptime(value, DB_DATETIME_FORMAT)
                except (ValueError, TypeError):
                    pass
                new_row[key] = value
            item = table(**new_row)
            db_session.add(item)
            db_session.commit()
    print('Finished importing json file')

def load_plugins(logger):
    '''
    Load plugins from paths
    '''
    absolute_path = pathlib.Path(__file__)
    # check plugin path for relevant py files
    plugin_path = absolute_path.parent / 'cogs' / 'plugins'
    
    cog_list = []
    for file_path in plugin_path.rglob('*.py'):
        # Ignore init file
        if file_path.name == '__init__.py':
            continue
        # Remove file suffixes
        # Ex: cogs/plugins/general.py
        proper_file = file_path.relative_to(absolute_path.parent.parent)
        proper_path = proper_file.parent / proper_file.stem
        # Make a proper import string
        # Ex: cogs.plugins.general
        import_name = str(proper_path).replace(pathlib.os.sep, '.')
        # Import, and then get "Cog" object
        logger.debug(f'Attempting to import cog from "{import_name}"')
        module = importlib.import_module(import_name)
        # Find all classes with 'Cog' in name, avoid 'CogHelper'
        for key, value in module.__dict__.items():
            if key == 'CogHelper':
                continue
            if isinstance(value, CogMeta):
                imported_cog = getattr(module, key)
                # Add cog to bot
                try:
                    cog_list.append(imported_cog(bot, db_engine, logger, settings))
                except ValidationError:
                    logger.warning(f'Unable to add cog "{import_name}" due to missing required args')
    return cog_list

def main():
    '''
    Main loop
    '''
    args = parse_args()

    settings = read_config(args.config_file)
    try:
        validate_config(settings['general'], GENERAL_SECTION_SCHEMA)
    except ValidationError as exc:
        raise DiscordBotException('Invalid config, general section does not match schema') from exc

    print('Generating intents')
    intents = Intents.default()
    try:
        intent_list = settings['general']['intents']
        print('Adding extra intents:', intent_list)
        for intent in intent_list:
            setattr(intents, intent, True)
    except KeyError:
        pass

    bot = commands.Bot(
        command_prefix=commands.when_mentioned_or("!"),
        description='Discord bot',
        intents=intents,
    )
    print('Starting logging')
    logger = get_logger(__name__, settings['general'].get('logging', {}))

    print('Starting database connection, setting up tables')
    try:
        db_engine = create_engine(settings['general']['sql_connection_statement'])
        BASE.metadata.create_all(db_engine)
        BASE.metadata.bind = db_engine
    except KeyError:
        print('Unable to find sql statement in settings, assuming no db')
        db_engine = None

    # Load default cogs
    cog_list = [
        CommandErrorHandler(bot, logger),
    ]

    for cog in CogHelper.__subclasses__():
        print('Importing cog:', cog)
        try:
            cog_list.append(cog(bot, logger, settings, db_engine))
        except CogMissingRequiredArg as e:
            logger.warning(f'Error importing cog: {str(e)}')

    cog_list += load_plugins(logger)

    print('Full cog list:', cog_list)

    @bot.event
    async def on_ready():
        logger.info(f'Starting bot, logged in as {bot.user} (ID: {bot.user.id})')

    async def main_loop():
        async with bot:
            for cog in cog_list:
                print('Adding cog:', cog)
                await bot.add_cog(cog)
            await bot.start(settings['general']['discord_token'])

    if args.command.lower() == 'run':
        run(main_loop())
    elif args.command.lower() == 'db_dump':
        db_dump(db_engine)
    elif args.command.lower() == 'db_load':
        with open(args.json_file, 'r') as o:
            json_data = load(o)
        db_load(db_engine, json_data)
    else:
        print('Invalid subcommand or no subcommand passed')
