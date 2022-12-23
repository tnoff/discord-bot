import argparse
from asyncio import run
from copy import deepcopy
import importlib
import pathlib
from yaml import safe_load

from discord import Intents
from discord.ext import commands
from discord.ext.commands.cog import CogMeta

from discord_bot.cogs.error import CommandErrorHandler
from discord_bot.cogs.general import General
from discord_bot.exceptions import CogMissingRequiredArg, DiscordBotException
from discord_bot.utils import get_logger, get_db_engine



REQUIRED_GENERAL_SETTINGS = [
    'log_file',
    'discord_token',
]

def parse_args():
    '''
    Basic cli arg parser
    '''
    parser = argparse.ArgumentParser(description="Discord Bot Runner")
    parser.add_argument("config_file", help="Config file")
    return parser.parse_args()

def read_config(config_file):
    '''
    Get values from config file
    '''
    if config_file is None:
        return {}
    with open(config_file, 'r') as reader:
        settings = safe_load(reader)

    sections = list(settings.keys())
    if 'general' not in sections:
        raise DiscordBotException('General config section required')

    for key in REQUIRED_GENERAL_SETTINGS:
        try:
            settings['general'][key]
        except KeyError as exc:
            raise DiscordBotException(f'Missing required general setting "{key}"') from exc
    return settings

def validate_config(settings, prefix_keys=None, depth=0):
    '''
    Validate some settings are set properly
    '''
    prefix_keys = prefix_keys or []
    # Guess type
    validated_settings = {}
    for key, value in settings.items():
        if isinstance(value, dict) and depth == 0:
            pks = deepcopy(prefix_keys)
            pks.append(key)
            validated_settings.update(validate_config(value, prefix_keys=pks, depth=depth + 1))
            continue
        new_key = key
        if prefix_keys:
            new_key = f'{"_".join(k for k in prefix_keys)}_{key}'
        validated_settings[new_key] = value
    return validated_settings

def main():
    '''
    Main loop
    '''
    args = parse_args()

    settings = read_config(args.config_file)
    settings = validate_config(settings)
    # Setup vars
    intents = Intents.default()
    intents.message_content = True #pylint:disable=assigning-non-slot

    bot = commands.Bot(
        command_prefix=commands.when_mentioned_or("!"),
        description='Discord bot',
        intents=intents,
    )
    logger = get_logger(__name__, settings['general_log_file'])
    db_engine = get_db_engine(settings)

    cog_list = [
        CommandErrorHandler(bot, logger),
        General(bot, db_engine, logger, settings),
    ]
    absolute_path = pathlib.Path(__file__)
    # check plugin path for relevant py files
    plugin_path = absolute_path.parent / 'cogs' / 'plugins'
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
                except CogMissingRequiredArg:
                    logger.warning(f'Unable to add cog "{import_name}"')


    @bot.event
    async def on_ready():
        logger.info(f'Starting bot, logged in as {bot.user} (ID: {bot.user.id})')

    async def main_loop():
        async with bot:
            for cog in cog_list:
                await bot.add_cog(cog)
            await bot.start(settings['general_discord_token'])
    run(main_loop())
    