import argparse
from configparser import NoSectionError, NoOptionError, SafeConfigParser

from discord.ext import commands

from discord_bot.exceptions import CogMissingRequiredArg, DiscordBotException
from discord_bot.cogs.error import CommandErrorHandler
from discord_bot.cogs.music import Music
from discord_bot.cogs.general import General
from discord_bot.cogs.markov import Markov
from discord_bot.cogs.role import RoleAssign
from discord_bot.cogs.twitter import Twitter
from discord_bot.utils import get_logger, get_db_session


REQUIRED_GENERAL_SETTINGS = [
    'log_file',
    'discord_token',
]

OPTIONAL_GENERAL_SETTINGS = [
    'db_type',
    'message_delete_after',
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
    parser = SafeConfigParser()
    parser.read(config_file)

    settings = {}

    sections = [item.lower() for item in parser.sections()]
    if 'general' not in sections:
        raise DiscordBotException('General config section required')

    for key in REQUIRED_GENERAL_SETTINGS:
        try:
            settings[key] = parser.get('general', key)
        except (NoSectionError, NoOptionError) as e:
            raise DiscordBotException(f'Missing required general setting "{key}"') from e

    for key in OPTIONAL_GENERAL_SETTINGS:
        try:
            settings[key] = parser.get('general', key)
        except (NoSectionError, NoOptionError):
            settings[key] = None

    for section in sections:
        if section == 'general':
            continue
        for key in parser[section]:
            settings[f'{section}_{key}'] = parser.get(section, key)
    return settings

def validate_config(settings):
    '''
    Validate some settings are set properly
    '''
    # Guess type
    for key in settings:
        try:
            settings[key] = int(settings[key])
            continue
        except (TypeError, ValueError):
            pass
        if settings[key].lower() == 'true' or settings[key].lower() == 'false':
            settings[key] = bool(settings[key])
    return settings

def main():
    '''
    Main loop
    '''
    args = parse_args()

    settings = read_config(args.config_file)
    settings = validate_config(settings)

    # Setup vars
    bot = commands.Bot(command_prefix='!')
    logger = get_logger(__name__, settings['log_file'])
    db_session = get_db_session(settings)

    # Run bot
    bot.add_cog(CommandErrorHandler(bot, logger))
    bot.add_cog(General(bot, db_session, logger, settings))
    bot.add_cog(Music(bot, db_session, logger, settings))
    bot.add_cog(RoleAssign(bot, db_session, logger, settings))
    bot.add_cog(Markov(bot, db_session, logger, settings))
    # Add twitter is settings given
    try:
        bot.add_cog(Twitter(bot, db_session, logger, settings))
    except CogMissingRequiredArg:
        logger.warning('Unable to add twitter cog')
    bot.run(settings['discord_token'])
