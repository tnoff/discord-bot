import argparse
from configparser import NoSectionError, NoOptionError, SafeConfigParser

from discord.ext import commands

from discord_bot.exceptions import DiscordBotException
from discord_bot.cogs.error import CommandErrorHandler
from discord_bot.cogs.music import Music
from discord_bot.cogs.general import General
from discord_bot.cogs.markov import Markov
from discord_bot.cogs.role import RoleAssign
from discord_bot.cogs.twitter import Twitter
from discord_bot.defaults import DELETE_AFTER_DEFAULT, QUEUE_MAX_SIZE_DEFAULT
from discord_bot.defaults import MAX_SONG_LENGTH_DEFAULT
from discord_bot.utils import get_logger, get_db_session


REQUIRED_GENERAL_SETTINGS = [
    'log_file',
    'discord_token',
]

OPTIONAL_GENERAL_SETTINGS = [
    'db_type',
    'download_dir',
    'message_delete_after',
    'queue_max_size',
    'max_song_length',
    'trim_audio',
]

MYSQL_SETTINGS = [
    'user',
    'password',
    'database',
    'host',
]

SQLITE_SETTINGS = [
    'file',
]

TWITTER_SETTINGS = [
    'api_key',
    'api_key_secret',
    'access_token',
    'access_token_secret',
]

SHOULD_BE_INTEGERS = [
    'message_delete_after',
    'queue_max_size',
    'max_song_length',
]

SHOULD_BE_BOOLEAN = [
    'trim_audio'
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

    for key in MYSQL_SETTINGS:
        try:
            settings[f'mysql_{key}'] = parser.get('mysql', key)
        except (NoSectionError, NoOptionError):
            settings[f'mysql_{key}'] = None

    for key in SQLITE_SETTINGS:
        try:
            settings[f'sqlite_{key}'] = parser.get('sqlite', key)
        except (NoSectionError, NoOptionError):
            settings[f'sqlite_{key}'] = None

    for key in TWITTER_SETTINGS:
        try:
            settings[f'twitter_{key}'] = parser.get('twitter', key)
        except (NoSectionError, NoOptionError):
            settings[f'twitter_{key}'] = None
    return settings

def validate_config(settings):
    '''
    Validate some settings are set properly
    '''
    for key in SHOULD_BE_INTEGERS:
        if settings[key]:
            try:
                settings[key] = int(settings[key])
            except Exception as e:
                raise DiscordBotException(f'Invalid message after '
                                            f'type {settings[key]}, should be integer') from e

    for key in SHOULD_BE_BOOLEAN:
        if settings[key]:
            try:
                settings[key] = bool(settings[key])
            except Exception as e:
                raise DiscordBotException(f'Invalid message after '
                                            f'type {settings[key]}, should be integer') from e

    settings['message_delete_after'] = settings['message_delete_after'] or DELETE_AFTER_DEFAULT
    settings['queue_max_size'] = settings['queue_max_size'] or QUEUE_MAX_SIZE_DEFAULT
    settings['max_song_length'] = settings['max_song_length'] or MAX_SONG_LENGTH_DEFAULT
    settings['trim_audio'] = settings['trim_audio'] or False

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

    # Check if twitter is enabled
    try:
        twitter_settings = {
            'consumer_key': settings['twitter_api_key'],
            'consumer_secret': settings['twitter_api_key_secret'],
            'access_token_key': settings['twitter_access_token'],
            'access_token_secret': settings['twitter_access_token_secret'],
        }
    except KeyError:
        twitter_settings = None

    # Run bot
    bot.add_cog(CommandErrorHandler(bot, db_session, logger))
    bot.add_cog(General(bot, db_session, logger))
    bot.add_cog(Music(bot, db_session, logger, settings['download_dir'], settings['message_delete_after'],
                      settings['queue_max_size'], settings['max_song_length']))
    bot.add_cog(RoleAssign(bot, db_session, logger))
    bot.add_cog(Markov(bot, db_session, logger))
    if twitter_settings:
        bot.add_cog(Twitter(bot, db_session, logger, twitter_settings))
    bot.run(settings['discord_token'])
