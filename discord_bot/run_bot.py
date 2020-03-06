import argparse
from configparser import NoSectionError, NoOptionError, SafeConfigParser
import os
import re
import random

import discord
from discord.ext import commands
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from discord_bot import functions
from discord_bot.database import BASE
from discord_bot.exceptions import DiscordBotException
from discord_bot.utils import get_logger

HOME_PATH = os.path.expanduser("~")
CONFIG_PATH_DEFAULT = os.path.join(HOME_PATH, ".discord-bot.conf")


def parse_args():
    parser = argparse.ArgumentParser(description="Discord Bot Runner")
    parser.add_argument("--config-file", "-c", default=CONFIG_PATH_DEFAULT, help="Config file")
    parser.add_argument("--log-file", "-l",
                        help="Logging file")
    parser.add_argument("--discord-token", "-t", help="Discord token, defaults to DISCORD_TOKEN env arg")
    return parser.parse_args()

def read_config(config_file):
    if config_file is None:
        return dict()
    parser = SafeConfigParser()
    parser.read(config_file)
    mapping = {
        'log_file' : ['general', 'log_file'],
        'discord_token' : ['general', 'discord_token'],
        'mysql_user' : ['mysql', 'user'],
        'mysql_password' : ['mysql', 'password'],
        'mysql_database' : ['mysql', 'database'],
    }
    return_data = dict()
    for key_name, args in mapping.items():
        try:
            value = parser.get(*args)
        except (NoSectionError, NoOptionError):
            value = None
        return_data[key_name] = value
    return return_data

def main():
    # First get cli args
    args = vars(parse_args())
    # Load settings
    settings = read_config(args.pop('config_file'))
    # Override settings if cli args passed
    for key, item in args.items():
        if item is not None:
            settings[key] = item
    # Check for token
    if settings['discord_token'] is None:
        raise DiscordBotException('No discord token given')

    # Setup vars
    logger = get_logger(__name__, settings['log_file'])
    bot = commands.Bot(command_prefix='!')
    # Setup database
    sql_statement = f'mysql+pymysql://{settings["mysql_user"]}:{settings["mysql_password"]}@localhost'
    sql_statement += f'/{settings["mysql_database"]}?host=localhost?port=3306'
    engine = create_engine(sql_statement, encoding='utf-8')
    BASE.metadata.create_all(engine)
    BASE.metadata.bind = engine
    db_session = sessionmaker(bind=engine)()


    # Bot commands
    @bot.command()
    async def hello(ctx):
        '''
        Say hello to the server
        '''
        _, message = functions.hello(ctx, logger)
        await ctx.send(message)

    @bot.command()
    async def roll(ctx, number):
        '''
        Get a random number between 1 and number given
        '''
        _status, message = functions.roll(ctx, logger, number)
        await ctx.send(message)

    @bot.command()
    async def windows(ctx):
        '''
        Get an inspirational note about your operating system
        '''
        _, message = functions.windows(ctx, logger)
        await ctx.send(message)

    @bot.group(pass_context=True)
    async def planner(ctx):
        '''
        Planner functions
        '''
        if ctx.invoked_subcommand is None:
            await ctx.send('Invalid sub command passed...')

    @planner.command(pass_context=True)
    async def register(ctx):
        '''
        Register yourself with planning service
        '''
        _, message = functions.planner_register(ctx, logger, db_session)
        await ctx.send(message)

    # Run bot
    bot.run(settings['discord_token'])
