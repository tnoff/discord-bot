import argparse
import os
import re
import random

import discord
from discord.ext import commands
from sqlalchemy.orm import sessionmaker

from discord_bot import functions
from discord_bot.exceptions import DiscordBotException
from discord_bot.utils import get_logger, get_database_session, read_config

HOME_PATH = os.path.expanduser("~")
CONFIG_PATH_DEFAULT = os.path.join(HOME_PATH, ".discord-bot.conf")


def parse_args():
    parser = argparse.ArgumentParser(description="Discord Bot Runner")
    parser.add_argument("--config-file", "-c", default=CONFIG_PATH_DEFAULT, help="Config file")
    parser.add_argument("--log-file", "-l",
                        help="Logging file")
    parser.add_argument("--discord-token", "-t", help="Discord token, defaults to DISCORD_TOKEN env arg")
    return parser.parse_args()


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
    db_session = get_database_session(settings['mysql_user'],
                                      settings['mysql_password'],
                                      settings['mysql_database'],
                                      settings['mysql_host'])


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
