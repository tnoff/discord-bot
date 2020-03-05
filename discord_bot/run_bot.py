import argparse
import os
import re
import random

import discord
from discord.ext import commands

from discord_bot import functions
from discord_bot.exceptions import DiscordBotException
from discord_bot.utils import get_logger

LOG_FILE_DEFAULT = '/var/log/discord-bot/discord.log'

def parse_args():
    parser = argparse.ArgumentParser(description="Discord Bot Runner")
    parser.add_argument("--log-file", "-l", default=LOG_FILE_DEFAULT,
                        help="Logging file")
    parser.add_argument("--discord-token", "-t", help="Discord token, defaults to DISCORD_TOKEN env arg")
    return parser.parse_args()

def main():
    args = parse_args()
    # Check for token
    if args.discord_token is None:
        try:
            args.discord_token = os.environ['DISCORD_TOKEN']
        except KeyError:
            raise DiscordBotException("No discord token given")

    logger = get_logger(__name__, args.log_file)
    bot = commands.Bot(command_prefix='!')


    @bot.command()
    async def hello(ctx):
        _, message = functions.hello(ctx, logger)
        await ctx.send(message)

    @bot.command()
    async def roll(ctx, number):
        _status, message = fucntions.roll(ctx, logger, number)
        await ctx.send(message)

    @bot.command()
    async def windows(ctx):
        _, message = functions.windows(ctx, logger)
        await ctx.send(message)

    bot.run(args.discord_token)
