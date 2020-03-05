import os
import re
import random

import discord
from discord.ext import commands

from discord_bot import functions
from discord_bot.utils import get_logger

def main():
    discord_token = os.environ['DISCORD_TOKEN']
    bot = commands.Bot(command_prefix='!')

    logger = get_logger(__name__, '/var/log/discord-bot/discord.log')

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

    discord_token = os.environ['DISCORD_TOKEN']
    bot.run(discord_token)
