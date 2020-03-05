import os
import re
import random

import discord
from discord.ext import commands

from discord_bot.utils import get_logger


ROLL_REGEX = '^d?(?P<number>[0-9]+)$'


bot = commands.Bot(command_prefix='!')

logger = get_logger(__name__, '/var/log/discord-bot/discord.log')


@bot.command()
async def hello(ctx):
    logger.debug("Sending message to %s", ctx.author.name)
    await ctx.send('Waddup %s' % ctx.author.name)

@bot.command()
async def roll(ctx, number):
    matcher = re.match(ROLL_REGEX, number)
    if not matcher:
        await ctx.send("Invalid number given")
        return
    number = matcher.group('number')
    try:
        number = int(number)
    except ValueError:
        await ctx.send("Invalid number given")
        return
    if number < 2:
        await ctx.send("Invalid number given")
    random_num = random.randint(1, number)
    logger.debug("%s rolled a %s", ctx.author.name, random_num)
    await ctx.send("%s rolled a %s" % (ctx.author.name, random_num))
    
@bot.command()
async def windows(ctx):
    logger.debug("Someone asked about windows")
    await ctx.send('Install linux coward')

def main():
    discord_token = os.environ['DISCORD_TOKEN']
    bot.run(discord_token)
