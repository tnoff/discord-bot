import os
import re
import random

import discord
from discord.ext import commands


ROLL_REGEX = '^d?(?P<number>[0-9]+)$'


discord_token = os.environ['DISCORD_TOKEN']
bot = commands.Bot(command_prefix='!')

@bot.command()
async def hello(ctx):
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
    await ctx.send("%s rolled a %s" % (ctx.author.name, random_num))
    

bot.run(discord_token)
