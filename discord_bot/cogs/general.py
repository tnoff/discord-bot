import random
import re

from discord.ext import commands

from discord_bot.cogs.common import CogHelper

ROLL_REGEX = r'^((?P<rolls>\d+)[dD])?(?P<sides>\d+) *(?P<operator>[+-])? *(?P<modifier>\d+)?'

class General(CogHelper):
    '''
    General use commands
    '''

    @commands.command(name='hello')
    async def hello(self, ctx):
        '''
        Say hello to the server
        '''
        await ctx.send(f'Waddup {ctx.author.name}')

    @commands.command(name='roll')
    async def roll(self, ctx, *, number):
        '''
        Get a random number between 1 and number given
        '''
        matcher = re.match(ROLL_REGEX, number)
        # First check if matches regex
        if not matcher:
            message = f'Invalid number given {number}'
            return False, message
        try:
            sides = int(matcher.group('sides'))
            rolls = matcher.group('rolls')
            modifier = matcher.group('modifier')
            if rolls is None:
                rolls = 1
            else:
                rolls = int(rolls)
            if modifier is None:
                modifier = 0
            else:
                modifier = int(modifier)
        except ValueError:
            message = 'Non integer value given'
            return False, message

        total = 0
        for _ in range(rolls):
            total += random.randint(1, sides)
        if modifier:
            if matcher.group('operator') == '-':
                total = total - modifier
            elif matcher.group('operator') == '+':
                total = total + modifier

        message = f'{ctx.author.name} rolled a {total}'

        await ctx.send(message)

    @commands.command(name='windows')
    async def windows(self, ctx):
        '''
        Get an inspirational note about your operating system
        '''
        await ctx.send('Install linux coward')

    @commands.command(name='meta')
    async def meta(self, ctx):
        '''
        Get meta information for channel and server
        '''
        message = f'```Server id: {ctx.guild.id}\n'\
                  f'Channel id: {ctx.channel.id}\n'\
                  f'User id: {ctx.author.id}```'
        await ctx.send(message)
