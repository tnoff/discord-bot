import random
import re

from discord.ext import commands

from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.cogs.common import CogHelper

ROLL_REGEX = r'^((?P<rolls>\d+)[dD])?(?P<sides>\d+)'

class General(CogHelper):
    '''
    General use commands
    '''
    def __init__(self, bot, logger, settings, db_engine):
        super().__init__(bot, logger, settings, db_engine)
        if not self.settings.get('include', {}).get('default', True):
            raise CogMissingRequiredArg('Default cog not enabled')

    @commands.command(name='hello')
    async def hello(self, ctx):
        '''
        Say hello to the server
        '''
        await ctx.send(f'Waddup {ctx.author.name}')

    @commands.command(name='roll')
    async def roll(self, ctx, *, number):
        '''
        Dice rolls

        Can give standard '!roll 6', for random number between 1 and 6
        Can give 'd' prefix, '!roll d6', for random number between 1 and 6
        Can give multipliers, '!roll 2d6', to get two random numbers between 1 and 6, and add total
        '''
        matcher = re.match(ROLL_REGEX, number)
        # First check if matches regex
        if not matcher:
            message = f'Invalid input given {number}'
            return False, message
        try:
            sides = int(matcher.group('sides'))
            rolls = matcher.group('rolls')
            if rolls is None:
                rolls = 1
            else:
                rolls = int(rolls)
        except ValueError:
            message = f'Non integer value given {number}'
            return False, message

        if rolls > 20:
            return False, 'Max rolls is 20'
        if sides > 100:
            return False, 'Max number is 100'

        roll_values = []
        total = 0
        for _ in range(rolls):
            num = random.randint(1, sides)
            total += num
            roll_values.append(num)
        if rolls == 1:
            message = f'{ctx.author.name} rolled a {total}'
        else:
            roll_values_message = ' + '.join(f'{d}' for d in roll_values)
            message = f'{ctx.author.name} rolled: {roll_values_message} = {total}'

        await ctx.send(message)

    @commands.command(name='meta')
    async def meta(self, ctx):
        '''
        Get meta information for channel and server
        '''
        message = f'```Server id: {ctx.guild.id}\n'\
                  f'Channel id: {ctx.channel.id}\n'\
                  f'User id: {ctx.author.id}```'
        await ctx.send(message)
