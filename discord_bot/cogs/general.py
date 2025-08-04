from functools import partial
from random import randint
from re import match

from discord.ext.commands import Bot, command, Context
from sqlalchemy.engine.base import Engine


from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.cogs.common import CogHelper
from discord_bot.utils.otel import command_wrapper
from discord_bot.utils.common import async_retry_discord_message_command

ROLL_REGEX = r'^(?P<rolls>\d+)?([dD])?(?P<sides>\d+)'

class General(CogHelper):
    '''
    General use commands
    '''
    def __init__(self, bot: Bot, settings: dict, _db_engine: Engine):
        if not settings.get('general', {}).get('include', {}).get('default', True):
            raise CogMissingRequiredArg('Default cog not enabled')
        super().__init__(bot, settings, None)

    @command(name='hello')
    @command_wrapper
    async def hello(self, ctx: Context):
        '''
        Say hello to the server
        '''
        return await async_retry_discord_message_command(partial(ctx.send, f'Waddup {ctx.author.display_name}'))

    @command(name='roll')
    @command_wrapper
    async def roll(self, ctx: Context, *, input_value: str):
        '''
        Dice rolls

        input_value: input_value string, can be one number or other input_value

        Can give standard '!roll 6', for random number between 1 and 6
        Can give 'd' prefix, '!roll d6', for random number between 1 and 6
        Can give multipliers, '!roll 2d6', to get two random numbers between 1 and 6, and add total
        '''
        # First check if matches regex
        matcher = match(ROLL_REGEX, input_value)
        if not matcher:
            message = f'Invalid input given "{input_value}"'
            return await async_retry_discord_message_command(partial(ctx.send, message))
        try:
            sides = int(matcher.group('sides'))
            rolls = matcher.group('rolls')
            if rolls is None:
                rolls = 1
            else:
                rolls = int(rolls)
        except ValueError:
            message = f'Non integer value given {input_value}'
            return await async_retry_discord_message_command(partial(ctx.send, message))

        if rolls > 20:
            return await async_retry_discord_message_command(partial(ctx.send, f'Invalid input given, max rolls is 20 but "{rolls}" given'))
        if sides > 100:
            return await async_retry_discord_message_command(partial(ctx.send, f'Invalid input given, max sides is 100 but "{sides}" given'))


        roll_values = []
        total = 0
        for _ in range(rolls):
            num = randint(1, sides)
            total += num
            roll_values.append(num)
        if rolls == 1:
            message = f'{ctx.author.display_name} rolled a {total}'
        else:
            roll_values_message = ' + '.join(f'{d}' for d in roll_values)
            message = f'{ctx.author.display_name} rolled: {roll_values_message} = {total}'

        return await async_retry_discord_message_command(partial(ctx.send, message))

    @command(name='meta')
    @command_wrapper
    async def meta(self, ctx: Context):
        '''
        Get meta information for channel and server
        '''
        message = f'```Server id: {ctx.guild.id}\n'\
                f'Channel id: {ctx.channel.id}\n'\
                f'User id: {ctx.author.id}```'
        return await async_retry_discord_message_command(partial(ctx.send, message))
