from discord.ext.commands import Bot, command, Context


from discord_core.exceptions import CogMissingRequiredArg

from discord_core.clients.dispatch_client_base import DispatchClientBase

from discord_gateway.cogs.common import CogHelperBase, build_commands_client
from discord_gateway.utils.otel_command import command_wrapper


class General(CogHelperBase):
    '''
    General use commands
    '''
    def __init__(self, bot: Bot, settings: dict, dispatcher: DispatchClientBase,
                 redis_manager=None):
        if not settings.get('general', {}).get('include', {}).get('default', True):
            raise CogMissingRequiredArg('Default cog not enabled')
        super().__init__(bot, settings, dispatcher, redis_manager=redis_manager)
        self.commands_client = build_commands_client(settings)

    @command(name='hello')
    @command_wrapper
    async def hello(self, ctx: Context):
        '''
        Say hello to the server
        '''
        await self.forward_command(ctx, 'hello')

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
        await self.forward_command(ctx, 'roll', input_value=input_value)

    @command(name='meta')
    @command_wrapper
    async def meta(self, ctx: Context):
        '''
        Get meta information for channel and server
        '''
        await self.forward_command(ctx, 'meta')
