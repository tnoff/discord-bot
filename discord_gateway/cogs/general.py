from discord.ext.commands import Bot, command, Context


from discord_core.exceptions import CogMissingRequiredArg

from discord_core.clients.dispatch_client_base import DispatchClientBase

from discord_gateway.cogs.common import CogHelperBase, envelope_from_ctx
from discord_gateway.command_logic.general import GeneralCommands
from discord_gateway.utils.otel_command import command_wrapper


class General(CogHelperBase):
    '''
    General use commands
    '''
    def __init__(self, bot: Bot, settings: dict, dispatcher: DispatchClientBase,
                 _stores: object = None, redis_manager=None):
        if not settings.get('general', {}).get('include', {}).get('default', True):
            raise CogMissingRequiredArg('Default cog not enabled')
        super().__init__(bot, settings, dispatcher, None, redis_manager=redis_manager)
        self.logic = GeneralCommands(self.dispatch_message)

    @command(name='hello')
    @command_wrapper
    async def hello(self, ctx: Context):
        '''
        Say hello to the server
        '''
        return await self.logic.hello(envelope_from_ctx(ctx))

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
        return await self.logic.roll(envelope_from_ctx(ctx), input_value)

    @command(name='meta')
    @command_wrapper
    async def meta(self, ctx: Context):
        '''
        Get meta information for channel and server
        '''
        return await self.logic.meta(envelope_from_ctx(ctx))
