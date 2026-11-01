from discord.ext.commands import Bot, command, Context

from discord_core.exceptions import CogMissingRequiredArg

from discord_core.clients.dispatch_client_base import DispatchClientBase

from discord_gateway.cogs.common import CogHelperBase, build_commands_client
from discord_gateway.utils.otel_command import command_wrapper


class UrbanDictionary(CogHelperBase):
    '''
    Class that looks up urban dictionary definitions
    '''

    def __init__(self, bot: Bot, settings: dict, dispatcher: DispatchClientBase,
                 redis_manager=None):
        if not settings.get('general', {}).get('include', {}).get('urban', False):
            raise CogMissingRequiredArg('Urban not enabled')
        super().__init__(bot, settings, dispatcher, redis_manager=redis_manager)
        self.commands_client = build_commands_client(settings)

    @command(name='urban')
    @command_wrapper
    async def word_lookup(self, ctx: Context, *, word: str):
        '''
        Lookup word on urban dictionary

        search: str [Required]
            The word or phrase to search in urban dictionary
        '''
        await self.forward_command(ctx, 'urban', word=word)
