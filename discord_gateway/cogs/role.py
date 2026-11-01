from discord.ext.commands import Bot, Context, group

from discord_core.exceptions import CogMissingRequiredArg

from discord_core.clients.dispatch_client_base import DispatchClientBase

from discord_gateway.cogs.common import CogHelperBase, build_commands_client
from discord_gateway.utils.otel_command import command_wrapper


class RoleAssignment(CogHelperBase):
    '''
    The ``!role`` commands. They run in the commands pod, which reads the role config and asks Discord
    through the dispatcher; this cog parses the command and forwards it (docs/role.md).
    '''
    def __init__(self, bot: Bot, settings: dict, dispatcher: DispatchClientBase,
                 _stores: object = None, redis_manager=None):
        if not settings.get('general', {}).get('include', {}).get('role', False):
            raise CogMissingRequiredArg('Role not enabled')
        super().__init__(bot, settings, dispatcher, None, redis_manager=redis_manager)
        self.commands_client = build_commands_client(settings)

    @group(name='role', invoke_without_command=False)
    async def role(self, ctx: Context):
        '''
        Role functions.
        '''
        if ctx.invoked_subcommand is None:
            await self.dispatch_message(ctx.guild.id, ctx.channel.id,'Invalid sub command passed...')

    @role.command(name='list')
    @command_wrapper
    async def role_list(self, ctx: Context):
        '''
        List all roles within the server
        '''
        await self.forward_command(ctx, 'role.list')

    @role.command(name='users')
    @command_wrapper
    async def role_list_users(self, ctx: Context, *, role_input: str):
        '''
        List all users with a specific role
        '''
        await self.forward_command(ctx, 'role.users', role_input=role_input)

    @role.command(name='available')
    @command_wrapper
    async def role_managed(self, ctx: Context):
        '''
        List all roles in the server that are available to your user to manage
        '''
        await self.forward_command(ctx, 'role.available')

    @role.command(name='add')
    @command_wrapper
    async def role_add(self, ctx: Context, *, inputs: str):
        '''
        Add user to role that is available to you

        inputs: Either @mention of user, @mention of role, or role name
                Role input must be last entered
        '''
        await self.forward_command(ctx, 'role.add', inputs=inputs)

    @role.command(name='remove')
    @command_wrapper
    async def role_remove(self, ctx: Context, *, inputs: str):
        '''
        Add user to role that is available to you

        inputs: Either @mention of user, @mention of role, or role name
            Role input must be last entered
        '''
        await self.forward_command(ctx, 'role.remove', inputs=inputs)
