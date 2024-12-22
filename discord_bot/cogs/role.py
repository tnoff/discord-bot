from logging import RootLogger
from re import search
from typing import List

from dappertable import DapperTable

from discord import Member, Role
from discord.errors import NotFound
from discord.ext.commands import Bot, Context, group
from sqlalchemy.engine.base import Engine

from discord_bot.cogs.common import CogHelper
from discord_bot.exceptions import CogMissingRequiredArg

# Role config schema
ROLE_SECTION_SCHEMA = {
    'type': 'object',
    'minProperties': 1,
    'additionalProperties': {
        'type': 'object',
        'properties': {
            'rejected_roles_list': {
                'type': 'array',
                'items' : {
                    'type': 'string',
                },
            },
            'required_roles_list': {
                'type': 'array',
                'items': {
                    'type': 'string',
                },
            },
            'admin_override_role_list': {
                'type': 'array',
                'items': {
                    'type': 'string'
                }
            },
            'self_service_role_list': {
                'type': 'array',
                'items': {
                    'type': 'string'
                }
            },
            'additionalProperties': {
                'type': 'object',
                'properties': {
                    'manages_roles': {
                        'type': 'array',
                        'minItems': 1,
                        'items': {
                            'type': 'string',
                        },
                    },
                },
            },
        }
    }
}

class RoleAssignment(CogHelper):
    '''
    Class that can add roles in more managed fashion
    '''
    def __init__(self, bot: Bot, logger: RootLogger, settings: dict, _db_engine: Engine):
        if not settings.get('general', {}).get('include', {}).get('role', False):
            raise CogMissingRequiredArg('Role not enabled')
        if not bot.intents.members:
            raise CogMissingRequiredArg('"members" intents required to run role commands')
        super().__init__(bot, logger, settings, None, settings_prefix='role', section_schema=ROLE_SECTION_SCHEMA)
        self.settings = settings['role']

    def clean_input(self, stringy: str) -> str:
        '''
        Remove “ chars from input, not treated as quotes
        '''
        return stringy.replace('“', '').replace('”', '')

    def get_rejected_roles_list(self, ctx: Context) -> List[str]:
        '''
        Get server reject list
        '''
        try:
            return self.settings[ctx.guild.id]['rejected_roles_list']
        except KeyError:
            return []

    def get_required_roles(self, ctx: Context) -> List[str]:
        '''
        Get server required role
        '''
        try:
            return self.settings[ctx.guild.id]['required_roles_list']
        except KeyError:
            return []

    def get_override_role(self, ctx: Context) -> List[str]:
        '''
        Get service override role
        '''
        try:
            return self.settings[ctx.guild.id]['admin_override_role_list']
        except KeyError:
            return []

    def get_self_service_roles(self, ctx: Context) -> List[str]:
        '''
        Get service role listing
        '''
        try:
            return self.settings[ctx.guild.id]['self_service_role_list']
        except KeyError:
            return []

    async def get_user(self, ctx: Context, user_input: str) -> Member:
        '''
        Get user from input

        ctx : Original discord context
        user_input : User ID input, usually from an @mention
        '''
        try:
            user_id = search(r'\d+', user_input).group()
        except AttributeError:
            return None
        try:
            user = await ctx.guild.fetch_member(user_id)
        except NotFound:
            return None
        return user

    def get_role(self, ctx: Context, role_input: str) -> Role:
        '''
        Get role from input

        ctx: Original Discord Context
        role_input : Either role id or role name
        '''
        try:
            role_id = search(r'\d+', role_input).group()
        except AttributeError:
            role_id = None
        # Get role first from id if present
        if role_id:
            try:
                role = ctx.guild.get_role(role_id)
                return role
            except NotFound:
                return None
        # If not try to find it by the name
        input_role = role_input.lower()
        for r in ctx.guild.roles:
            role_name = r.name.lower()
            if role_name == input_role:
                return r
        return None

    async def get_user_or_role(self, ctx: Context, inputs: str) -> tuple[List[Member], Role]:
        '''
        Get user and role list from string inputs

        ctx: Original Discord Context
        inputs: List of strings and role, role should be at the end
        '''
        # Assume role is last input, and we see users until then
        inputs = inputs.split(' ')
        users = []
        for (count, i) in enumerate(inputs):
            user = await self.get_user(ctx, i)
            if not user:
                break
            users.append(user)
        role_string = ' '.join(i for i in inputs[count::]) #pylint: disable=undefined-loop-variable
        role_obj = self.get_role(ctx, role_string)
        return users, role_obj

    def check_required_roles(self, ctx: Context, user: Member = None) -> bool:
        '''
        Check user has required role before adding

        ctx: Original Discord Context
        user: Also check additional member has required perms
        '''
        author_required_role = False
        required_roles = self.get_required_roles(ctx)
        if not required_roles:
            return True
        for role in ctx.author.roles:
            if role.id in required_roles:
                author_required_role = True
                break
        if not user:
            return author_required_role
        # If author doesnt have required role, return False outright
        if not author_required_role:
            return False
        for role in user.roles:
            if role.id in required_roles:
                return True
        return False

    def check_override_role(self, ctx: Context) -> bool:
        '''
        Check if user has override role

        ctx: Original Discord Context
        '''
        for role in ctx.author.roles:
            if role.id in self.get_override_role(ctx):
                return True
        return False

    @group(name='role', invoke_without_command=False)
    async def role(self, ctx: Context):
        '''
        Role functions.
        '''
        if ctx.invoked_subcommand is None:
            await ctx.send('Invalid sub command passed...')

    @role.command(name='list')
    async def role_list(self, ctx: Context):
        '''
        List all roles within the server
        '''
        if not self.check_required_roles(ctx):
            return await ctx.send(f'User "{ctx.author.display_name}" does not have required roles, skipping')
        headers = [
            {
                'name': 'Role Name',
                'length': 30,
            },
        ]
        table = DapperTable(headers, rows_per_message=15)
        for role in ctx.guild.roles:
            if role.id in self.get_rejected_roles_list(ctx):
                continue
            table.add_row([f'@{role.name}'])
        if table.size() == 0:
            return await ctx.send('No roles found')
        for item in table.print():
            await ctx.send(f'```{item}```')
        return True

    @role.command(name='users')
    async def role_list_users(self, ctx: Context, *, role_input: str):
        '''
        List all users with a specific role
        '''
        if not self.check_required_roles(ctx):
            return await ctx.send(f'User "{ctx.author.display_name}" does not have required roles, skipping')
        role = self.clean_input(role_input)
        role_obj = self.get_role(ctx, role)
        if role_obj is None:
            return await ctx.send(f'Unable to find role "{role}"')
        # Check for rejected list
        if role_obj.id in self.get_rejected_roles_list(ctx):
            return await ctx.send(f'Unable to list users for role "{role}", in reject list')

        headers = [
            {
                'name': 'User Name',
                'length': 30,
            },
        ]
        table = DapperTable(headers, rows_per_message=15)
        for member in role_obj.members:
            table.add_row([f'@{member.display_name}'])
        if table.size() == 0:
            return await ctx.send(f'No users found for role "{role}"')
        for item in table.print():
            await ctx.send(f'```{item}```')
        return True

    def get_managed_roles(self, ctx: Context, exclude_self_service: bool = False) -> dict:
        '''
        Get list of roles user roles_managed

        ctx: Original Discord Context
        exclude_self_service: Exclude self service from check
        Returns
        {
            <role-id> : <self-service-flag>
        }

        '''
        managed_roles = {}
        role_cache = {}
        rejected_role_list = self.get_rejected_roles_list(ctx)

        if not exclude_self_service:
            for self_service_id in self.get_self_service_roles(ctx):
                if self_service_id in rejected_role_list:
                    continue
                try:
                    role_obj = role_cache[self_service_id]
                except KeyError:
                    try:
                        role_obj = ctx.guild.get_role(self_service_id)
                    except NotFound:
                        role_obj = None
                    role_cache[self_service_id] = role_obj
                # Skip if role doesn't exist
                if not role_obj:
                    continue
                managed_roles[role_obj] = True

        for role in ctx.author.roles:
            # For every role, check what that role manages
            try:
                role_rules = self.settings[ctx.guild.id][role.id]
            except KeyError:
                continue
            for role_id in role_rules['manages_roles']:
                if role_id in rejected_role_list:
                    continue
                try:
                    role_obj = role_cache[role_id]
                except KeyError:
                    try:
                        role_obj = ctx.guild.get_role(role_id)
                    except NotFound:
                        role_obj = None
                    role_cache[role_id] = role_obj
                # Skip role if it doesn't exist
                if not role_obj:
                    continue
                # If role already declared, assume we can skip
                if role_obj in managed_roles:
                    continue
                managed_roles[role_obj] = False
        return managed_roles

    @role.command(name='available')
    async def role_managed(self, ctx: Context):
        '''
        List all roles in the server that are available to your user to manage
        '''
        if not self.check_required_roles(ctx):
            return await ctx.send(f'User "{ctx.author.display_name}" does not have required roles, skipping')
        headers = [
            {
                'name': 'Role Name',
                'length': 30,
            },
            {
                'name': 'Control',
                'length': 10,
            },
        ]
        table = DapperTable(headers, rows_per_message=15)
        rows = []
        for role, is_self_service in self.get_managed_roles(ctx).items():
            row = [f'@{role.name}']
            if is_self_service:
                row += ['Self-Serve']
            else:
                row += ['Full']
            rows.append(row)
        # Sort output
        rows = sorted(rows)
        for row in rows:
            table.add_row(row)
        if table.size() == 0:
            return await ctx.send('No roles found')
        for item in table.print():
            await ctx.send(f'```{item}```')
        return True

    def check_only_self_service(self, ctx: Context, users: List[Member]) -> bool:
        '''
        Check if this is a self service call

        ctx: Original Discord Context
        users: List of members from role call
        '''
        if len(users) > 1:
            return False

        return users[0].id == ctx.author.id

    @role.command(name='add')
    async def role_add(self, ctx: Context, *, inputs: str):
        '''
        Add user to role that is available to you

        inputs: Either @mention of user, @mention of role, or role name
                Role input must be last entered
        '''
        if not self.check_required_roles(ctx):
            return await ctx.send(f'User "{ctx.author.display_name}" does not have required roles, skipping')

        inputs = self.clean_input(inputs)
        users, role_obj = await self.get_user_or_role(ctx, inputs)
        if not users or not role_obj:
            return await ctx.send('Unable to find users or role from input')

        if not self.check_override_role(ctx):
            if role_obj not in self.get_managed_roles(ctx, exclude_self_service=not self.check_only_self_service(ctx, users)):
                return await ctx.send(f'Cannot add users to role "{role_obj.name}", you do not manage role. Use `!role available` to see a list of roles you manage')

        for user_obj in users:
            if not self.check_required_roles(ctx, user=user_obj) and not self.check_override_role(ctx):
                await ctx.send(f'User "{user_obj.display_name}" does not have required roles, skipping')
                continue
            if role_obj in user_obj.roles:
                await ctx.send(f'User "{user_obj.display_name}" already has role "{role_obj.name}", skipping')
                continue
            await user_obj.add_roles(role_obj)
            await ctx.send(f'Added user "{user_obj.display_name}" to role "{role_obj.name}"')
        return True

    @role.command(name='remove')
    async def role_remove(self, ctx: Context, *, inputs: str):
        '''
        Add user to role that is available to you

        inputs: Either @mention of user, @mention of role, or role name
            Role input must be last entered
        '''
        if not self.check_required_roles(ctx):
            return await ctx.send(f'User "{ctx.author.display_name}" does not have required roles, skipping')

        inputs = self.clean_input(inputs)
        users, role_obj = await self.get_user_or_role(ctx, inputs)
        if not users or not role_obj:
            return await ctx.send('Unable to find users or role from input')

        if not self.check_override_role(ctx):
            if role_obj not in self.get_managed_roles(ctx, exclude_self_service=not self.check_only_self_service(ctx, users)):
                return await ctx.send(f'Cannot remove users from role "{role_obj.name}", you do not manage role. Use `!role available` to see a list of roles you manage')

        for user_obj in users:
            if role_obj not in user_obj.roles:
                await ctx.send(f'User "{user_obj.display_name}" does not have role "{role_obj.name}", skipping')
                continue
            await user_obj.remove_roles(role_obj)
            await ctx.send(f'Removed user "{user_obj.display_name}" from role "{role_obj.name}"')
