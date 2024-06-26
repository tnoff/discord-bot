from re import search

from dappertable import DapperTable

from discord.errors import NotFound
from discord.ext import commands
from discord_bot.cogs.common import CogHelper
from discord_bot.exceptions import CogMissingRequiredArg

# Role config schema
ROLE_SECTION_SCHEMA = {
    'type': 'object',
    'minProperties': 1,
    'additionalProperties': {
        'type': 'object',
        'properties': {
            'role_list': {
                'type': 'object',
                'properties': {
                    'hide_list': {
                        'type': 'array',
                    },
                    'items': {
                        'type': 'integer'
                    },
                },
            },
            'role_manages': {
                'type': 'object',
                'properties': {
                    'reject_list': {
                        'type': 'array',
                        'items' : {
                            'type': 'integer',
                        },
                    },
                    'required_roles': {
                        'type': 'array',
                        'items': {
                            'type': 'integer',
                        },
                    },
                    'override_roles': {
                        'type': 'array',
                        'items': {
                            'type': 'integer'
                        }
                    },
                },
                'minProperties': 1,
                'additionalProperties': {
                    'type': 'object',
                    'properties': {
                        'manages': {
                            'type': 'array',
                            'minItems': 1,
                            'items': {
                                'type': 'integer',
                            },
                        },
                        'only_self': {
                            'type': 'boolean',
                            'default': False,
                        },
                    },
                    'required': [
                        'manages',
                    ]
                },
            },
        }
    }
}

class RoleAssignment(CogHelper):
    '''
    Class that can add roles in more managed fashion
    '''
    def __init__(self, bot, logger, settings, db_engine):
        super().__init__(bot, logger, settings, None, settings_prefix='role', section_schema=ROLE_SECTION_SCHEMA)
        if not self.settings.get('general', {}).get('include', {}).get('role', False):
            raise CogMissingRequiredArg('Role not enabled')
        self.settings = settings['role']

    def __clean_input(self, stringy):
        '''
        Remove “ chars from input, not treated as quotes
        '''
        return stringy.replace('“', '').replace('”', '')

    def __get_reject_list(self, ctx):
        '''
        Get server reject list
        '''
        try:
            return self.settings[ctx.guild.id]['role_manages']['reject_list']
        except KeyError:
            return []

    def __get_required_roles(self, ctx):
        '''
        Get server required role
        '''
        try:
            return self.settings[ctx.guild.id]['role_manages']['required_roles']
        except KeyError:
            return None

    def __get_override_role(self, ctx):
        '''
        Get service override role
        '''
        try:
            return self.settings[ctx.guild.id]['role_manages']['override_roles']
        except KeyError:
            return []

    def __get_role_hide(self, ctx):
        '''
        Get roles you cannot list users for
        '''
        try:
            return self.settings[ctx.guild.id]['role_list']['hide_list']
        except KeyError:
            return []

    async def get_user(self, ctx, user):
        '''
        Get user from input
        '''
        try:
            user_id = int(search(r'\d+', user).group())
        except AttributeError:
            return None
        try:
            user = await ctx.guild.fetch_member(user_id)
        except NotFound:
            return None
        return user

    def get_role(self, ctx, role):
        '''
        Get role from input
        '''
        try:
            role_id = int(search(r'\d+', role).group())
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
        input_role = role.lower().replace(' ', '').replace('"', '')
        for r in ctx.guild.roles:
            role_name = r.name.lower().replace(' ', '')
            if role_name == input_role:
                return r
        return None

    async def get_user_or_role(self, ctx, inputs):
        '''
        Get user and role list from string inputs
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

    def check_required_roles(self, ctx, user=None):
        '''
        Check user has required role before adding
        '''
        author_required_role = False
        required_roles = self.__get_required_roles(ctx)
        if not required_roles:
            return True
        for role in ctx.author.roles:
            if role.id in required_roles:
                author_required_role = True
                break
        if not user:
            return author_required_role
        for role in user.roles:
            if role.id in required_roles:
                return True
        return False

    def check_override_role(self, ctx):
        '''
        Check if user has override role
        '''
        try:
            for role in ctx.author.roles:
                if role.id in self.__get_override_role(ctx):
                    return True
        except KeyError:
            pass
        return False

    @commands.group(name='role', invoke_without_command=False)
    async def role(self, ctx):
        '''
        Role functions.
        '''
        if ctx.invoked_subcommand is None:
            await ctx.send('Invalid sub command passed...')

    @role.command(name='list')
    async def role_list(self, ctx):
        '''
        List all roles within the server
        '''
        if not self.check_required_roles(ctx):
            return await ctx.send(f'User {ctx.author.name} does not have required roles, skipping')
        headers = [
            {
                'name': 'Role Name',
                'length': 30,
            },
        ]
        table = DapperTable(headers, rows_per_message=15)
        role_names = []
        for role in ctx.guild.roles:
            if role.id in self.__get_reject_list(ctx):
                continue
            role_names.append(role.name)
        for name in sorted(role_names):
            table.add_row([f'@{name}'])
        if table.size() == 0:
            return await ctx.send('No roles found')
        for item in table.print():
            await ctx.send(f'```{item}```')

    @role.command(name='users')
    async def role_list_users(self, ctx, *, role: str):
        '''
        List all users with a specific role
        '''
        role = self.__clean_input(role)
        role_obj = self.get_role(ctx, role)
        if role_obj is None:
            return await ctx.send(f'Unable to find role {role}')
        if role_obj.id in self.__get_role_hide(ctx):
            return await ctx.send(f'Unable to list users for role {role}, in reject list')
        headers = [
            {
                'name': 'User Name',
                'length': 30,
            },
        ]
        table = DapperTable(headers, rows_per_message=15)
        for member in role_obj.members:
            user_name = member.nick or member.display_name or member.name
            table.add_row([f'@{user_name}'])
        if table.size() == 0:
            return await ctx.send('No users found for role')
        for item in table.print():
            await ctx.send(f'```{item}```')

    def get_managed_roles(self, ctx, user=None):
        '''
        Get list of roles user manages
        '''
        managed_roles = {}
        for role in ctx.author.roles:
            if role.id in self.__get_reject_list(ctx):
                continue
            try:
                manages = self.settings['role_manages'][ctx.guild.id]['role_manages'][role.id]
            except KeyError:
                continue
            manages.setdefault('only_self', False)
            if manages['only_self'] and user:
                if ctx.author != user:
                    continue
            for role_id in manages['manages']:
                manage_role = ctx.guild.get_role(role_id)
                # Cannot find role
                if manage_role is None:
                    continue
                # We want to make sure if any of the managed roles have only_self as false, we
                # set the value there to false
                try:
                    existing_value = managed_roles[manage_role]
                    if existing_value is False:
                        continue
                    managed_roles[manage_role] = manages['only_self']
                except KeyError:
                    managed_roles[manage_role] = manages['only_self']
        return managed_roles

    @role.command(name='available')
    async def role_managed(self, ctx):
        '''
        List all roles in the server that are available to your user to manage
        '''
        if not self.check_required_roles(ctx):
            return await ctx.send(f'User {ctx.author.name} does not have required roles, skipping')
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
        for role, only_self in self.get_managed_roles(ctx).items():
            row = [f'@{role.name}']
            if only_self:
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

    @role.command(name='add')
    async def role_add(self, ctx, *, inputs: str):
        '''
        Add user to role that is available to you

        inputs: Either @mention of user, @mention of role, or role name
            Role input must be last entered
        '''
        inputs = self.__clean_input(inputs)
        users, role_obj = await self.get_user_or_role(ctx, inputs)
        if not users:
            return await ctx.send('Unable to find any users in input')
        if not role_obj:
            return await ctx.send('Unable to find role')

        for user_obj in users:
            user_name = user_obj.nick or user_obj.display_name or user_obj.name
            if not self.check_override_role(ctx):
                managed_roles = list(self.get_managed_roles(ctx, user=user_obj).keys())
                if role_obj not in managed_roles:
                    await ctx.send(f'Cannot add users to role {role_obj.name}, you do not manage role. Use `!role available` to see a list of roles you manage')
                    continue
            elif role_obj.id in self.__get_reject_list(ctx):
                await ctx.send(f'Role {role_obj.name} in rejected roles list, cannot add user to role')
                continue
            if not self.check_required_roles(ctx, user=user_obj):
                await ctx.send(f'User {user_name} does not have required roles, skipping')
                continue
            if role_obj in user_obj.roles:
                await ctx.send(f'User {user_name} already has role {role_obj.name}, skipping')
                continue
            await user_obj.add_roles(role_obj)
            await ctx.send(f'Added user {user_name} to role {role_obj.name}')

    @role.command(name='remove')
    async def role_remove(self, ctx, *, inputs: str):
        '''
        Add user to role that is available to you

        inputs: Either @mention of user, @mention of role, or role name
            Role input must be last entered
        '''
        inputs = self.__clean_input(inputs)
        users, role_obj = await self.get_user_or_role(ctx, inputs)
        if not users:
            return await ctx.send('Unable to find any users in input')
        if not role_obj:
            return await ctx.send('Unable to find role')

        for user_obj in users:
            user_name = user_obj.nick or user_obj.display_name or user_obj.name
            if not self.check_override_role(ctx):
                managed_roles = list(self.get_managed_roles(ctx, user=user_obj).keys())
                if role_obj not in managed_roles:
                    await ctx.send(f'Cannot remove users to role {role_obj.name}, you do not manage role. Use `!role available` to see a list of roles you manage')
                    continue
            elif role_obj.id in self.settings[ctx.guild.id]['reject_list']:
                await ctx.send(f'Role {role_obj.name} in rejected roles list, cannot add user to role')
                continue
            if not self.check_required_roles(ctx, user=user_obj):
                await ctx.send(f'User {user_name} does not have required roles, skipping')
                continue
            if role_obj not in user_obj.roles:
                await ctx.send(f'User {user_name} does not have role {role_obj.name}, skipping')
                continue
            await user_obj.remove_roles(role_obj)
            return await ctx.send(f'Removed user {user_name} from role {role_obj.name}')
