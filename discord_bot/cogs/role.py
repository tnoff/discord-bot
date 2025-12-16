from re import search
from typing import List

from dappertable import DapperTable, DapperTableHeaderOptions, DapperTableHeader, PaginationLength

from discord import Member, Role
from discord.errors import NotFound
from discord.ext.commands import Bot, Context, group
from pydantic import BaseModel, Field
from sqlalchemy.engine.base import Engine

from discord_bot.common import DISCORD_MAX_MESSAGE_LENGTH
from discord_bot.cogs.common import CogHelper
from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.utils.otel import command_wrapper

# Pydantic config models
class RoleManagementConfig(BaseModel):
    '''Role management configuration'''
    manages_roles: list[int] = Field(min_length=1)

class RoleServerConfig(BaseModel):
    '''Per-server role configuration'''
    rejected_roles_list: list[int] = Field(default_factory=list)
    required_roles_list: list[int] = Field(default_factory=list)
    admin_override_role_list: list[int] = Field(default_factory=list)
    self_service_role_list: list[int] = Field(default_factory=list)

class RoleConfig(BaseModel):
    '''Top-level role cog configuration - validates server and role configs'''
    model_config = {"extra": "allow"}

    @classmethod
    def model_validate(cls, obj):  # pylint: disable=arguments-differ
        '''Validate config and convert keys to integers'''
        if not isinstance(obj, dict):
            return super().model_validate(obj)

        # Step 1: Convert integer keys to strings for Pydantic validation
        str_keyed = {}
        for server_id, server_config in obj.items():
            str_key = str(server_id)

            if isinstance(server_config, dict):
                # Validate nested structure using the appropriate model
                validated_config = {}
                for k, v in server_config.items():
                    str_k = str(k)

                    # If this is a dict with 'manages_roles', validate as RoleManagementConfig
                    if isinstance(v, dict) and 'manages_roles' in v:
                        validated_config[str_k] = RoleManagementConfig(**v).model_dump()
                    # Otherwise validate as a field value
                    elif str_k in ['rejected_roles_list', 'required_roles_list',
                                   'admin_override_role_list', 'self_service_role_list']:
                        validated_config[str_k] = v
                    # If it's a nested dict (role ID -> management config)
                    elif isinstance(v, dict):
                        validated_config[str_k] = RoleManagementConfig(**v).model_dump()
                    else:
                        validated_config[str_k] = v

                # Validate the full server config structure
                try:
                    RoleServerConfig(**{k: v for k, v in validated_config.items()
                                       if k in ['rejected_roles_list', 'required_roles_list',
                                               'admin_override_role_list', 'self_service_role_list']})
                except Exception:
                    pass  # Server config might have additional role management entries

                str_keyed[str_key] = validated_config
            else:
                str_keyed[str_key] = server_config

        # Step 2: Create instance with string keys (Pydantic requirement)
        instance = super().model_validate(str_keyed)

        # Step 3: Convert back to integer keys and store
        int_keyed_config = {}
        for k, v in instance.__pydantic_extra__.items():
            # Convert server ID key to int
            int_key = int(k) if k.isdigit() else k

            # Convert nested role ID keys to int
            if isinstance(v, dict):
                int_v = {}
                for vk, vv in v.items():
                    int_vk = int(vk) if isinstance(vk, str) and vk.isdigit() else vk
                    int_v[int_vk] = vv
                int_keyed_config[int_key] = int_v
            else:
                int_keyed_config[int_key] = v

        # Store in private attribute
        object.__setattr__(instance, '_int_keyed_config', int_keyed_config)

        return instance

    def model_dump(self, **kwargs):  # pylint: disable=unused-argument
        '''Return the integer-keyed config'''
        return object.__getattribute__(self, '_int_keyed_config')

class RoleAssignment(CogHelper):
    '''
    Class that can add roles in more managed fashion
    '''
    def __init__(self, bot: Bot, settings: dict, _db_engine: Engine):
        if not settings.get('general', {}).get('include', {}).get('role', False):
            raise CogMissingRequiredArg('Role not enabled')
        if not bot.intents.members:
            raise CogMissingRequiredArg('"members" intents required to run role commands')
        super().__init__(bot, settings, None, settings_prefix='role', config_model=RoleConfig)
        # Use validated config with integer keys
        self.settings = self.config.model_dump()

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
        # Convert integer input to string for regex
        user_input = str(user_input)
        try:
            user_id = int(search(r'\d+', user_input).group())
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
        # Convert integer input to string for regex
        role_input = str(role_input)
        try:
            role_id = int(search(r'\d+', role_input).group())
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
    @command_wrapper
    async def role_list(self, ctx: Context):
        '''
        List all roles within the server
        '''
        if not self.check_required_roles(ctx):
            return await ctx.send(f'User "{ctx.author.display_name}" does not have required roles, skipping')
        headers = [
            DapperTableHeader('Role Name', 30)
        ]
        table = DapperTable(header_options=DapperTableHeaderOptions(headers), pagination_options=PaginationLength(DISCORD_MAX_MESSAGE_LENGTH))
        for role in ctx.guild.roles:
            if role.id in self.get_rejected_roles_list(ctx):
                continue
            table.add_row([f'@{role.name}'])
        if table.size == 0:
            return await ctx.send('No roles found')
        for item in table.print():
            await ctx.send(f'```{item}```')
        return True

    @role.command(name='users')
    @command_wrapper
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

        headers = [
            DapperTableHeader('User Name', 30)
        ]
        table = DapperTable(header_options=DapperTableHeaderOptions(headers), pagination_options=PaginationLength(DISCORD_MAX_MESSAGE_LENGTH))
        for member in role_obj.members:
            table.add_row([f'@{member.display_name}'])
        if table.size == 0:
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
    @command_wrapper
    async def role_managed(self, ctx: Context):
        '''
        List all roles in the server that are available to your user to manage
        '''
        if not self.check_required_roles(ctx):
            return await ctx.send(f'User "{ctx.author.display_name}" does not have required roles, skipping')
        headers = [
            DapperTableHeader('Role Name', 30),
            DapperTableHeader('Control', 10)
        ]
        table = DapperTable(header_options=DapperTableHeaderOptions(headers), pagination_options=PaginationLength(DISCORD_MAX_MESSAGE_LENGTH))
        rows = []
        # Print managed rows first, save self servic for later
        # Make sure we order them by name for ease
        managed = []
        self_service = []
        for role, is_self_service in self.get_managed_roles(ctx).items():
            if is_self_service:
                self_service.append(role.name)
                continue
            managed.append(role.name)
        managed = sorted(managed)
        self_service = sorted(self_service)
        for item in managed:
            rows.append([
                f'@{item}',
                'Full',
            ])
        for item in self_service:
            rows.append([
                f'@{item}',
                'Self-Serve'
            ])

        for row in rows:
            table.add_row(row)
        if table.size == 0:
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
    @command_wrapper
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
    @command_wrapper
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
