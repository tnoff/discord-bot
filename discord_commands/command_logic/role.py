from re import search

from dappertable import DapperTable, Columns, Column, PaginationLength

from discord_core.common import DISCORD_MAX_MESSAGE_LENGTH
from discord_core.types.command_envelope import CommandEnvelope

from discord_commands.command_logic.reply import Reply
from discord_commands.command_logic.role_gateway import MemberInfo, RoleGateway, RoleInfo


class RoleCommands:
    '''
    The ``!role`` subcommands. Discord-free: see ``discord_core.types.command_envelope``.

    ``settings`` is the validated role config keyed by integer guild id.
    '''

    def __init__(self, reply: Reply, role_gateway: RoleGateway, settings: dict):
        self.reply = reply
        self.role_gateway = role_gateway
        self.settings = settings

    def clean_input(self, stringy: str) -> str:
        '''
        Remove “ chars from input, not treated as quotes
        '''
        return stringy.replace('“', '').replace('”', '')

    def get_rejected_roles_list(self, guild_id: int) -> list[int]:
        '''Server reject list.'''
        try:
            return self.settings[guild_id]['rejected_roles_list']
        except KeyError:
            return []

    def get_required_roles(self, guild_id: int) -> list[int]:
        '''Server required roles.'''
        try:
            return self.settings[guild_id]['required_roles_list']
        except KeyError:
            return []

    def get_override_role(self, guild_id: int) -> list[int]:
        '''Server admin override roles.'''
        try:
            return self.settings[guild_id]['admin_override_role_list']
        except KeyError:
            return []

    def get_self_service_roles(self, guild_id: int) -> list[int]:
        '''Server self service roles.'''
        try:
            return self.settings[guild_id]['self_service_role_list']
        except KeyError:
            return []

    async def get_user(self, env: CommandEnvelope, user_input: str) -> MemberInfo | None:
        '''
        Get user from input

        user_input : User ID input, usually from an @mention
        '''
        # Convert integer input to string for regex
        user_input = str(user_input)
        try:
            user_id = int(search(r'\d+', user_input).group())
        except AttributeError:
            return None
        return await self.role_gateway.get_member(env.guild_id, user_id)

    async def get_role(self, env: CommandEnvelope, role_input: str) -> RoleInfo | None:
        '''
        Get role from input

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
            return await self.role_gateway.get_role(env.guild_id, role_id)
        # If not try to find it by the name
        input_role = role_input.lower()
        for role in await self.role_gateway.list_roles(env.guild_id):
            if role.name.lower() == input_role:
                return role
        return None

    async def get_user_or_role(self, env: CommandEnvelope, inputs: str) -> tuple[list[MemberInfo], RoleInfo | None]:
        '''
        Get user and role list from string inputs

        inputs: List of strings and role, role should be at the end
        '''
        # Assume role is last input, and we see users until then
        inputs = inputs.split(' ')
        users = []
        for (count, i) in enumerate(inputs):
            user = await self.get_user(env, i)
            if not user:
                break
            users.append(user)
        role_string = ' '.join(i for i in inputs[count::]) #pylint: disable=undefined-loop-variable
        role_obj = await self.get_role(env, role_string)
        return users, role_obj

    def check_required_roles(self, env: CommandEnvelope, user: MemberInfo = None) -> bool:
        '''
        Check user has required role before adding

        user: Also check additional member has required perms
        '''
        author_required_role = False
        required_roles = self.get_required_roles(env.guild_id)
        if not required_roles:
            return True
        for role_id in env.author_role_ids:
            if role_id in required_roles:
                author_required_role = True
                break
        if not user:
            return author_required_role
        # If author doesnt have required role, return False outright
        if not author_required_role:
            return False
        for role_id in user.role_ids:
            if role_id in required_roles:
                return True
        return False

    def check_override_role(self, env: CommandEnvelope) -> bool:
        '''Check if user has override role'''
        for role_id in env.author_role_ids:
            if role_id in self.get_override_role(env.guild_id):
                return True
        return False

    def _not_allowed(self, env: CommandEnvelope):
        return self.reply(env.guild_id, env.channel_id, f'User "{env.author_display_name}" does not have required roles, skipping')

    async def role_list(self, env: CommandEnvelope):
        '''List all roles within the server'''
        if not self.check_required_roles(env):
            return await self._not_allowed(env)
        headers = [
            Column('Role Name', 30)
        ]
        table = DapperTable(columns=Columns(headers), pagination_options=PaginationLength(DISCORD_MAX_MESSAGE_LENGTH),
                            enclosure_start='```', enclosure_end='```')
        for role in await self.role_gateway.list_roles(env.guild_id):
            if role.id in self.get_rejected_roles_list(env.guild_id):
                continue
            table.add_row([f'@{role.name}'])
        if table.size == 0:
            return await self.reply(env.guild_id, env.channel_id, 'No roles found')
        for item in table.render():
            await self.reply(env.guild_id, env.channel_id, f'{item}')
        return True

    async def role_list_users(self, env: CommandEnvelope, role_input: str):
        '''List all users with a specific role'''
        if not self.check_required_roles(env):
            return await self._not_allowed(env)
        role = self.clean_input(role_input)
        role_obj = await self.get_role(env, role)
        if role_obj is None:
            return await self.reply(env.guild_id, env.channel_id, f'Unable to find role "{role}"')

        headers = [
            Column('User Name', 30)
        ]
        table = DapperTable(columns=Columns(headers), pagination_options=PaginationLength(DISCORD_MAX_MESSAGE_LENGTH),
                            enclosure_start='```', enclosure_end='```')
        for member in await self.role_gateway.role_members(env.guild_id, role_obj.id):
            table.add_row([f'@{member.display_name}'])
        if table.size == 0:
            return await self.reply(env.guild_id, env.channel_id, f'No users found for role "{role}"')
        for item in table.render():
            await self.reply(env.guild_id, env.channel_id, f'{item}')
        return True

    async def get_managed_roles(self, env: CommandEnvelope, exclude_self_service: bool = False) -> dict[RoleInfo, bool]:
        '''
        Get the roles the author manages

        exclude_self_service: Exclude self service from check
        Returns
        {
            <role>: <self-service-flag>
        }
        '''
        managed_roles = {}
        role_cache = {}
        rejected_role_list = self.get_rejected_roles_list(env.guild_id)

        async def lookup(role_id):
            if role_id not in role_cache:
                role_cache[role_id] = await self.role_gateway.get_role(env.guild_id, role_id)
            return role_cache[role_id]

        if not exclude_self_service:
            for self_service_id in self.get_self_service_roles(env.guild_id):
                if self_service_id in rejected_role_list:
                    continue
                role_obj = await lookup(self_service_id)
                # Skip if role doesn't exist
                if not role_obj:
                    continue
                managed_roles[role_obj] = True

        for author_role_id in env.author_role_ids:
            # For every role, check what that role manages
            try:
                role_rules = self.settings[env.guild_id][author_role_id]
            except KeyError:
                continue
            for role_id in role_rules['manages_roles']:
                if role_id in rejected_role_list:
                    continue
                role_obj = await lookup(role_id)
                # Skip role if it doesn't exist
                if not role_obj:
                    continue
                # If role already declared, assume we can skip
                if role_obj in managed_roles:
                    continue
                managed_roles[role_obj] = False
        return managed_roles

    async def role_managed(self, env: CommandEnvelope):
        '''List all roles in the server that are available to the author to manage'''
        if not self.check_required_roles(env):
            return await self._not_allowed(env)
        headers = [
            Column('Role Name', 30),
            Column('Control', 10)
        ]
        table = DapperTable(columns=Columns(headers), pagination_options=PaginationLength(DISCORD_MAX_MESSAGE_LENGTH),
                            enclosure_start='```', enclosure_end='```')
        rows = []
        # Print managed rows first, save self servic for later
        # Make sure we order them by name for ease
        managed = []
        self_service = []
        for role, is_self_service in (await self.get_managed_roles(env)).items():
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
            return await self.reply(env.guild_id, env.channel_id, 'No roles found')
        for item in table.render():
            await self.reply(env.guild_id, env.channel_id, f'{item}')
        return True

    def check_only_self_service(self, env: CommandEnvelope, users: list[MemberInfo]) -> bool:
        '''
        Check if this is a self service call

        users: List of members from role call
        '''
        if len(users) > 1:
            return False

        return users[0].id == env.author_id

    async def role_add(self, env: CommandEnvelope, inputs: str):
        '''Add users to a role that is available to the author.'''
        if not self.check_required_roles(env):
            return await self._not_allowed(env)

        inputs = self.clean_input(inputs)
        users, role_obj = await self.get_user_or_role(env, inputs)
        if not users or not role_obj:
            return await self.reply(env.guild_id, env.channel_id, 'Unable to find users or role from input')

        if not self.check_override_role(env):
            managed = await self.get_managed_roles(env, exclude_self_service=not self.check_only_self_service(env, users))
            if role_obj not in managed:
                return await self.reply(env.guild_id, env.channel_id, f'Cannot add users to role "{role_obj.name}", you do not manage role. Use `!role available` to see a list of roles you manage')

        for user_obj in users:
            if not self.check_required_roles(env, user=user_obj) and not self.check_override_role(env):
                await self.reply(env.guild_id, env.channel_id, f'User "{user_obj.display_name}" does not have required roles, skipping')
                continue
            if role_obj.id in user_obj.role_ids:
                await self.reply(env.guild_id, env.channel_id, f'User "{user_obj.display_name}" already has role "{role_obj.name}", skipping')
                continue
            await self.role_gateway.add_role(env.guild_id, user_obj.id, role_obj.id)
            await self.reply(env.guild_id, env.channel_id, f'Added user "{user_obj.display_name}" to role "{role_obj.name}"')
        return True

    async def role_remove(self, env: CommandEnvelope, inputs: str):
        '''Remove users from a role that is available to the author.'''
        if not self.check_required_roles(env):
            return await self._not_allowed(env)

        inputs = self.clean_input(inputs)
        users, role_obj = await self.get_user_or_role(env, inputs)
        if not users or not role_obj:
            return await self.reply(env.guild_id, env.channel_id, 'Unable to find users or role from input')

        if not self.check_override_role(env):
            managed = await self.get_managed_roles(env, exclude_self_service=not self.check_only_self_service(env, users))
            if role_obj not in managed:
                return await self.reply(env.guild_id, env.channel_id, f'Cannot remove users from role "{role_obj.name}", you do not manage role. Use `!role available` to see a list of roles you manage')

        for user_obj in users:
            if role_obj.id not in user_obj.role_ids:
                await self.reply(env.guild_id, env.channel_id, f'User "{user_obj.display_name}" does not have role "{role_obj.name}", skipping')
                continue
            await self.role_gateway.remove_role(env.guild_id, user_obj.id, role_obj.id)
            await self.reply(env.guild_id, env.channel_id, f'Removed user "{user_obj.display_name}" from role "{role_obj.name}"')
        return True
