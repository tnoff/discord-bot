'''
The Discord REST calls behind the guild-roles routes (discord_core/routes/guild_roles.py).

The dispatcher never connects to the gateway, so there is no guild cache to read: every call asks
Discord. ``Bot.fetch_guild`` returns the guild with its roles, which is all a member needs to
report the roles they hold.

``fetch_members`` is the one call that needs ``Intents.members`` set on the bot (discord.py refuses
without it), so the dispatcher's ``general.intents`` must list ``members``. The bot never opens a
gateway connection, so the flag costs nothing here; the Discord application must have the Server
Members privileged intent enabled, which the gateway already requires.
'''
from discord import Member, Object
from discord.errors import NotFound
from discord.ext.commands import Bot

from discord_core.types.guild_roles import (
    ChangeMemberRoleResultBody, GuildRolesResultBody, MemberBody, MemberResultBody, RoleBody,
    RoleMembersResultBody,
)

FETCH_ROLES = 'fetch_roles'
FETCH_MEMBER = 'fetch_member'
FETCH_ROLE_MEMBERS = 'fetch_role_members'
CHANGE_MEMBER_ROLE = 'change_member_role'


def _member_body(member: Member) -> MemberBody:
    return MemberBody(id=member.id, display_name=member.display_name,
                      role_ids=[role.id for role in member.roles])


class GuildRolesCalls:
    '''Runs one guild-roles call and returns its JSON-safe result body.'''

    def __init__(self, bot: Bot):
        self.bot = bot

    async def run(self, kind: str, payload: dict) -> dict:
        '''Run the call named *kind* for *payload*; raises whatever Discord raises.'''
        handler = {
            FETCH_ROLES: self._fetch_roles,
            FETCH_MEMBER: self._fetch_member,
            FETCH_ROLE_MEMBERS: self._fetch_role_members,
            CHANGE_MEMBER_ROLE: self._change_member_role,
        }[kind]
        return await handler(payload)

    async def _fetch_roles(self, payload: dict) -> dict:
        guild = await self.bot.fetch_guild(int(payload['guild_id']))
        return GuildRolesResultBody(
            guild_id=guild.id,
            roles=[RoleBody(id=role.id, name=role.name) for role in guild.roles],
        ).model_dump()

    async def _fetch_member(self, payload: dict) -> dict:
        guild = await self.bot.fetch_guild(int(payload['guild_id']))
        try:
            member = _member_body(await guild.fetch_member(int(payload['user_id'])))
        except NotFound:
            member = None
        return MemberResultBody(guild_id=guild.id, user_id=int(payload['user_id']), member=member).model_dump()

    async def _fetch_role_members(self, payload: dict) -> dict:
        role_id = int(payload['role_id'])
        guild = await self.bot.fetch_guild(int(payload['guild_id']))
        members = [_member_body(member) async for member in guild.fetch_members(limit=None)
                   if any(role.id == role_id for role in member.roles)]
        return RoleMembersResultBody(guild_id=guild.id, role_id=role_id, members=members).model_dump()

    async def _change_member_role(self, payload: dict) -> dict:
        guild = await self.bot.fetch_guild(int(payload['guild_id']))
        member = await guild.fetch_member(int(payload['user_id']))
        role = Object(id=int(payload['role_id']))
        if payload['action'] == 'add':
            await member.add_roles(role)
        else:
            await member.remove_roles(role)
        return ChangeMemberRoleResultBody(
            guild_id=guild.id, user_id=member.id, role_id=role.id, action=payload['action']).model_dump()
