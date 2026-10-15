from discord import Member
from discord.errors import NotFound

from discord_gateway.command_logic.role_gateway import MemberInfo, RoleInfo


def _member_info(member: Member) -> MemberInfo:
    return MemberInfo(id=member.id, display_name=member.display_name,
                      role_ids=tuple(role.id for role in member.roles))


class CogRoleGateway:
    '''
    ``RoleGateway`` implemented in-process, over the bot's guild cache.

    Every method resolves ids to discord objects and does what the command bodies used to
    do inline, so the behaviour is the same code moved, not rewritten. Stateless.
    '''

    def __init__(self, cog):
        self.cog = cog

    def _guild(self, guild_id: int):
        return self.cog.bot.get_guild(guild_id)

    def _role(self, guild_id: int, role_id: int):
        try:
            return self._guild(guild_id).get_role(role_id)
        except NotFound:
            return None

    async def list_roles(self, guild_id: int) -> list[RoleInfo]:
        '''Every role in the guild.'''
        return [RoleInfo(id=role.id, name=role.name) for role in self._guild(guild_id).roles]

    async def get_role(self, guild_id: int, role_id: int) -> RoleInfo | None:
        '''One role by id.'''
        role = self._role(guild_id, role_id)
        if not role:
            return None
        return RoleInfo(id=role.id, name=role.name)

    async def role_members(self, guild_id: int, role_id: int) -> list[MemberInfo]:
        '''The members holding a role.'''
        role = self._role(guild_id, role_id)
        if not role:
            return []
        return [_member_info(member) for member in role.members]

    async def get_member(self, guild_id: int, user_id: int) -> MemberInfo | None:
        '''One member, fetched from Discord rather than the cache.'''
        try:
            member = await self._guild(guild_id).fetch_member(user_id)
        except NotFound:
            return None
        return _member_info(member)

    async def add_role(self, guild_id: int, user_id: int, role_id: int) -> None:
        '''Give the member the role.'''
        member = await self._guild(guild_id).fetch_member(user_id)
        await member.add_roles(self._role(guild_id, role_id))

    async def remove_role(self, guild_id: int, user_id: int, role_id: int) -> None:
        '''Take the role from the member.'''
        member = await self._guild(guild_id).fetch_member(user_id)
        await member.remove_roles(self._role(guild_id, role_id))
