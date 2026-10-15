'''
What the role commands need from Discord, as plain data and one Protocol.

Roles and members live in discord.py's cache, so command logic reaches them only through
``RoleGateway`` and sees only these dataclasses, never a ``Member`` or ``Role``. The cog
implements it in-process today; the commands pod (#1030, Phase 2) would put an HTTP
client behind the same Protocol.
'''
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class RoleInfo:
    '''A guild role.'''
    id: int
    name: str


@dataclass(frozen=True)
class MemberInfo:
    '''A guild member and the ids of the roles they hold.'''
    id: int
    display_name: str
    role_ids: tuple[int, ...] = ()


class RoleGateway(Protocol):
    '''The calls role command logic may make on a guild.'''

    async def list_roles(self, guild_id: int) -> list[RoleInfo]:
        '''Every role in the guild, in the order Discord lists them.'''

    async def get_role(self, guild_id: int, role_id: int) -> RoleInfo | None:
        '''One role by id, or None when the guild has no such role.'''

    async def role_members(self, guild_id: int, role_id: int) -> list[MemberInfo]:
        '''The members holding a role.'''

    async def get_member(self, guild_id: int, user_id: int) -> MemberInfo | None:
        '''One member by id, or None when they are not in the guild.'''

    async def add_role(self, guild_id: int, user_id: int, role_id: int) -> None:
        '''Give the member the role.'''

    async def remove_role(self, guild_id: int, user_id: int, role_id: int) -> None:
        '''Take the role from the member.'''
