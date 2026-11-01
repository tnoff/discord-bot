'''
The role commands' view of Discord, over the dispatcher (#1030).

``RoleGateway`` (command_logic/role_gateway.py) implemented on the dispatch seam's guild-roles
fetches: each call is a POST that answers 202 with a request id, then polls the result, which
``HttpDispatchClient`` already does for the history and emoji fetches. This pod has no gateway
connection and so no guild cache, and the dispatcher asks Discord over REST.

It is an ``HttpDispatchClient`` because it is the pod's dispatch client: the same instance also
sends the replies, and shares the breaker and the peer route check, which now covers the
guild-roles routes.

Every call carries a fresh ``call_id``. The dispatcher keeps a result for a day under an id derived
from the request, so without one a second ``role add`` for the same member and role would be
answered with the first outcome without reaching Discord. A failure raises ``DispatchRemoteError``.
'''
from uuid import uuid4

from pydantic import BaseModel

from discord_core.clients.http_dispatch_client import HttpDispatchClient
from discord_core.routes import guild_roles as guild_roles_routes
from discord_core.routes.route import Route
from discord_core.types import guild_roles as bodies
from discord_core.utils.otel import capture_span_context

from discord_commands.command_logic.role_gateway import MemberInfo, RoleInfo


def _member_info(member: bodies.MemberBody | None) -> MemberInfo | None:
    if member is None:
        return None
    return MemberInfo(id=member.id, display_name=member.display_name, role_ids=tuple(member.role_ids))


class HttpGuildRolesClient(HttpDispatchClient):
    '''A dispatch client that also answers the role commands' ``RoleGateway``.'''

    async def _call(self, route: Route, response_model: type[BaseModel], result_model: type[BaseModel],
                    request: BaseModel) -> BaseModel:
        request_id = await self._submit_fetch(route, request.model_dump(), response_model)
        payload = await self._poll_result(request_id)
        return self._validate(result_model, self._checked_result(payload, result_model))

    async def list_roles(self, guild_id: int) -> list[RoleInfo]:
        '''Every role in the guild.'''
        result = await self._call(
            guild_roles_routes.FETCH_ROLES, bodies.FetchRolesResponse, bodies.GuildRolesResultBody,
            bodies.FetchRolesRequestBody(guild_id=guild_id, call_id=uuid4().hex,
                                         span_context=capture_span_context()))
        return [RoleInfo(id=role.id, name=role.name) for role in result.roles]

    async def get_role(self, guild_id: int, role_id: int) -> RoleInfo | None:
        '''One role by id, or None when the guild has no such role.'''
        return next((role for role in await self.list_roles(guild_id) if role.id == role_id), None)

    async def role_members(self, guild_id: int, role_id: int) -> list[MemberInfo]:
        '''The members holding a role.'''
        result = await self._call(
            guild_roles_routes.FETCH_ROLE_MEMBERS, bodies.FetchRoleMembersResponse,
            bodies.RoleMembersResultBody,
            bodies.FetchRoleMembersRequestBody(guild_id=guild_id, role_id=role_id, call_id=uuid4().hex,
                                               span_context=capture_span_context()))
        return [_member_info(member) for member in result.members]

    async def get_member(self, guild_id: int, user_id: int) -> MemberInfo | None:
        '''One member by id, or None when they are not in the guild.'''
        result = await self._call(
            guild_roles_routes.FETCH_MEMBER, bodies.FetchMemberResponse, bodies.MemberResultBody,
            bodies.FetchMemberRequestBody(guild_id=guild_id, user_id=user_id, call_id=uuid4().hex,
                                          span_context=capture_span_context()))
        return _member_info(result.member)

    async def add_role(self, guild_id: int, user_id: int, role_id: int) -> None:
        '''Give the member the role.'''
        await self._change_member_role(guild_id, user_id, role_id, 'add')

    async def remove_role(self, guild_id: int, user_id: int, role_id: int) -> None:
        '''Take the role from the member.'''
        await self._change_member_role(guild_id, user_id, role_id, 'remove')

    async def _change_member_role(self, guild_id: int, user_id: int, role_id: int, action: str) -> None:
        await self._call(
            guild_roles_routes.CHANGE_MEMBER_ROLE, bodies.ChangeMemberRoleResponse,
            bodies.ChangeMemberRoleResultBody,
            bodies.ChangeMemberRoleRequestBody(guild_id=guild_id, user_id=user_id, role_id=role_id,
                                               action=action, call_id=uuid4().hex,
                                               span_context=capture_span_context()))
