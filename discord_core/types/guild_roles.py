'''
Bodies for the guild-roles routes (routes/guild_roles.py): requests, 202 answers and results.

Results are the 200 bodies of `GET /dispatch/results/{request_id}`, as `types/results.py` has them
for the older fetches, and a failed call is a `DispatchErrorResultBody` there rather than anything
here.

**Field order is the wire**, for the reason `types/results.py` gives, and `call_id` is a field of
every request on purpose: the dispatcher folds the body into the request id (routes/guild_roles.py).
'''
from typing import Literal

from pydantic import BaseModel


class FetchRolesRequestBody(BaseModel):
    '''POST /dispatch/fetch_roles'''
    guild_id: int
    call_id: str
    span_context: dict | None = None


class FetchMemberRequestBody(BaseModel):
    '''POST /dispatch/fetch_member'''
    guild_id: int
    user_id: int
    call_id: str
    span_context: dict | None = None


class FetchRoleMembersRequestBody(BaseModel):
    '''POST /dispatch/fetch_role_members'''
    guild_id: int
    role_id: int
    call_id: str
    span_context: dict | None = None


class ChangeMemberRoleRequestBody(BaseModel):
    '''POST /dispatch/change_member_role'''
    guild_id: int
    user_id: int
    role_id: int
    action: Literal['add', 'remove']
    call_id: str
    span_context: dict | None = None


class FetchRolesResponse(BaseModel):
    '''202 from POST /dispatch/fetch_roles; poll GET /dispatch/results/{id}.'''
    request_id: str


class FetchMemberResponse(BaseModel):
    '''202 from POST /dispatch/fetch_member; poll GET /dispatch/results/{id}.'''
    request_id: str


class FetchRoleMembersResponse(BaseModel):
    '''202 from POST /dispatch/fetch_role_members; poll GET /dispatch/results/{id}.'''
    request_id: str


class ChangeMemberRoleResponse(BaseModel):
    '''202 from POST /dispatch/change_member_role; poll GET /dispatch/results/{id}.'''
    request_id: str


class RoleBody(BaseModel):
    '''One role.'''
    id: int
    name: str


class MemberBody(BaseModel):
    '''One member and the ids of the roles they hold.'''
    id: int
    display_name: str
    role_ids: list[int]


class GuildRolesResultBody(BaseModel):
    '''200 body for a completed fetch_roles: every role, in the order Discord lists them.'''
    guild_id: int
    roles: list[RoleBody]


class MemberResultBody(BaseModel):
    '''200 body for a completed fetch_member. `member` is None when they are not in the guild.'''
    guild_id: int
    user_id: int
    member: MemberBody | None = None


class RoleMembersResultBody(BaseModel):
    '''200 body for a completed fetch_role_members.'''
    guild_id: int
    role_id: int
    members: list[MemberBody]


class ChangeMemberRoleResultBody(BaseModel):
    '''200 body for a completed change_member_role: the change was made.'''
    guild_id: int
    user_id: int
    role_id: int
    action: Literal['add', 'remove']
