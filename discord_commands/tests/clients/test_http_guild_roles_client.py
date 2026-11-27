'''HttpGuildRolesClient, tested against the dispatch fetch contract and nothing else.

The client is served by a `ContractStub` built from `testing/contracts/dispatch_fetches.py`, the same description
the dispatcher's real server is proved against. So this checks the client's half of the seam: each call POSTs a
body the contract accepts (the stub records a violation, and the test fails, if not), polls the id it was given,
and turns every declared result into what the role commands expect. It does not need the dispatcher pod.

That the dispatcher does the right thing with a body it accepts is its own tests
(discord_dispatcher/tests/), plus the round trips in
tests/integration/commands_to_dispatcher/test_http_guild_roles_client.py.
'''
# NOTE: test names here deliberately avoid being exactly 40 characters long.
# trufflehog's Lob detector matches `test_` + 35 chars, so a 40-char test
# function name is reported as a VERIFIED secret and fails pr-check:secrets.
import pytest

from discord_core.clients import http_dispatch_client
from discord_core.clients.dispatch_client_base import DispatchRemoteError
from discord_core.routes import dispatch as dispatch_routes
from discord_core.routes import guild_roles as routes
from discord_core.types import guild_roles as bodies
from discord_core.types.dispatch_responses import ResultPendingResponse
from discord_core.types.results import DispatchErrorDetailBody, DispatchErrorResultBody

from discord_commands.clients.http_guild_roles_client import HttpGuildRolesClient
from discord_commands.command_logic.role_gateway import MemberInfo, RoleInfo

from testing.contracts.dispatch_fetches import CONTRACTS
from testing.contracts.stub import ContractStub, serving

GUILD = 11
REQUEST_ID = 'req-7'


def _client(tc) -> HttpGuildRolesClient:
    return HttpGuildRolesClient(str(tc.make_url('')), session=tc.session)


def _answering(route, accepted, result) -> ContractStub:
    '''A stub whose ``route`` hands out REQUEST_ID and whose result route then answers ``result``.'''
    stub = ContractStub(CONTRACTS)
    stub.script(route, accepted(request_id=REQUEST_ID), status=202)
    stub.script(dispatch_routes.GET_RESULT, result)
    return stub


@pytest.mark.asyncio
async def test_list_roles_asks_for_the_guild_and_polls_the_id_it_got():
    '''The roles come back in Discord's order, and the poll goes to the id the 202 handed out.'''
    stub = _answering(routes.FETCH_ROLES, bodies.FetchRolesResponse, bodies.GuildRolesResultBody(
        guild_id=GUILD, roles=[bodies.RoleBody(id=5, name='dj'), bodies.RoleBody(id=6, name='mod')]))
    async with serving(stub) as tc:
        roles = await _client(tc).list_roles(GUILD)
    assert roles == [RoleInfo(id=5, name='dj'), RoleInfo(id=6, name='mod')]
    assert stub.calls_to(routes.FETCH_ROLES)[0]['guild_id'] == GUILD
    assert stub.paths_to(dispatch_routes.GET_RESULT) == [{'request_id': REQUEST_ID}]


@pytest.mark.asyncio
@pytest.mark.parametrize('role_id, expected', [(6, RoleInfo(id=6, name='mod')), (99, None)])
async def test_get_role_is_one_of_the_guilds_roles_or_none(role_id, expected):
    '''There is no route for one role: it is picked out of the guild's list.'''
    stub = _answering(routes.FETCH_ROLES, bodies.FetchRolesResponse, bodies.GuildRolesResultBody(
        guild_id=GUILD, roles=[bodies.RoleBody(id=5, name='dj'), bodies.RoleBody(id=6, name='mod')]))
    async with serving(stub) as tc:
        assert await _client(tc).get_role(GUILD, role_id) == expected


@pytest.mark.asyncio
async def test_get_member_maps_the_member_and_sends_both_ids():
    '''The roles a member holds become a tuple, and the member asked about is the one on the wire.'''
    stub = _answering(routes.FETCH_MEMBER, bodies.FetchMemberResponse, bodies.MemberResultBody(
        guild_id=GUILD, user_id=2, member=bodies.MemberBody(id=2, display_name='Ann', role_ids=[5, 6])))
    async with serving(stub) as tc:
        member = await _client(tc).get_member(GUILD, 2)
    assert member == MemberInfo(id=2, display_name='Ann', role_ids=(5, 6))
    sent = stub.calls_to(routes.FETCH_MEMBER)[0]
    assert (sent['guild_id'], sent['user_id']) == (GUILD, 2)


@pytest.mark.asyncio
async def test_get_member_is_none_when_they_are_not_in_the_guild():
    '''A null member is an answer, not a failure.'''
    stub = _answering(routes.FETCH_MEMBER, bodies.FetchMemberResponse,
                      bodies.MemberResultBody(guild_id=GUILD, user_id=9, member=None))
    async with serving(stub) as tc:
        assert await _client(tc).get_member(GUILD, 9) is None


@pytest.mark.asyncio
async def test_role_members_maps_every_member_and_sends_the_role():
    '''Each holder is mapped, and the role asked about is the one on the wire.'''
    stub = _answering(routes.FETCH_ROLE_MEMBERS, bodies.FetchRoleMembersResponse, bodies.RoleMembersResultBody(
        guild_id=GUILD, role_id=6, members=[bodies.MemberBody(id=3, display_name='Bo', role_ids=[6])]))
    async with serving(stub) as tc:
        members = await _client(tc).role_members(GUILD, 6)
    assert members == [MemberInfo(id=3, display_name='Bo', role_ids=(6,))]
    sent = stub.calls_to(routes.FETCH_ROLE_MEMBERS)[0]
    assert (sent['guild_id'], sent['role_id']) == (GUILD, 6)


@pytest.mark.asyncio
async def test_add_and_remove_send_their_own_action():
    '''One route does both, and the action is the only thing telling the dispatcher which.'''
    stub = _answering(routes.CHANGE_MEMBER_ROLE, bodies.ChangeMemberRoleResponse,
                      bodies.ChangeMemberRoleResultBody(guild_id=GUILD, user_id=2, role_id=5, action='add'))
    async with serving(stub) as tc:
        client = _client(tc)
        await client.add_role(GUILD, 2, 5)
        await client.remove_role(GUILD, 2, 5)
    sent = stub.calls_to(routes.CHANGE_MEMBER_ROLE)
    assert [(call['action'], call['guild_id'], call['user_id'], call['role_id']) for call in sent] == [
        ('add', GUILD, 2, 5), ('remove', GUILD, 2, 5)]


@pytest.mark.asyncio
async def test_every_call_has_its_own_call_id():
    '''The dispatcher keeps a result for a day under an id derived from the body, so a repeat of the same body
    would be answered with the first outcome and never reach Discord. A fresh call_id is what stops that.'''
    stub = _answering(routes.CHANGE_MEMBER_ROLE, bodies.ChangeMemberRoleResponse,
                      bodies.ChangeMemberRoleResultBody(guild_id=GUILD, user_id=2, role_id=5, action='add'))
    async with serving(stub) as tc:
        client = _client(tc)
        for _ in range(3):
            await client.add_role(GUILD, 2, 5)
    assert len({call['call_id'] for call in stub.calls_to(routes.CHANGE_MEMBER_ROLE)}) == 3


@pytest.mark.asyncio
async def test_a_failed_fetch_raises_what_the_dispatcher_said():
    '''A failure is a 200 holding an error body, and it surfaces as DispatchRemoteError with its detail.'''
    stub = _answering(routes.CHANGE_MEMBER_ROLE, bodies.ChangeMemberRoleResponse, DispatchErrorResultBody(
        error='Unknown Member', error_detail=DispatchErrorDetailBody(
            message='Unknown Member', type='NotFound', status=404, code=10007)))
    async with serving(stub) as tc:
        with pytest.raises(DispatchRemoteError, match='Unknown Member'):
            await _client(tc).add_role(GUILD, 404, 5)


@pytest.mark.asyncio
async def test_it_polls_again_while_the_result_is_pending(monkeypatch):
    '''A 202 from the result route means not yet: the client asks the same id again until it is 200.'''
    monkeypatch.setattr(http_dispatch_client, '_POLL_INTERVAL_BASE', 0)
    stub = ContractStub(CONTRACTS)
    stub.script(routes.FETCH_ROLES, bodies.FetchRolesResponse(request_id=REQUEST_ID), status=202)
    stub.script_sequence(dispatch_routes.GET_RESULT,
                         (202, ResultPendingResponse(status='pending')),
                         (202, ResultPendingResponse(status='pending')),
                         (200, bodies.GuildRolesResultBody(guild_id=GUILD, roles=[])))
    async with serving(stub) as tc:
        assert await _client(tc).list_roles(GUILD) == []
    assert stub.paths_to(dispatch_routes.GET_RESULT) == [{'request_id': REQUEST_ID}] * 3
