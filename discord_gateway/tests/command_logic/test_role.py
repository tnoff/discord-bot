import pytest

from discord_gateway.command_logic.envelope import CommandEnvelope
from discord_gateway.command_logic.role import RoleCommands
from discord_gateway.command_logic.role_gateway import MemberInfo, RoleInfo

GUILD = 11
STAFF = RoleInfo(id=100, name='Staff')
GAMER = RoleInfo(id=200, name='Gamer')
EVERYONE = RoleInfo(id=300, name='Team 2')
ALICE = MemberInfo(id=1, display_name='Alice', role_ids=(100,))
BOB = MemberInfo(id=2, display_name='Bob', role_ids=(100,))


class FakeRoleGateway:
    '''Guild state held in memory; add/remove edit the members it hands back.'''

    def __init__(self, roles, members):
        self.roles = list(roles)
        self.members = {m.id: m for m in members}

    async def list_roles(self, _guild_id):
        return list(self.roles)

    async def get_role(self, _guild_id, role_id):
        return next((r for r in self.roles if r.id == role_id), None)

    async def role_members(self, _guild_id, role_id):
        return [m for m in self.members.values() if role_id in m.role_ids]

    async def get_member(self, _guild_id, user_id):
        return self.members.get(user_id)

    async def add_role(self, _guild_id, user_id, role_id):
        member = self.members[user_id]
        self.members[user_id] = MemberInfo(member.id, member.display_name, member.role_ids + (role_id,))

    async def remove_role(self, _guild_id, user_id, role_id):
        member = self.members[user_id]
        self.members[user_id] = MemberInfo(member.id, member.display_name, tuple(r for r in member.role_ids if r != role_id))


def make_env(role_ids=(100,), author_id=1, name='Alice'):
    return CommandEnvelope(guild_id=GUILD, channel_id=22, author_id=author_id, author_display_name=name,
                           author_role_ids=role_ids)


def make_logic(reply, settings=None, roles=(STAFF, GAMER, EVERYONE), members=(ALICE, BOB)):
    gateway = FakeRoleGateway(roles, members)
    return RoleCommands(reply, gateway, settings if settings is not None else {}), gateway


def messages(sent):
    return [content for _, _, content in sent]


@pytest.mark.asyncio
async def test_role_list_skips_rejected(reply, sent):  #pylint:disable=redefined-outer-name
    logic, _ = make_logic(reply, {GUILD: {'rejected_roles_list': [200]}})
    assert await logic.role_list(make_env()) is True
    assert messages(sent) == ['```Role Name\n---------\n@Staff\n@Team 2```']


@pytest.mark.asyncio
async def test_role_list_requires_role(reply):  #pylint:disable=redefined-outer-name
    logic, _ = make_logic(reply, {GUILD: {'required_roles_list': [999]}})
    assert await logic.role_list(make_env()) == 'User "Alice" does not have required roles, skipping'


@pytest.mark.asyncio
async def test_role_list_users(reply, sent):  #pylint:disable=redefined-outer-name
    logic, _ = make_logic(reply)
    assert await logic.role_list_users(make_env(), 'staff') is True
    assert messages(sent) == ['```User Name\n---------\n@Alice\n@Bob```']
    assert await logic.role_list_users(make_env(), '“gamer”') == 'No users found for role "gamer"'
    assert await logic.role_list_users(make_env(), 'nope') == 'Unable to find role "nope"'


@pytest.mark.asyncio
async def test_get_role_reads_digits_as_an_id_before_the_name(reply):  #pylint:disable=redefined-outer-name
    '''A role named "Team 2" is looked up as id 2: kept from before the move.'''
    logic, _ = make_logic(reply)
    assert await logic.get_role(make_env(), 'Team 2') is None
    assert await logic.get_role(make_env(), '<@&200>') == GAMER


@pytest.mark.asyncio
async def test_get_user_or_role_all_users_leaves_no_role(reply):  #pylint:disable=redefined-outer-name
    logic, _ = make_logic(reply)
    users, role = await logic.get_user_or_role(make_env(), '<@1> <@2>')
    assert users == [ALICE, BOB]
    assert role is None


@pytest.mark.asyncio
async def test_role_managed_orders_full_before_self_serve(reply, sent):  #pylint:disable=redefined-outer-name
    settings = {GUILD: {100: {'manages_roles': [200]}, 'self_service_role_list': [300]}}
    logic, _ = make_logic(reply, settings)
    assert await logic.role_managed(make_env()) is True
    assert len(sent) == 1
    assert '@Gamer' in sent[0][2] and sent[0][2].index('@Gamer') < sent[0][2].index('@Team 2')


@pytest.mark.asyncio
async def test_role_managed_none(reply):  #pylint:disable=redefined-outer-name
    logic, _ = make_logic(reply)
    assert await logic.role_managed(make_env()) == 'No roles found'


@pytest.mark.asyncio
async def test_managed_roles_look_each_role_up_once(reply):  #pylint:disable=redefined-outer-name
    settings = {GUILD: {100: {'manages_roles': [200]}, 400: {'manages_roles': [200]}}}
    logic, gateway = make_logic(reply, settings)
    calls = []
    original = gateway.get_role

    async def counting(guild_id, role_id):
        calls.append(role_id)
        return await original(guild_id, role_id)
    gateway.get_role = counting
    managed = await logic.get_managed_roles(make_env(role_ids=(100, 400)))
    assert managed == {GAMER: False}
    assert calls == [200]


@pytest.mark.asyncio
async def test_role_add_and_remove(reply, sent):  #pylint:disable=redefined-outer-name
    settings = {GUILD: {100: {'manages_roles': [200]}}}
    logic, gateway = make_logic(reply, settings)
    assert await logic.role_add(make_env(), '<@2> <@&200>') is True
    assert messages(sent) == ['Added user "Bob" to role "Gamer"']
    assert 200 in gateway.members[2].role_ids
    assert await logic.role_add(make_env(), '<@2> <@&200>') is True
    assert sent[-1][2] == 'User "Bob" already has role "Gamer", skipping'
    assert await logic.role_remove(make_env(), '<@2> <@&200>') is True
    assert sent[-1][2] == 'Removed user "Bob" from role "Gamer"'
    assert await logic.role_remove(make_env(), '<@2> <@&200>') is True
    assert sent[-1][2] == 'User "Bob" does not have role "Gamer", skipping'


@pytest.mark.asyncio
async def test_role_add_requires_the_target_to_hold_a_required_role(reply, sent):  #pylint:disable=redefined-outer-name
    settings = {GUILD: {'required_roles_list': [100], 100: {'manages_roles': [200]}}}
    outsider = MemberInfo(id=3, display_name='Cy', role_ids=())
    logic, gateway = make_logic(reply, settings, members=(ALICE, outsider))
    await logic.role_add(make_env(), '<@3> <@&200>')
    assert messages(sent) == ['User "Cy" does not have required roles, skipping']
    assert gateway.members[3].role_ids == ()


@pytest.mark.asyncio
async def test_role_add_override_skips_manage_and_target_checks(reply, sent):  #pylint:disable=redefined-outer-name
    settings = {GUILD: {'required_roles_list': [100], 'admin_override_role_list': [100]}}
    outsider = MemberInfo(id=3, display_name='Cy', role_ids=())
    logic, gateway = make_logic(reply, settings, members=(ALICE, outsider))
    await logic.role_add(make_env(), '<@3> <@&200>')
    assert messages(sent) == ['Added user "Cy" to role "Gamer"']
    assert 200 in gateway.members[3].role_ids


@pytest.mark.asyncio
async def test_role_add_unmanaged_role_refused(reply):  #pylint:disable=redefined-outer-name
    logic, _ = make_logic(reply)
    result = await logic.role_add(make_env(), '<@2> <@&200>')
    assert result == 'Cannot add users to role "Gamer", you do not manage role. Use `!role available` to see a list of roles you manage'
    result = await logic.role_remove(make_env(), '<@2> <@&200>')
    assert result == 'Cannot remove users from role "Gamer", you do not manage role. Use `!role available` to see a list of roles you manage'


@pytest.mark.asyncio
async def test_role_add_self_service_only_for_yourself(reply, sent):  #pylint:disable=redefined-outer-name
    settings = {GUILD: {'self_service_role_list': [200]}}
    logic, gateway = make_logic(reply, settings)
    await logic.role_add(make_env(), '<@2> <@&200>')
    assert sent[-1][2].startswith('Cannot add users to role "Gamer"')
    await logic.role_add(make_env(), '<@1> <@&200>')
    assert sent[-1][2] == 'Added user "Alice" to role "Gamer"'
    assert 200 in gateway.members[1].role_ids


@pytest.mark.asyncio
async def test_role_add_remove_need_users_and_role(reply):  #pylint:disable=redefined-outer-name
    logic, _ = make_logic(reply)
    assert await logic.role_add(make_env(), 'nobody') == 'Unable to find users or role from input'
    assert await logic.role_remove(make_env(), 'nobody') == 'Unable to find users or role from input'


@pytest.mark.asyncio
async def test_role_remove_requires_role(reply):  #pylint:disable=redefined-outer-name
    logic, _ = make_logic(reply, {GUILD: {'required_roles_list': [999]}})
    assert await logic.role_remove(make_env(), '<@2> <@&200>') == 'User "Alice" does not have required roles, skipping'
    assert await logic.role_managed(make_env()) == 'User "Alice" does not have required roles, skipping'
    assert await logic.role_list_users(make_env(), 'staff') == 'User "Alice" does not have required roles, skipping'
