'''
The role rules that decide who may change which role: settings lookups, required and override
roles, and what each author manages. Ported from the gateway cog's tests (#1030).
'''
import pytest

from discord_commands.command_logic.role_gateway import MemberInfo, RoleInfo
from discord_commands.tests.command_logic.test_role import (
    ALICE, BOB, EVERYONE, GAMER, GUILD, STAFF, make_env, make_logic, messages,
)


@pytest.mark.parametrize('method,key', [
    ('get_rejected_roles_list', 'rejected_roles_list'),
    ('get_required_roles', 'required_roles_list'),
    ('get_override_role', 'admin_override_role_list'),
    ('get_self_service_roles', 'self_service_role_list'),
])
def test_settings_lookups_read_the_guild_list_or_an_empty_one(reply, method, key):  #pylint:disable=redefined-outer-name
    logic, _ = make_logic(reply, {GUILD: {key: [7, 8]}})
    assert getattr(logic, method)(GUILD) == [7, 8]
    assert getattr(logic, method)(GUILD + 1) == []


def test_clean_input_drops_curly_quotes(reply):  #pylint:disable=redefined-outer-name
    logic, _ = make_logic(reply)
    assert logic.clean_input('“foo”') == 'foo'
    assert logic.clean_input('foo') == 'foo'


def test_no_required_roles_lets_anyone_through(reply):  #pylint:disable=redefined-outer-name
    logic, _ = make_logic(reply)
    assert logic.check_required_roles(make_env(role_ids=())) is True


def test_required_role_is_checked_on_the_author(reply):  #pylint:disable=redefined-outer-name
    logic, _ = make_logic(reply, {GUILD: {'required_roles_list': [100]}})
    assert logic.check_required_roles(make_env(role_ids=(100,))) is True
    assert logic.check_required_roles(make_env(role_ids=(5,))) is False


def test_required_role_is_checked_on_the_target_too(reply):  #pylint:disable=redefined-outer-name
    logic, _ = make_logic(reply, {GUILD: {'required_roles_list': [100]}})
    holder = MemberInfo(id=3, display_name='Cy', role_ids=(100,))
    outsider = MemberInfo(id=4, display_name='Di', role_ids=(5,))
    assert logic.check_required_roles(make_env(), user=holder) is True
    assert logic.check_required_roles(make_env(), user=outsider) is False


def test_an_author_without_the_required_role_fails_whatever_the_target_holds(reply):  #pylint:disable=redefined-outer-name
    logic, _ = make_logic(reply, {GUILD: {'required_roles_list': [100]}})
    holder = MemberInfo(id=3, display_name='Cy', role_ids=(100,))
    assert logic.check_required_roles(make_env(role_ids=(5,)), user=holder) is False


def test_override_role(reply):  #pylint:disable=redefined-outer-name
    logic, _ = make_logic(reply, {GUILD: {'admin_override_role_list': [100]}})
    assert logic.check_override_role(make_env(role_ids=(100,))) is True
    assert logic.check_override_role(make_env(role_ids=(5,))) is False
    assert make_logic(reply)[0].check_override_role(make_env()) is False


@pytest.mark.asyncio
async def test_role_list_with_every_role_rejected(reply):  #pylint:disable=redefined-outer-name
    logic, _ = make_logic(reply, {GUILD: {'rejected_roles_list': [100, 200, 300]}})
    assert await logic.role_list(make_env()) == 'No roles found'


@pytest.mark.asyncio
async def test_get_role_by_name_is_case_insensitive(reply):  #pylint:disable=redefined-outer-name
    logic, _ = make_logic(reply, roles=(STAFF, GAMER))
    assert await logic.get_role(make_env(), 'GAMER') == GAMER
    assert await logic.get_role(make_env(), 'nobody') is None


@pytest.mark.asyncio
async def test_get_role_by_unknown_id(reply):  #pylint:disable=redefined-outer-name
    logic, _ = make_logic(reply)
    assert await logic.get_role(make_env(), '<@&999>') is None


@pytest.mark.asyncio
async def test_get_user_without_digits_or_unknown(reply):  #pylint:disable=redefined-outer-name
    logic, _ = make_logic(reply)
    assert await logic.get_user(make_env(), 'nobody') is None
    assert await logic.get_user(make_env(), '<@999>') is None
    assert await logic.get_user(make_env(), '<@1>') == ALICE


@pytest.mark.asyncio
async def test_get_user_or_role_with_a_spaced_role_name(reply):  #pylint:disable=redefined-outer-name
    role = RoleInfo(id=400, name='Big Gamers')
    logic, _ = make_logic(reply, roles=(role,))
    users, found = await logic.get_user_or_role(make_env(), '<@1> <@2> Big Gamers')
    assert users == [ALICE, BOB]
    assert found == role


@pytest.mark.asyncio
async def test_managed_roles_skip_rejected_and_missing_ones(reply):  #pylint:disable=redefined-outer-name
    settings = {GUILD: {
        'rejected_roles_list': [200],
        'self_service_role_list': [200, 999, 300],
        100: {'manages_roles': [200, 999, 300, 100]},
    }}
    logic, _ = make_logic(reply, settings)
    managed = await logic.get_managed_roles(make_env())
    assert managed == {EVERYONE: True, STAFF: False}


@pytest.mark.asyncio
async def test_managed_roles_can_leave_self_service_out(reply):  #pylint:disable=redefined-outer-name
    settings = {GUILD: {'self_service_role_list': [300], 100: {'manages_roles': [200]}}}
    logic, _ = make_logic(reply, settings)
    assert await logic.get_managed_roles(make_env(), exclude_self_service=True) == {GAMER: False}


@pytest.mark.asyncio
async def test_managed_roles_do_not_override_a_self_service_entry(reply):  #pylint:disable=redefined-outer-name
    settings = {GUILD: {'self_service_role_list': [200], 100: {'manages_roles': [200]}}}
    logic, _ = make_logic(reply, settings)
    assert await logic.get_managed_roles(make_env()) == {GAMER: True}


def test_self_service_means_the_only_target_is_the_author(reply):  #pylint:disable=redefined-outer-name
    logic, _ = make_logic(reply)
    assert logic.check_only_self_service(make_env(), [ALICE]) is True
    assert logic.check_only_self_service(make_env(), [BOB]) is False
    assert logic.check_only_self_service(make_env(), [ALICE, BOB]) is False


@pytest.mark.asyncio
async def test_role_add_requires_the_authors_required_role(reply, sent):  #pylint:disable=redefined-outer-name
    logic, gateway = make_logic(reply, {GUILD: {'required_roles_list': [999], 100: {'manages_roles': [200]}}})
    assert await logic.role_add(make_env(), '<@2> <@&200>') == 'User "Alice" does not have required roles, skipping'
    assert messages(sent) == ['User "Alice" does not have required roles, skipping']
    assert 200 not in gateway.members[2].role_ids


@pytest.mark.asyncio
async def test_role_remove_with_override_skips_the_manage_check(reply, sent):  #pylint:disable=redefined-outer-name
    settings = {GUILD: {'admin_override_role_list': [100]}}
    logic, gateway = make_logic(reply, settings, members=(ALICE, BOB))
    await logic.role_add(make_env(), '<@2> <@&200>')
    await logic.role_remove(make_env(), '<@2> <@&200>')
    assert messages(sent) == ['Added user "Bob" to role "Gamer"', 'Removed user "Bob" from role "Gamer"']
    assert 200 not in gateway.members[2].role_ids


@pytest.mark.asyncio
async def test_self_service_cannot_touch_another_user_without_override(reply, sent):  #pylint:disable=redefined-outer-name
    settings = {GUILD: {'self_service_role_list': [200]}}
    logic, gateway = make_logic(reply, settings)
    await logic.role_remove(make_env(), '<@2> <@&200>')
    assert sent[-1][2].startswith('Cannot remove users from role "Gamer"')
    assert gateway.members[2].role_ids == (100,)
