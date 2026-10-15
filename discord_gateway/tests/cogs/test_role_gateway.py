import pytest

from discord_gateway.cogs.role import RoleAssignment
from discord_gateway.cogs.role_gateway import CogRoleGateway
from discord_gateway.command_logic.role_gateway import MemberInfo, RoleInfo

from tests.helpers import FakeAuthor, FakeRole
from tests.helpers import fake_context #pylint:disable=unused-import

CONFIG = {'general': {'include': {'role': True}}, 'role': {'foo': {'bar': []}}}


def make_gateway(fakes):
    return CogRoleGateway(RoleAssignment(fakes['bot'], CONFIG, fakes['dispatcher']))


@pytest.mark.asyncio
async def test_list_and_get_role(fake_context):  #pylint:disable=redefined-outer-name
    gateway = make_gateway(fake_context)
    role = fake_context['guild'].roles[0]
    guild_id = fake_context['guild'].id
    assert await gateway.list_roles(guild_id) == [RoleInfo(role.id, role.name)]
    assert await gateway.get_role(guild_id, role.id) == RoleInfo(role.id, role.name)
    assert await gateway.get_role(guild_id, role.id + 1) is None


@pytest.mark.asyncio
async def test_get_role_when_discord_returns_none(fake_context, mocker):  #pylint:disable=redefined-outer-name
    '''discord.py itself returns None for an unknown role rather than raising.'''
    mocker.patch.object(fake_context['guild'], 'get_role', return_value=None)
    assert await make_gateway(fake_context).get_role(fake_context['guild'].id, 5) is None


@pytest.mark.asyncio
async def test_role_members(fake_context):  #pylint:disable=redefined-outer-name
    gateway = make_gateway(fake_context)
    author = fake_context['author']
    role = fake_context['guild'].roles[0]
    assert await gateway.role_members(fake_context['guild'].id, role.id) == [
        MemberInfo(author.id, author.display_name, (role.id,))]
    assert await gateway.role_members(fake_context['guild'].id, role.id + 1) == []


@pytest.mark.asyncio
async def test_get_member(fake_context):  #pylint:disable=redefined-outer-name
    gateway = make_gateway(fake_context)
    author = fake_context['author']
    guild_id = fake_context['guild'].id
    assert (await gateway.get_member(guild_id, author.id)).display_name == author.display_name
    assert await gateway.get_member(guild_id, author.id + 1) is None


@pytest.mark.asyncio
async def test_add_and_remove_role(fake_context):  #pylint:disable=redefined-outer-name
    gateway = make_gateway(fake_context)
    role = FakeRole()
    user = FakeAuthor()
    fake_context['guild'].roles.append(role)
    fake_context['guild'].members.append(user)
    await gateway.add_role(fake_context['guild'].id, user.id, role.id)
    assert role in user.roles
    await gateway.remove_role(fake_context['guild'].id, user.id, role.id)
    assert role not in user.roles
