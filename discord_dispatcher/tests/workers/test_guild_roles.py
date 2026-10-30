'''
GuildRolesCalls (workers/guild_roles.py) over a fake Discord, and the MessageDispatcher path that
runs it: enqueue, route, store the result or the failure.
'''
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from discord.errors import NotFound

from discord_dispatcher.workers.guild_roles import (
    CHANGE_MEMBER_ROLE, FETCH_MEMBER, FETCH_ROLE_MEMBERS, FETCH_ROLES, GuildRolesCalls,
)

from discord_dispatcher.tests.cogs.test_message_dispatcher import make_dispatcher


def make_member(member_id, name, role_ids):
    return SimpleNamespace(id=member_id, display_name=name,
                           roles=[SimpleNamespace(id=role_id) for role_id in role_ids],
                           add_roles=AsyncMock(), remove_roles=AsyncMock())


def make_guild(members=()):
    by_id = {member.id: member for member in members}

    async def fetch_member(user_id):
        if user_id not in by_id:
            raise NotFound(MagicMock(status=404), 'unknown member')
        return by_id[user_id]

    async def fetch_members(limit=None):  # pylint: disable=unused-argument
        for member in members:
            yield member

    return SimpleNamespace(
        id=11, roles=[SimpleNamespace(id=5, name='dj'), SimpleNamespace(id=6, name='mod')],
        fetch_member=fetch_member, fetch_members=fetch_members)


def make_calls(guild):
    return GuildRolesCalls(SimpleNamespace(fetch_guild=AsyncMock(return_value=guild)))


@pytest.mark.asyncio
async def test_fetch_roles():
    result = await make_calls(make_guild()).run(FETCH_ROLES, {'guild_id': 11})
    assert result == {'guild_id': 11, 'roles': [{'id': 5, 'name': 'dj'}, {'id': 6, 'name': 'mod'}]}


@pytest.mark.asyncio
async def test_fetch_member():
    guild = make_guild([make_member(2, 'Ann', [5, 6])])
    result = await make_calls(guild).run(FETCH_MEMBER, {'guild_id': 11, 'user_id': 2})
    assert result == {'guild_id': 11, 'user_id': 2,
                      'member': {'id': 2, 'display_name': 'Ann', 'role_ids': [5, 6]}}


@pytest.mark.asyncio
async def test_a_member_who_left_is_a_result_with_no_member():
    result = await make_calls(make_guild()).run(FETCH_MEMBER, {'guild_id': 11, 'user_id': 2})
    assert result == {'guild_id': 11, 'user_id': 2, 'member': None}


@pytest.mark.asyncio
async def test_fetch_role_members_keeps_only_holders():
    guild = make_guild([make_member(2, 'Ann', [5]), make_member(3, 'Bo', [6]), make_member(4, 'Cy', [5, 6])])
    result = await make_calls(guild).run(FETCH_ROLE_MEMBERS, {'guild_id': 11, 'role_id': 5})
    assert [member['id'] for member in result['members']] == [2, 4]
    assert result['role_id'] == 5


@pytest.mark.asyncio
@pytest.mark.parametrize('action,added,removed', [('add', 1, 0), ('remove', 0, 1)])
async def test_change_member_role(action, added, removed):
    member = make_member(2, 'Ann', [])
    result = await make_calls(make_guild([member])).run(
        CHANGE_MEMBER_ROLE, {'guild_id': 11, 'user_id': 2, 'role_id': 5, 'action': action})
    assert result == {'guild_id': 11, 'user_id': 2, 'role_id': 5, 'action': action}
    assert member.add_roles.await_count == added
    assert member.remove_roles.await_count == removed
    changed = member.add_roles if added else member.remove_roles
    assert changed.await_args.args[0].id == 5


@pytest.mark.asyncio
async def test_enqueue_adds_a_guild_roles_item():
    dispatcher = make_dispatcher()
    await dispatcher.enqueue_guild_roles_call('req-1', FETCH_ROLES, {'guild_id': 11})
    _priority, _seq, member, payload = dispatcher._work_queue._queue.get_nowait()  # pylint: disable=protected-access
    assert member == 'guild_roles:req-1'
    assert payload == {'kind': FETCH_ROLES, 'params': {'guild_id': 11}, 'span_context': None}


@pytest.mark.asyncio
async def test_dispatch_item_routes_guild_roles(mocker):
    dispatcher = make_dispatcher()
    handler = mocker.patch.object(dispatcher, '_process_guild_roles', new=AsyncMock())
    payload = {'kind': FETCH_ROLES, 'params': {'guild_id': 11}}
    await dispatcher._dispatch_item('guild_roles:req-1', payload)  # pylint: disable=protected-access
    handler.assert_awaited_once_with('req-1', payload)


@pytest.mark.asyncio
async def test_the_result_is_stored_under_the_request_id():
    dispatcher = make_dispatcher()
    dispatcher.bot.fetch_guild = AsyncMock(return_value=make_guild())
    await dispatcher._process_guild_roles(  # pylint: disable=protected-access
        'req-1', {'kind': FETCH_ROLES, 'params': {'guild_id': 11}})
    assert (await dispatcher._work_queue.get_result('req-1'))['roles'][0] == {'id': 5, 'name': 'dj'}  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_a_failure_is_stored_as_the_result_so_the_caller_does_not_hang():
    dispatcher = make_dispatcher()
    dispatcher.bot.fetch_guild = AsyncMock(side_effect=RuntimeError('discord is down'))
    await dispatcher._process_guild_roles(  # pylint: disable=protected-access
        'req-1', {'kind': FETCH_ROLES, 'params': {'guild_id': 11}})
    result = await asyncio.wait_for(dispatcher._work_queue.get_result('req-1'), 1)  # pylint: disable=protected-access
    assert result['error'] == 'discord is down'
    assert result['error_detail']['type'] == 'RuntimeError'
