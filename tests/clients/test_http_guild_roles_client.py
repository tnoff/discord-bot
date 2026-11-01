'''
HttpGuildRolesClient (the commands pod's RoleGateway over the dispatcher), tested over the wire:
a real DispatchHttpServer in front of a dispatcher that runs the real GuildRolesCalls against a
fake Discord, as MessageDispatcher does once it has dequeued the call.
'''
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiohttp.test_utils import TestClient, TestServer

from discord_core.clients.dispatch_client_base import DispatchRemoteError

from discord_commands.clients.http_guild_roles_client import HttpGuildRolesClient
from discord_commands.command_logic.role_gateway import MemberInfo, RoleInfo

from discord_dispatcher.servers.dispatch_server import DispatchHttpServer
from discord_dispatcher.tests.workers.test_guild_roles import make_guild, make_member
from discord_dispatcher.workers.guild_roles import GuildRolesCalls

from tests.helpers import FakeDispatchServer, FakeRedisDispatchQueue


class RunningDispatcher(FakeDispatchServer):
    '''Runs each guild-roles call at once and stores what MessageDispatcher would store.'''

    def __init__(self, guild, result_store):
        super().__init__(result_store)
        self.guild = guild

    async def enqueue_guild_roles_call(self, request_id, kind, params, **_):
        await super().enqueue_guild_roles_call(request_id, kind, params)
        bot = SimpleNamespace(fetch_guild=AsyncMock(return_value=self.guild))
        try:
            self._result_store[request_id] = await GuildRolesCalls(bot).run(kind, params)
        except Exception as exc:  # pylint: disable=broad-except
            self._result_store[request_id] = {'error': str(exc)}


def make_pod(guild):
    result_store = {}
    dispatcher = RunningDispatcher(guild, result_store)
    return dispatcher, DispatchHttpServer(dispatcher, FakeRedisDispatchQueue(result_store))


@pytest.mark.asyncio
async def test_roles_and_one_role():
    _, server = make_pod(make_guild())
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpGuildRolesClient(str(tc.make_url('')), session=tc.session)
        assert await client.list_roles(11) == [RoleInfo(id=5, name='dj'), RoleInfo(id=6, name='mod')]
        assert await client.get_role(11, 6) == RoleInfo(id=6, name='mod')
        assert await client.get_role(11, 99) is None


@pytest.mark.asyncio
async def test_members():
    guild = make_guild([make_member(2, 'Ann', [5]), make_member(3, 'Bo', [6])])
    _, server = make_pod(guild)
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpGuildRolesClient(str(tc.make_url('')), session=tc.session)
        assert await client.get_member(11, 2) == MemberInfo(id=2, display_name='Ann', role_ids=(5,))
        assert await client.get_member(11, 9) is None
        assert await client.role_members(11, 6) == [MemberInfo(id=3, display_name='Bo', role_ids=(6,))]


@pytest.mark.asyncio
async def test_add_then_remove_reach_discord_each_time():
    '''Each call has its own call_id, so a repeat is a new request and not the stored result.'''
    member = make_member(2, 'Ann', [])
    dispatcher, server = make_pod(make_guild([member]))
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpGuildRolesClient(str(tc.make_url('')), session=tc.session)
        await client.add_role(11, 2, 5)
        await client.remove_role(11, 2, 5)
        await client.add_role(11, 2, 5)
    assert member.add_roles.await_count == 2
    assert member.remove_roles.await_count == 1
    assert len({call[1] for call in dispatcher.calls}) == 3


@pytest.mark.asyncio
async def test_a_failure_raises_to_the_command():
    _, server = make_pod(make_guild())
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpGuildRolesClient(str(tc.make_url('')), session=tc.session)
        # The fake member lookup raises for a member that does not exist when asked to change roles.
        with pytest.raises(DispatchRemoteError):
            await client.add_role(11, 404, 5)
