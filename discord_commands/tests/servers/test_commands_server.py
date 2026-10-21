import asyncio
import contextlib

import aiohttp
import pytest
from aiohttp.test_utils import TestServer

from discord_core.clients import http_commands_client
from discord_core.clients.http_commands_client import HttpCommandsClient
from discord_core.interfaces.commands_client import CommandsUnavailable
from discord_core.routes import commands as command_routes
from discord_core.types.command_args import PlayArgs
from discord_core.types.command_envelope import CommandEnvelope
from discord_core.utils.circuit_breaker import CircuitBreaker

from discord_commands.servers.commands_server import CommandsHttpServer
from discord_commands.workers.command_executor import CommandExecutor

ENVELOPE = CommandEnvelope(guild_id=1, channel_id=2, author_id=3, author_display_name='tyler',
                           author_role_ids=(9,))


@contextlib.asynccontextmanager
async def served(handlers):
    '''A real server over a real executor, and a client pointed at it.'''
    executor = CommandExecutor(handlers)
    commands_server = CommandsHttpServer(executor)
    async with TestServer(commands_server.build_app()) as server:
        async with aiohttp.ClientSession() as session:
            yield (executor, server, session,
                   HttpCommandsClient(str(server.make_url('')), session=session), commands_server)
    await executor.close()


@pytest.fixture(autouse=True)
def fresh_breaker(monkeypatch):
    '''The client's breaker is per process; a test that trips it must not affect the next.'''
    monkeypatch.setattr(http_commands_client, '_BREAKER', CircuitBreaker(name='commands'))


def test_it_serves_exactly_the_routes_in_the_registry():
    app = CommandsHttpServer(CommandExecutor({})).build_app()
    served_routes = {(r.method, r.resource.canonical) for r in app.router.routes()
                     if r.method not in ('HEAD',) and r.resource.canonical != '/_contract'}
    assert {('POST', route.template) for route in command_routes.ALL} <= served_routes
    assert set(CommandsHttpServer(CommandExecutor({})).route_handlers()) == set(command_routes.ALL)


@pytest.mark.asyncio
async def test_the_client_and_server_agree_end_to_end():
    ran = []

    async def play(env, args):
        ran.append((env, args))

    async with served({'play': play}) as (executor, _, _, client, _):
        reply = await client.forward('play', ENVELOPE, {'search': 'a song'}, request_id='r-1')
        assert (reply.status, reply.duplicate) == ('ok', False)
        await asyncio.gather(*executor._workers.values())  # pylint: disable=protected-access
    assert ran == [(ENVELOPE, PlayArgs(search='a song'))]


@pytest.mark.asyncio
async def test_a_replayed_request_is_reported_as_a_duplicate_and_runs_once():
    ran = []

    async def play(_env, args):
        ran.append(args)

    async with served({'play': play}) as (executor, _, _, client, _):
        first = await client.forward('play', ENVELOPE, {'search': 'a'}, request_id='r-1')
        again = await client.forward('play', ENVELOPE, {'search': 'a'}, request_id='r-1')
        await asyncio.gather(*executor._workers.values())  # pylint: disable=protected-access
    assert (first.duplicate, again.duplicate) == (False, True)
    assert len(ran) == 1


async def _post(session, server, path, body=None, data=None):
    async with session.post(str(server.make_url(path)), json=body, data=data) as resp:
        return resp.status


@pytest.mark.asyncio
async def test_a_command_the_contract_does_not_know_is_a_422():
    async with served({}) as (_, server, session, _, _):
        status = await _post(session, server, '/commands/not-a-command', {
            'envelope': ENVELOPE.model_dump(mode='json'), 'args': {}, 'request_id': 'r-1'})
    assert status == 422


@pytest.mark.asyncio
async def test_arguments_that_do_not_fit_are_a_422():
    async with served({'play': lambda *_: None}) as (_, server, session, _, _):
        status = await _post(session, server, '/commands/play', {
            'envelope': ENVELOPE.model_dump(mode='json'), 'args': {}, 'request_id': 'r-1'})
    assert status == 422


@pytest.mark.asyncio
async def test_a_body_that_is_not_json_is_a_422():
    async with served({'skip': lambda *_: None}) as (_, server, session, _, _):
        status = await _post(session, server, '/commands/skip', data='not json')
    assert status == 422


@pytest.mark.asyncio
async def test_a_body_without_an_envelope_is_a_422():
    async with served({'skip': lambda *_: None}) as (_, server, session, _, _):
        status = await _post(session, server, '/commands/skip', {'args': {}, 'request_id': 'r-1'})
    assert status == 422


@pytest.mark.asyncio
async def test_a_known_command_with_no_handler_is_a_501_the_client_calls_unavailable():
    """A gateway rolled ahead of this pod gets 'commands unavailable', not a dropped command."""
    async with served({}) as (_, _, _, client, _):
        with pytest.raises(CommandsUnavailable):
            await client.forward('skip', ENVELOPE)


@pytest.mark.asyncio
async def test_a_draining_server_refuses_new_commands():
    body = {'envelope': ENVELOPE.model_dump(mode='json'), 'args': {}, 'request_id': 'r-1'}
    async with served({'skip': lambda *_: None}) as (_, server, session, _, commands_server):
        commands_server.start_draining()
        status = await _post(session, server, '/commands/skip', body)
    assert status == 503
