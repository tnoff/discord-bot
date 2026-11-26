'''The player-session calls on HttpBrokerClient against a scripted broker, including deploy-skew 404s.'''
import aiohttp
import pytest

from discord_core.clients.http_broker_client import HttpBrokerClient
from discord_core.clients.http_client_base import SeamResponseInvalid
from discord_core.routes import broker as broker_routes
from discord_core.tests.clients.stub_broker import serve
from discord_core.tests.clients.stub_http import ScriptedServer, ok
from discord_core.types.player_session import PlayerSession

SESSIONS = (broker_routes.LIST_SESSIONS, broker_routes.SAVE_SESSION, broker_routes.DELETE_SESSION)


def _session(guild_id: int = 31) -> PlayerSession:
    '''A session that was mid-track.'''
    return PlayerSession(guild_id=guild_id, voice_channel_id=32, text_channel_id=33, was_playing=True)


@pytest.mark.asyncio
async def test_save_puts_the_session_under_its_guild():
    '''save_player_session PUTs the dumped session to /sessions/{guild_id}.'''
    server = ScriptedServer(SESSIONS)
    server.script(broker_routes.SAVE_SESSION, ok({'status': 'ok'}, status=201))
    async with serve(server) as (url, http):
        await HttpBrokerClient(url, session=http).save_player_session(_session())
    assert len(server.calls) == 1
    call = server.calls[0]
    assert (call['method'], call['path']) == ('PUT', '/sessions/31')
    assert call['body'] == {'guild_id': 31, 'voice_channel_id': 32,
                            'text_channel_id': 33, 'was_playing': True}


@pytest.mark.asyncio
async def test_save_swallows_a_missing_route(caplog):
    '''Shutdown must not abort because the broker predates /sessions: a 404 is logged and dropped.'''
    server = ScriptedServer(SESSIONS)
    server.script(broker_routes.SAVE_SESSION, ok(None, status=404))
    async with serve(server) as (url, http):
        assert await HttpBrokerClient(url, session=http).save_player_session(_session()) is None
    assert '/sessions/{guild_id}' in caplog.text


@pytest.mark.asyncio
async def test_save_raises_any_other_client_error():
    '''A 403 is not deploy skew and is raised to the caller.'''
    server = ScriptedServer(SESSIONS)
    server.script(broker_routes.SAVE_SESSION, ok(None, status=403))
    async with serve(server) as (url, http):
        with pytest.raises(aiohttp.ClientResponseError) as caught:
            await HttpBrokerClient(url, session=http).save_player_session(_session())
    assert caught.value.status == 403


@pytest.mark.asyncio
async def test_list_parses_every_stored_session():
    '''list_player_sessions GETs /sessions and returns PlayerSession objects in order.'''
    server = ScriptedServer(SESSIONS)
    server.script(broker_routes.LIST_SESSIONS, ok({'sessions': [
        _session(1).model_dump(), _session(2).model_dump(),
    ]}))
    async with serve(server) as (url, http):
        sessions = await HttpBrokerClient(url, session=http).list_player_sessions()
    assert sessions == [_session(1), _session(2)]
    assert (server.calls[0]['method'], server.calls[0]['path']) == ('GET', '/sessions')


@pytest.mark.asyncio
async def test_list_ignores_the_retired_queue_key():
    '''A session saved before the queue moved to the broker still loads; the old key is dropped.'''
    server = ScriptedServer(SESSIONS)
    legacy = dict(_session(5).model_dump(), queue=[{'old': 'shape'}])
    server.script(broker_routes.LIST_SESSIONS, ok({'sessions': [legacy]}))
    async with serve(server) as (url, http):
        assert await HttpBrokerClient(url, session=http).list_player_sessions() == [_session(5)]


@pytest.mark.asyncio
@pytest.mark.parametrize('answer', [None, {}])
async def test_list_with_nothing_stored_is_empty(answer):
    '''An empty body or one without sessions means nothing to resume.'''
    server = ScriptedServer(SESSIONS)
    server.script(broker_routes.LIST_SESSIONS, ok(answer))
    async with serve(server) as (url, http):
        assert await HttpBrokerClient(url, session=http).list_player_sessions() == []


@pytest.mark.asyncio
async def test_list_reads_a_missing_route_as_no_sessions(caplog):
    '''A bot newer than its broker starts with nothing to resume instead of failing init.'''
    server = ScriptedServer(SESSIONS)
    server.script(broker_routes.LIST_SESSIONS, ok(None, status=404))
    async with serve(server) as (url, http):
        assert await HttpBrokerClient(url, session=http).list_player_sessions() == []
    assert 'peer not upgraded' in caplog.text


@pytest.mark.asyncio
async def test_list_raises_other_client_errors():
    '''Only a 404 is tolerated; a 401 propagates.'''
    server = ScriptedServer(SESSIONS)
    server.script(broker_routes.LIST_SESSIONS, ok(None, status=401))
    async with serve(server) as (url, http):
        with pytest.raises(aiohttp.ClientResponseError) as caught:
            await HttpBrokerClient(url, session=http).list_player_sessions()
    assert caught.value.status == 401


@pytest.mark.asyncio
async def test_list_rejects_a_session_this_build_cannot_parse():
    '''A stored session missing a required field is a seam error, not a silently shorter list.'''
    server = ScriptedServer(SESSIONS)
    server.script(broker_routes.LIST_SESSIONS, ok({'sessions': [{'guild_id': 1}]}))
    async with serve(server) as (url, http):
        with pytest.raises(SeamResponseInvalid):
            await HttpBrokerClient(url, session=http).list_player_sessions()


@pytest.mark.asyncio
async def test_delete_issues_a_delete_for_the_guild():
    '''delete_player_session sends DELETE /sessions/{guild_id}.'''
    server = ScriptedServer(SESSIONS)
    server.script(broker_routes.DELETE_SESSION, ok({'status': 'ok'}))
    async with serve(server) as (url, http):
        await HttpBrokerClient(url, session=http).delete_player_session(31)
    assert len(server.calls) == 1
    call = server.calls[0]
    assert (call['method'], call['path'], call['body']) == ('DELETE', '/sessions/31', None)


@pytest.mark.asyncio
async def test_delete_treats_a_missing_route_as_a_no_op(caplog):
    '''Deleting from a broker without the route is harmless and is logged, not raised.'''
    server = ScriptedServer(SESSIONS)
    server.script(broker_routes.DELETE_SESSION, ok(None, status=404))
    async with serve(server) as (url, http):
        assert await HttpBrokerClient(url, session=http).delete_player_session(31) is None
    assert 'peer not upgraded' in caplog.text


@pytest.mark.asyncio
async def test_delete_raises_other_client_errors():
    '''A 400 on delete is a real error.'''
    server = ScriptedServer(SESSIONS)
    server.script(broker_routes.DELETE_SESSION, ok(None, status=400))
    async with serve(server) as (url, http):
        with pytest.raises(aiohttp.ClientResponseError) as caught:
            await HttpBrokerClient(url, session=http).delete_player_session(31)
    assert caught.value.status == 400
