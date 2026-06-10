'''
Tests for HttpDispatchClient — tested against a real DispatchHttpServer.
'''
import asyncio

import aiohttp
import pytest
from aiohttp.test_utils import TestClient, TestServer

from discord_bot.servers.dispatch_server import DispatchHttpServer
from discord_bot.types.dispatch_request import (
    DeleteRequest,
    FetchChannelHistoryRequest,
    FetchGuildEmojisRequest,
    SendRequest,
)
from discord_bot.types.dispatch_result import ChannelHistoryResult, GuildEmojisResult
from discord_bot.clients.dispatch_client_base import DispatchRemoteError
from discord_bot.clients.http_dispatch_client import HttpDispatchClient
from tests.helpers import FakeDispatchServer, FakeRedisDispatchQueue


def _make_setup():
    result_store: dict = {}
    dispatcher = FakeDispatchServer(result_store)
    redis_queue = FakeRedisDispatchQueue(result_store)
    server = DispatchHttpServer(dispatcher, redis_queue)
    return dispatcher, server


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_start_stop_are_noops():
    '''start() and stop() are no-ops — no background tasks to manage.'''
    client = HttpDispatchClient('http://localhost:9999')
    await client.start()
    client.stop()


@pytest.mark.asyncio
async def test_close_session():
    '''close() closes an externally-supplied session.'''
    session = aiohttp.ClientSession()
    client = HttpDispatchClient('http://localhost:9999', session=session)
    await client.close()
    assert session.closed


@pytest.mark.asyncio
async def test_lazy_session_creation():
    '''_get_session() creates a session on first call when none was supplied.'''
    client = HttpDispatchClient('http://localhost:9999')
    session = client._get_session()  # pylint:disable=protected-access
    assert session is not None
    await client.close()


# ---------------------------------------------------------------------------
# register_cog_queue
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_register_cog_queue_returns_queue():
    '''register_cog_queue returns an asyncio.Queue for the named cog.'''
    client = HttpDispatchClient('http://localhost:9999')
    q = client.register_cog_queue('my_cog')
    assert isinstance(q, asyncio.Queue)


# ---------------------------------------------------------------------------
# Fire-and-forget methods
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_send_message_posts_to_server():
    '''send_message fire-and-forget POSTs to /dispatch/send on the server.'''
    dispatcher, server = _make_setup()
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpDispatchClient(str(tc.make_url('')), session=tc.session)
        client.send_message(1, 2, 'hello')
        await asyncio.sleep(0.1)  # let the fire-and-forget task execute
    assert any(c[0] == 'send_message' for c in dispatcher.calls)


@pytest.mark.asyncio
async def test_delete_message_posts_to_server():
    '''delete_message fire-and-forget POSTs to /dispatch/delete on the server.'''
    dispatcher, server = _make_setup()
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpDispatchClient(str(tc.make_url('')), session=tc.session)
        client.delete_message(1, 2, 999)
        await asyncio.sleep(0.1)
    assert any(c[0] == 'delete_message' for c in dispatcher.calls)


@pytest.mark.asyncio
async def test_update_mutable_posts_to_server():
    '''update_mutable fire-and-forget POSTs to /dispatch/update_mutable on the server.'''
    dispatcher, server = _make_setup()
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpDispatchClient(str(tc.make_url('')), session=tc.session)
        client.update_mutable('k', 1, ['msg'], 2)
        await asyncio.sleep(0.1)
    assert any(c[0] == 'update_mutable' for c in dispatcher.calls)


@pytest.mark.asyncio
async def test_update_mutable_empty_content_routes_to_remove():
    '''update_mutable with empty content routes to remove_mutable instead.'''
    dispatcher, server = _make_setup()
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpDispatchClient(str(tc.make_url('')), session=tc.session)
        client.update_mutable('k', 1, [], 2)
        await asyncio.sleep(0.1)
    assert any(c[0] == 'remove_mutable' for c in dispatcher.calls)


@pytest.mark.asyncio
async def test_remove_mutable_posts_to_server():
    '''remove_mutable fire-and-forget POSTs to /dispatch/remove_mutable on the server.'''
    dispatcher, server = _make_setup()
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpDispatchClient(str(tc.make_url('')), session=tc.session)
        client.remove_mutable('k')
        await asyncio.sleep(0.1)
    assert any(c[0] == 'remove_mutable' for c in dispatcher.calls)


@pytest.mark.asyncio
async def test_update_mutable_channel_posts_to_server():
    '''update_mutable_channel fire-and-forget POSTs to /dispatch/update_mutable_channel.'''
    dispatcher, server = _make_setup()
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpDispatchClient(str(tc.make_url('')), session=tc.session)
        client.update_mutable_channel('k', 1, 99)
        await asyncio.sleep(0.1)
    assert any(c[0] == 'update_mutable_channel' for c in dispatcher.calls)


# ---------------------------------------------------------------------------
# submit_request routing
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_submit_request_send():
    '''submit_request routes a SendRequest to send_message on the server.'''
    dispatcher, server = _make_setup()
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpDispatchClient(str(tc.make_url('')), session=tc.session)
        await client.submit_request(SendRequest(
            guild_id=1, channel_id=2, content='hi',
        ))
        await asyncio.sleep(0.1)
    assert any(c[0] == 'send_message' for c in dispatcher.calls)


@pytest.mark.asyncio
async def test_submit_request_delete():
    '''submit_request routes a DeleteRequest to delete_message on the server.'''
    dispatcher, server = _make_setup()
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpDispatchClient(str(tc.make_url('')), session=tc.session)
        await client.submit_request(DeleteRequest(
            guild_id=1, channel_id=2, message_id=3,
        ))
        await asyncio.sleep(0.1)
    assert any(c[0] == 'delete_message' for c in dispatcher.calls)


# ---------------------------------------------------------------------------
# Awaitable fetch via submit_request
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_submit_request_fetch_history_delivers_to_cog_queue():
    '''FetchChannelHistoryRequest submission delivers a ChannelHistoryResult to the cog queue.'''
    _, server = _make_setup()
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpDispatchClient(str(tc.make_url('')), session=tc.session)
        q = client.register_cog_queue('my_cog')
        await client.submit_request(FetchChannelHistoryRequest(
            cog_name='my_cog', guild_id=1, channel_id=2, limit=50,
        ))
        result = await asyncio.wait_for(q.get(), timeout=5.0)
    assert isinstance(result, ChannelHistoryResult)
    assert result.guild_id == 1
    assert result.channel_id == 2


@pytest.mark.asyncio
async def test_submit_request_fetch_emojis_delivers_to_cog_queue():
    '''FetchGuildEmojisRequest submission delivers a GuildEmojisResult to the cog queue.'''
    _, server = _make_setup()
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpDispatchClient(str(tc.make_url('')), session=tc.session)
        q = client.register_cog_queue('my_cog')
        await client.submit_request(FetchGuildEmojisRequest(
            cog_name='my_cog', guild_id=1,
        ))
        result = await asyncio.wait_for(q.get(), timeout=5.0)
    assert isinstance(result, GuildEmojisResult)
    assert result.guild_id == 1


@pytest.mark.asyncio
async def test_fetch_history_error_delivers_error_result_to_queue():
    '''If the result payload contains an error key, a ChannelHistoryResult with error is delivered.'''
    result_store: dict = {}
    dispatcher = FakeDispatchServer(result_store)

    # Override enqueue to store an error result instead
    async def _enqueue_error(request_id, *_args, **_kwargs):
        result_store[request_id] = {'error': 'something went wrong'}

    dispatcher.enqueue_fetch_history = _enqueue_error

    redis_queue = FakeRedisDispatchQueue(result_store)
    server = DispatchHttpServer(dispatcher, redis_queue)
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpDispatchClient(str(tc.make_url('')), session=tc.session)
        q = client.register_cog_queue('my_cog')
        await client.submit_request(FetchChannelHistoryRequest(
            cog_name='my_cog', guild_id=1, channel_id=2, limit=10,
        ))
        result = await asyncio.wait_for(q.get(), timeout=5.0)
    assert isinstance(result, ChannelHistoryResult)
    assert result.error is not None


@pytest.mark.asyncio
async def test_fetch_emojis_error_delivers_error_result_to_queue():
    '''If fetch_emojis result payload contains an error key, a GuildEmojisResult with error is delivered.'''
    result_store: dict = {}
    dispatcher = FakeDispatchServer(result_store)

    async def _enqueue_error(request_id, *_args, **_kwargs):
        result_store[request_id] = {'error': 'emoji fetch failed'}

    dispatcher.enqueue_fetch_emojis = _enqueue_error

    redis_queue = FakeRedisDispatchQueue(result_store)
    server = DispatchHttpServer(dispatcher, redis_queue)
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpDispatchClient(str(tc.make_url('')), session=tc.session)
        q = client.register_cog_queue('my_cog')
        await client.submit_request(FetchGuildEmojisRequest(
            cog_name='my_cog', guild_id=1,
        ))
        result = await asyncio.wait_for(q.get(), timeout=5.0)
    assert isinstance(result, GuildEmojisResult)
    assert result.error is not None


@pytest.mark.asyncio
async def test_http_returns_none_for_non_json_response(mocker):
    '''_http returns None when the response content-type is not application/json.'''
    mock_resp = mocker.AsyncMock()
    mock_resp.content_type = 'text/plain'
    mock_resp.raise_for_status = mocker.Mock()
    mock_resp.__aenter__ = mocker.AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = mocker.AsyncMock(return_value=False)
    mock_session = mocker.MagicMock()
    mock_session.closed = False
    mock_session.request = mocker.MagicMock(return_value=mock_resp)
    client = HttpDispatchClient('http://localhost:9999')
    client._session = mock_session  # pylint: disable=protected-access
    result = await client._http('POST', 'http://localhost:9999/test', {})  # pylint: disable=protected-access
    assert result is None


@pytest.mark.asyncio
async def test_post_failure_logs_error_and_does_not_raise(mocker):
    '''_post swallows and logs exceptions so fire-and-forget callers are not affected.'''
    mocker.patch(
        'discord_bot.clients.http_dispatch_client.async_retry_broker_command',
        side_effect=RuntimeError('connection refused'),
    )
    client = HttpDispatchClient('http://localhost:9999')
    # Should not raise even though the underlying call failed
    await client._post('/dispatch/send', {'guild_id': 1, 'channel_id': 2, 'content': 'hi'})  # pylint: disable=protected-access
    await client.close()


@pytest.mark.asyncio
async def test_poll_result_raises_dispatch_remote_error_on_timeout(mocker):
    '''_poll_result raises DispatchRemoteError after _POLL_TIMEOUT elapses with no result.'''
    mocker.patch('discord_bot.clients.http_dispatch_client._POLL_TIMEOUT', 0)
    _, server = _make_setup()
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpDispatchClient(str(tc.make_url('')), session=tc.session)
        with pytest.raises(DispatchRemoteError, match='poll timeout'):
            await client._poll_result('nonexistent-id')  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_poll_result_sleeps_and_retries_until_result_available(mocker):
    '''_poll_result retries with backoff (lines 194-195) when the result is not yet ready.'''
    mocker.patch('discord_bot.clients.http_dispatch_client._POLL_INTERVAL_BASE', 0)

    result_store: dict = {}

    class _DelayedQueue:
        '''Returns None on the first call, then the result on subsequent calls.'''

        def __init__(self):
            self._count = 0

        async def get_result(self, _request_id):
            '''Return None until the second call, then the ready result.'''
            self._count += 1
            return result_store.get('ready') if self._count >= 2 else None

    dispatcher = FakeDispatchServer(result_store)
    server = DispatchHttpServer(dispatcher, _DelayedQueue())
    result_store['ready'] = {'guild_id': 7, 'channel_id': 8, 'messages': []}

    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpDispatchClient(str(tc.make_url('')), session=tc.session)
        payload = await client._poll_result('ready')  # pylint: disable=protected-access

    assert payload == {'guild_id': 7, 'channel_id': 8, 'messages': []}
