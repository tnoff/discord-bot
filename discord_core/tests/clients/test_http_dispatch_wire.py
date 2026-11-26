'''HttpDispatchClient against a scripted dispatcher: sends, fetch polling, errors and the breaker.'''
import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

from discord_core.clients import http_dispatch_client
from discord_core.clients.dispatch_client_base import DispatchRemoteError
from discord_core.clients.http_client_base import SeamResponseInvalid
from discord_core.clients.http_dispatch_client import HttpDispatchClient
from discord_core.routes import dispatch as dispatch_routes
from discord_core.tests.clients.stub_broker import serve
from discord_core.tests.clients.stub_http import ScriptedServer, ok, recording_tracer
from discord_core.types.dispatch_request import (
    DeleteRequest, FetchChannelHistoryRequest, FetchGuildEmojisRequest, SendRequest,
)
from discord_core.types.dispatch_result import is_not_found_error
from discord_core.utils.circuit_breaker import CircuitBreaker, CircuitBreakerOpenError
from discord_core.utils.otel import AttributeNaming

D = dispatch_routes
MESSAGE = {'id': 900, 'content': 'hello there', 'created_at': '2026-01-02T03:04:05+00:00', 'author_bot': False}


class _OpenAfter:
    '''A breaker stand-in that lets a fixed number of calls through, then reports itself open.'''

    def __init__(self, allowed: int):
        self.allowed = allowed

    async def call(self, func):
        '''Run func while calls remain, otherwise refuse like an open breaker.'''
        if self.allowed <= 0:
            raise CircuitBreakerOpenError('dispatch is open')
        self.allowed -= 1
        return await func()


@pytest.fixture(name='spawned')
def fixture_spawned(mocker):
    '''A function returning the tasks the dispatch clients have spawned so far.'''
    real_create_task = asyncio.create_task
    tasks = []

    def spy(coro, **kwargs):
        task = real_create_task(coro, **kwargs)
        if coro.__qualname__.split('.')[0] in ('HttpDispatchClient', 'DispatchClientBase'):
            tasks.append(task)
        return task

    mocker.patch('asyncio.create_task', side_effect=spy)
    return lambda: list(tasks)


@pytest.fixture(name='counter')
def fixture_counter(mocker):
    '''Replace the request counter so the recorded outcomes can be read back.'''
    return mocker.patch.object(http_dispatch_client, '_REQUEST_COUNTER', MagicMock())


@pytest.fixture(name='no_sleep')
def fixture_no_sleep(mocker):
    '''Make the retry ladder instant.'''
    return mocker.patch('discord_core.utils.retry.async_sleep', new=AsyncMock())


@pytest.fixture(name='quick_polls')
def fixture_quick_polls(mocker):
    '''Shrink the poll cadence and give-up time so polling tests finish in milliseconds.'''
    mocker.patch.object(http_dispatch_client, '_POLL_INTERVAL_BASE', 0.005)
    mocker.patch.object(http_dispatch_client, '_POLL_INTERVAL_MAX', 0.01)
    mocker.patch.object(http_dispatch_client, '_POLL_TIMEOUT', 0.1)


def _outcomes(counter) -> list:
    '''The (outcome, path) pairs the counter was incremented with, in order.'''
    return [(call.args[1][AttributeNaming.OUTCOME.value], call.args[1][AttributeNaming.PATH.value])
            for call in counter.add.call_args_list]


def _server() -> ScriptedServer:
    '''A scripted dispatcher serving the whole dispatch seam.'''
    return ScriptedServer(D.ALL)


def _history_request(**overrides) -> FetchChannelHistoryRequest:
    '''A history fetch for the cog named "cog".'''
    fields = {'guild_id': 1, 'channel_id': 2, 'limit': 5, 'cog_name': 'cog'}
    fields.update(overrides)
    return FetchChannelHistoryRequest(**fields)


# ---------------------------------------------------------------------------
# fire-and-forget calls
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_start_and_stop_do_nothing(counter):
    '''There is no background poller, so start and stop are inert.'''
    client = HttpDispatchClient('http://dispatcher')
    assert await client.start() is None
    assert client.stop() is None
    counter.add.assert_not_called()


@pytest.mark.asyncio
async def test_send_message_posts_the_full_body(spawned, counter):
    '''send_message POSTs every field, including allow_404, and counts a success.'''
    server = _server()
    server.script(D.SEND, ok({'status': 'ok'}, status=202))
    async with serve(server) as (url, session):
        HttpDispatchClient(url, session=session).send_message(
            1, 2, 'hi', delete_after=30, allow_404=True, span_context={'k': 'v'})
        await asyncio.gather(*spawned())
    assert len(server.calls) == 1
    call = server.calls[0]
    assert (call['method'], call['path']) == ('POST', '/dispatch/send')
    assert call['body'] == {'guild_id': 1, 'channel_id': 2, 'content': 'hi', 'delete_after': 30,
                            'allow_404': True, 'span_context': {'k': 'v'}}
    assert _outcomes(counter) == [('success', '/dispatch/send')]


@pytest.mark.asyncio
async def test_delete_message_posts_the_target(spawned):
    '''delete_message POSTs the guild, channel and message id.'''
    server = _server()
    server.script(D.DELETE, ok({'status': 'ok'}, status=202))
    async with serve(server) as (url, session):
        HttpDispatchClient(url, session=session).delete_message(1, 2, 3)
        await asyncio.gather(*spawned())
    assert server.calls[0]['path'] == '/dispatch/delete'
    assert server.calls[0]['body'] == {'guild_id': 1, 'channel_id': 2, 'message_id': 3, 'span_context': None}


@pytest.mark.asyncio
async def test_submit_request_routes_a_send_and_a_delete(spawned):
    '''Typed SendRequest and DeleteRequest objects become the matching POSTs.'''
    server = _server()
    server.script(D.SEND, ok({'status': 'ok'}, status=202))
    server.script(D.DELETE, ok({'status': 'ok'}, status=202))
    async with serve(server) as (url, session):
        client = HttpDispatchClient(url, session=session)
        await client.submit_request(SendRequest(guild_id=1, channel_id=2, content='yo', delete_after=9))
        await client.submit_request(DeleteRequest(guild_id=1, channel_id=2, message_id=4))
        await asyncio.gather(*spawned())
    assert server.calls_to(D.SEND)[0]['body'] == {
        'guild_id': 1, 'channel_id': 2, 'content': 'yo', 'delete_after': 9,
        'allow_404': False, 'span_context': None}
    assert server.calls_to(D.DELETE)[0]['body']['message_id'] == 4


@pytest.mark.asyncio
async def test_update_mutable_returns_a_request_id_and_posts(spawned):
    '''update_mutable posts the content and hands back a generated request id.'''
    server = _server()
    server.script(D.UPDATE_MUTABLE, ok({'status': 'ok'}, status=202))
    async with serve(server) as (url, session):
        request_id = HttpDispatchClient(url, session=session).update_mutable(
            'queue', 8, ['line one', 'line two'], 9, sticky=False, delete_after=60)
        await asyncio.gather(*spawned())
    assert isinstance(request_id, str) and request_id
    assert server.calls[0]['path'] == '/dispatch/update_mutable'
    assert server.calls[0]['body'] == {'key': 'queue', 'guild_id': 8, 'content': ['line one', 'line two'],
                                       'channel_id': 9, 'sticky': False, 'delete_after': 60}


@pytest.mark.asyncio
async def test_update_mutable_with_no_content_removes_the_mutable(spawned):
    '''An empty content list is a removal, not an update, and returns no request id.'''
    server = _server()
    server.script(D.REMOVE_MUTABLE, ok({'status': 'ok'}, status=202))
    async with serve(server) as (url, session):
        assert HttpDispatchClient(url, session=session).update_mutable('queue', 8, [], 9) is None
        await asyncio.gather(*spawned())
    assert not server.calls_to(D.UPDATE_MUTABLE)
    assert server.calls[0]['path'] == '/dispatch/remove_mutable'
    assert server.calls[0]['body'] == {'key': 'queue'}


@pytest.mark.asyncio
async def test_update_mutable_channel_posts_the_new_channel(spawned):
    '''update_mutable_channel moves a mutable by key.'''
    server = _server()
    server.script(D.UPDATE_MUTABLE_CHANNEL, ok({'status': 'ok'}, status=202))
    async with serve(server) as (url, session):
        HttpDispatchClient(url, session=session).update_mutable_channel('queue', 8, 77)
        await asyncio.gather(*spawned())
    assert server.calls[0]['path'] == '/dispatch/update_mutable_channel'
    assert server.calls[0]['body'] == {'key': 'queue', 'guild_id': 8, 'new_channel_id': 77}


@pytest.mark.asyncio
async def test_a_failed_post_is_logged_and_swallowed(spawned, counter, caplog):
    '''A 4xx on a fire-and-forget call never reaches the caller; it is logged and counted.'''
    server = _server()
    server.script(D.SEND, ok({'detail': 'bad'}, status=400))
    async with serve(server) as (url, session):
        HttpDispatchClient(url, session=session).send_message(1, 2, 'hi')
        await asyncio.gather(*spawned())
    assert 'POST /dispatch/send failed' in caplog.text
    assert _outcomes(counter) == [('failure', '/dispatch/send')]


@pytest.mark.asyncio
async def test_a_post_is_retried_through_a_server_error(spawned, no_sleep, counter):
    '''A 503 is retried on the broker ladder and the call still counts as a success.'''
    server = _server()
    server.script(D.SEND, ok(None, status=503), ok({'status': 'ok'}, status=202))
    async with serve(server) as (url, session):
        HttpDispatchClient(url, session=session).send_message(1, 2, 'hi')
        await asyncio.gather(*spawned())
    assert len(server.calls) == 2
    assert no_sleep.await_count == 1
    assert _outcomes(counter) == [('success', '/dispatch/send')]


@pytest.mark.asyncio
async def test_an_open_breaker_drops_the_post_without_a_request(mocker, spawned, counter, caplog):
    '''With the breaker open nothing is sent; the drop is logged and counted.'''
    mocker.patch.object(http_dispatch_client, '_BREAKER', _OpenAfter(0))
    server = _server()
    async with serve(server) as (url, session):
        HttpDispatchClient(url, session=session).send_message(1, 2, 'hi')
        await asyncio.gather(*spawned())
    assert not server.calls
    assert 'breaker open, dropping POST /dispatch/send' in caplog.text
    assert _outcomes(counter) == [('breaker_open', '/dispatch/send')]


@pytest.mark.asyncio
async def test_repeated_failures_trip_the_real_breaker(mocker, spawned, no_sleep):
    '''Two consecutive failed sends open a threshold-2 breaker and the third send is never made.'''
    mocker.patch.object(http_dispatch_client, '_BREAKER', CircuitBreaker('t', failure_threshold=2))
    server = _server()
    server.script(D.SEND, ok(None, status=400))
    async with serve(server) as (url, session):
        client = HttpDispatchClient(url, session=session)
        for _ in range(3):
            client.send_message(1, 2, 'hi')
            await asyncio.gather(*spawned())
    assert len(server.calls) == 2
    assert no_sleep.await_count == 0


@pytest.mark.asyncio
async def test_posts_carry_the_callers_trace(mocker, spawned):
    '''A fire-and-forget call made inside a span forwards that span's trace id.'''
    tracer, _ = recording_tracer()
    mocker.patch('discord_core.utils.otel.TRACER', tracer)
    server = _server()
    server.script(D.DELETE, ok({'status': 'ok'}, status=202))
    async with serve(server) as (url, session):
        with tracer.start_as_current_span('caller') as span:
            HttpDispatchClient(url, session=session).delete_message(1, 2, 3)
            expected = format(span.get_span_context().trace_id, '032x')
        await asyncio.gather(*spawned())
    assert server.calls[0]['traceparent'].split('-')[1] == expected


# ---------------------------------------------------------------------------
# awaitable fetches
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.usefixtures('quick_polls')
async def test_history_fetch_polls_until_the_result_is_ready(spawned, counter):
    '''A 202 pending is waited out; the 200 body is decoded and delivered to the cog queue.'''
    server = _server()
    server.script(D.FETCH_HISTORY, ok({'request_id': 'req-1'}, status=202))
    server.script(D.GET_RESULT, ok({'status': 'pending'}, status=202), ok({'status': 'pending'}, status=202),
                  ok({'guild_id': 1, 'channel_id': 2, 'after_message_id': 50, 'messages': [MESSAGE]}))
    async with serve(server) as (url, session):
        client = HttpDispatchClient(url, session=session)
        queue = client.register_cog_queue('cog')
        after = datetime(2026, 1, 1, tzinfo=timezone.utc)
        await client.submit_request(_history_request(after=after, after_message_id=50, oldest_first=False,
                                                     span_context={'trace': 'x'}))
        await asyncio.gather(*spawned())
    result = queue.get_nowait()
    assert (result.guild_id, result.channel_id, result.after_message_id) == (1, 2, 50)
    assert [m.content for m in result.messages] == ['hello there']
    assert result.error is None
    assert result.span_context == {'trace': 'x'}
    submitted = server.calls_to(D.FETCH_HISTORY)[0]['body']
    assert submitted == {'guild_id': 1, 'channel_id': 2, 'limit': 5, 'after': after.isoformat(),
                         'after_message_id': 50, 'oldest_first': False, 'span_context': {'trace': 'x'}}
    polls = server.calls_to(D.GET_RESULT)
    assert len(polls) == 3
    assert {call['path'] for call in polls} == {'/dispatch/results/req-1'}
    assert _outcomes(counter) == [('success', '/dispatch/fetch_history'), ('success', '/dispatch/results')]


@pytest.mark.asyncio
@pytest.mark.usefixtures('quick_polls')
async def test_emoji_fetch_delivers_the_emojis(spawned):
    '''fetch_emojis submits, polls and delivers a GuildEmojisResult.'''
    server = _server()
    emoji = {'id': 5, 'name': 'party', 'animated': True}
    server.script(D.FETCH_EMOJIS, ok({'request_id': 'req-2'}, status=202))
    server.script(D.GET_RESULT, ok({'guild_id': 1, 'emojis': [emoji]}))
    async with serve(server) as (url, session):
        client = HttpDispatchClient(url, session=session)
        queue = client.register_cog_queue('cog')
        await client.submit_request(FetchGuildEmojisRequest(cog_name='cog', guild_id=1, span_context={'a': 1}))
        await asyncio.gather(*spawned())
    result = queue.get_nowait()
    assert (result.guild_id, result.emojis, result.error) == (1, [emoji], None)
    assert result.span_context == {'a': 1}
    assert server.calls_to(D.FETCH_EMOJIS)[0]['body'] == {'guild_id': 1, 'max_retries': 3, 'span_context': {'a': 1}}


@pytest.mark.asyncio
@pytest.mark.usefixtures('quick_polls')
async def test_a_remote_error_result_is_delivered_with_its_status(spawned):
    '''An error body becomes a DispatchRemoteError carrying the remote status and code.'''
    server = _server()
    server.script(D.FETCH_HISTORY, ok({'request_id': 'req-3'}, status=202))
    server.script(D.GET_RESULT, ok({'error': 'Unknown Message', 'error_detail': {
        'message': 'Unknown Message', 'type': 'NotFound', 'status': 404, 'code': 10008}}))
    async with serve(server) as (url, session):
        client = HttpDispatchClient(url, session=session)
        queue = client.register_cog_queue('cog')
        await client.submit_request(_history_request(after_message_id=7))
        await asyncio.gather(*spawned())
    result = queue.get_nowait()
    assert isinstance(result.error, DispatchRemoteError)
    assert (result.error.status, result.error.code, result.error.error_type) == (404, 10008, 'NotFound')
    assert is_not_found_error(result.error)
    assert result.messages == []
    assert result.after_message_id == 7


@pytest.mark.asyncio
@pytest.mark.usefixtures('quick_polls')
async def test_a_remote_error_without_detail_still_delivers(spawned):
    '''An error from a dispatcher that sends no detail degrades to the message alone.'''
    server = _server()
    server.script(D.FETCH_EMOJIS, ok({'request_id': 'req-4'}, status=202))
    server.script(D.GET_RESULT, ok({'error': 'boom'}))
    async with serve(server) as (url, session):
        client = HttpDispatchClient(url, session=session)
        queue = client.register_cog_queue('cog')
        await client.submit_request(FetchGuildEmojisRequest(cog_name='cog', guild_id=1))
        await asyncio.gather(*spawned())
    result = queue.get_nowait()
    assert str(result.error) == 'boom'
    assert (result.error.status, result.error.code, result.error.error_type) == (None, None, None)
    assert result.emojis == []


@pytest.mark.asyncio
@pytest.mark.usefixtures('quick_polls')
async def test_polling_gives_up_after_the_timeout(spawned, counter):
    '''A result that never completes ends as a delivered poll-timeout error and a timeout count.'''
    server = _server()
    server.script(D.FETCH_HISTORY, ok({'request_id': 'req-5'}, status=202))
    server.script(D.GET_RESULT, ok({'status': 'pending'}, status=202))
    async with serve(server) as (url, session):
        client = HttpDispatchClient(url, session=session)
        queue = client.register_cog_queue('cog')
        await client.submit_request(_history_request())
        await asyncio.gather(*spawned())
    result = queue.get_nowait()
    assert str(result.error) == 'poll timeout for request_id=req-5'
    assert len(server.calls_to(D.GET_RESULT)) >= 3
    assert _outcomes(counter)[-1] == ('timeout', '/dispatch/results')


@pytest.mark.asyncio
@pytest.mark.usefixtures('no_sleep')
async def test_a_failed_submit_is_raised_after_retries(spawned, counter):
    '''A persistently failing submit exhausts the ladder and fails the request task.'''
    server = _server()
    server.script(D.FETCH_HISTORY, ok(None, status=500))
    async with serve(server) as (url, session):
        client = HttpDispatchClient(url, session=session)
        queue = client.register_cog_queue('cog')
        await client.submit_request(_history_request())
        task = spawned()[0]
        with pytest.raises(aiohttp.ClientResponseError) as caught:
            await task
    assert caught.value.status == 500
    assert len(server.calls) == 4
    assert queue.empty()
    assert _outcomes(counter) == [('failure', '/dispatch/fetch_history')]


@pytest.mark.asyncio
async def test_a_submit_reply_without_a_request_id_is_a_seam_error(spawned):
    '''A 202 that does not carry a request_id raises instead of polling for nothing.'''
    server = _server()
    server.script(D.FETCH_EMOJIS, ok({'unexpected': True}, status=202))
    async with serve(server) as (url, session):
        client = HttpDispatchClient(url, session=session)
        client.register_cog_queue('cog')
        await client.submit_request(FetchGuildEmojisRequest(cog_name='cog', guild_id=1))
        task = spawned()[0]
        with pytest.raises(SeamResponseInvalid):
            await task
    assert not server.calls_to(D.GET_RESULT)


@pytest.mark.asyncio
async def test_a_submit_is_refused_while_the_breaker_is_open(mocker, spawned, counter):
    '''An open breaker fails the submit at once with the breaker error and counts it.'''
    mocker.patch.object(http_dispatch_client, '_BREAKER', _OpenAfter(0))
    server = _server()
    async with serve(server) as (url, session):
        client = HttpDispatchClient(url, session=session)
        client.register_cog_queue('cog')
        await client.submit_request(_history_request())
        task = spawned()[0]
        with pytest.raises(CircuitBreakerOpenError):
            await task
    assert not server.calls
    assert _outcomes(counter) == [('breaker_open', '/dispatch/fetch_history')]


@pytest.mark.asyncio
@pytest.mark.usefixtures('quick_polls')
async def test_a_poll_failure_is_raised_and_counted(spawned, counter, no_sleep):
    '''A server error while polling surfaces from the request task and counts a failure.'''
    server = _server()
    server.script(D.FETCH_HISTORY, ok({'request_id': 'req-6'}, status=202))
    server.script(D.GET_RESULT, ok(None, status=502))
    async with serve(server) as (url, session):
        client = HttpDispatchClient(url, session=session)
        client.register_cog_queue('cog')
        await client.submit_request(_history_request())
        task = spawned()[0]
        with pytest.raises(aiohttp.ClientResponseError) as caught:
            await task
    assert caught.value.status == 502
    assert _outcomes(counter)[-1] == ('failure', '/dispatch/results')
    assert no_sleep.await_count == 3


@pytest.mark.asyncio
@pytest.mark.usefixtures('quick_polls')
async def test_the_breaker_opening_mid_poll_is_raised(mocker, spawned, counter):
    '''If the breaker opens between submit and poll, the poll raises and counts breaker_open.'''
    mocker.patch.object(http_dispatch_client, '_BREAKER', _OpenAfter(1))
    server = _server()
    server.script(D.FETCH_EMOJIS, ok({'request_id': 'req-7'}, status=202))
    async with serve(server) as (url, session):
        client = HttpDispatchClient(url, session=session)
        client.register_cog_queue('cog')
        await client.submit_request(FetchGuildEmojisRequest(cog_name='cog', guild_id=1))
        task = spawned()[0]
        with pytest.raises(CircuitBreakerOpenError):
            await task
    assert _outcomes(counter) == [('success', '/dispatch/fetch_emojis'), ('breaker_open', '/dispatch/results')]


@pytest.mark.asyncio
@pytest.mark.parametrize('route,request_id,body', [
    (D.FETCH_HISTORY, 'req-8', {'guild_id': 1, 'messages': []}),
    (D.FETCH_EMOJIS, 'req-9', {'guild_id': 1, 'emojis': [{'id': 'not-a-number'}]}),
])
@pytest.mark.usefixtures('quick_polls')
async def test_a_result_that_does_not_fit_its_model_is_a_seam_error(spawned, route, request_id, body):
    '''A 200 result whose shape drifted raises a seam error rather than a KeyError in a cog.'''
    server = _server()
    server.script(route, ok({'request_id': request_id}, status=202))
    server.script(D.GET_RESULT, ok(body))
    async with serve(server) as (url, session):
        client = HttpDispatchClient(url, session=session)
        client.register_cog_queue('cog')
        request = (_history_request() if route is D.FETCH_HISTORY
                   else FetchGuildEmojisRequest(cog_name='cog', guild_id=1))
        await client.submit_request(request)
        task = spawned()[0]
        with pytest.raises(SeamResponseInvalid):
            await task


@pytest.mark.asyncio
@pytest.mark.usefixtures('quick_polls')
async def test_a_malformed_error_body_is_a_seam_error(spawned):
    '''An error body that is not itself well formed is rejected before it is raised remotely.'''
    server = _server()
    server.script(D.FETCH_HISTORY, ok({'request_id': 'req-10'}, status=202))
    server.script(D.GET_RESULT, ok({'error': {'not': 'a string'}}))
    async with serve(server) as (url, session):
        client = HttpDispatchClient(url, session=session)
        queue = client.register_cog_queue('cog')
        await client.submit_request(_history_request())
        task = spawned()[0]
        with pytest.raises(SeamResponseInvalid):
            await task
    assert queue.empty()


@pytest.mark.asyncio
async def test_unrecognised_requests_are_ignored(spawned):
    '''submit_request on an object that is none of the four request types does nothing.'''
    server = _server()
    async with serve(server) as (url, session):
        await HttpDispatchClient(url, session=session).submit_request(object())
    assert not spawned()
    assert not server.calls
