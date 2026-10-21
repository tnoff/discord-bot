'''
HttpCommandsClient against a stub commands pod.

The stub registers the real route from the registry and records what the client sent, so both
directions of the contract are checked; the server itself is the commands pod's own tests.
'''
import asyncio
import contextlib

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from pydantic import ValidationError

from discord_core.clients import http_commands_client
from discord_core.clients.http_client_base import SeamResponseInvalid
from discord_core.clients.http_commands_client import HttpCommandsClient
from discord_core.interfaces.commands_client import CommandsClient, CommandsUnavailable
from discord_core.routes import commands as command_routes
from discord_core.types.command_envelope import CommandEnvelope
from discord_core.types.command_wire import CommandAcceptedResponse
from discord_core.utils.circuit_breaker import CircuitBreaker, CircuitState

ENVELOPE = CommandEnvelope(guild_id=1, channel_id=2, author_id=3, author_display_name='tyler',
                           author_role_ids=(9,))


@pytest.fixture(name='breaker', autouse=True)
def fresh_breaker(monkeypatch):
    '''The breaker is per process; give every test its own so one test's failures do not open it.'''
    breaker = CircuitBreaker(name='commands', failure_threshold=2, recovery_timeout=30.0)
    monkeypatch.setattr(http_commands_client, '_BREAKER', breaker)
    return breaker


class StubPod:
    '''A commands pod that remembers every request and answers with a canned status and body.'''

    def __init__(self, status=202, body=None):
        self.status = status
        self.body = {'status': 'ok', 'duplicate': False} if body is None else body
        self.calls = []

    async def handle(self, request: web.Request) -> web.StreamResponse:
        self.calls.append({'match': dict(request.match_info), 'body': await request.json(),
                           'traceparent': request.headers.get('traceparent')})
        return web.json_response(self.body, status=self.status)

    def build_app(self) -> web.Application:
        app = web.Application()
        route = command_routes.FORWARD_COMMAND
        app.router.add_route(route.method, route.template, self.handle)
        return app


@contextlib.asynccontextmanager
async def _connected(pod: StubPod):
    async with TestServer(pod.build_app()) as server:
        async with aiohttp.ClientSession() as session:
            yield HttpCommandsClient(str(server.make_url('')), session=session)


def test_the_client_implements_the_protocol():
    assert callable(getattr(HttpCommandsClient, 'forward'))
    assert set(CommandsClient.__dict__) >= {'forward'}


@pytest.mark.asyncio
async def test_forward_sends_the_envelope_args_and_id():
    pod = StubPod()
    async with _connected(pod) as client:
        reply = await client.forward('play', ENVELOPE, {'search': 'a song'}, request_id='r-1',
                                     span_context={'trace_id': 'x'})
    assert reply == CommandAcceptedResponse(status='ok', duplicate=False)
    call = pod.calls[0]
    assert call['match'] == {'command': 'play'}
    assert call['body']['request_id'] == 'r-1'
    assert call['body']['args'] == {'search': 'a song'}
    assert call['body']['span_context'] == {'trace_id': 'x'}
    assert call['body']['envelope']['author_role_ids'] == [9]
    assert call['body']['envelope']['guild_id'] == 1


@pytest.mark.asyncio
async def test_forward_fills_in_a_request_id_and_default_args():
    pod = StubPod()
    async with _connected(pod) as client:
        await client.forward('markov.speak', ENVELOPE)
    body = pod.calls[0]['body']
    assert len(body['request_id']) == 32
    assert body['args'] == {'first_word': '', 'sentence_length': 32}


@pytest.mark.asyncio
async def test_forward_reports_a_duplicate():
    pod = StubPod(body={'status': 'ok', 'duplicate': True})
    async with _connected(pod) as client:
        assert (await client.forward('skip', ENVELOPE)).duplicate is True


@pytest.mark.asyncio
async def test_forward_refuses_bad_args_before_sending():
    pod = StubPod()
    async with _connected(pod) as client:
        with pytest.raises(ValidationError):
            await client.forward('play', ENVELOPE, {})
    assert not pod.calls


@pytest.mark.asyncio
async def test_a_5xx_is_unavailable_and_is_not_retried():
    pod = StubPod(status=503, body={'error': 'busy'})
    async with _connected(pod) as client:
        with pytest.raises(CommandsUnavailable):
            await client.forward('skip', ENVELOPE)
    assert len(pod.calls) == 1


@pytest.mark.asyncio
async def test_a_4xx_is_the_senders_bug_and_does_not_count_against_the_pod(breaker):
    pod = StubPod(status=422, body={'error': 'bad'})
    async with _connected(pod) as client:
        for _ in range(3):
            with pytest.raises(aiohttp.ClientResponseError) as caught:
                await client.forward('skip', ENVELOPE)
            assert caught.value.status == 422
    assert breaker.state is CircuitState.CLOSED


@pytest.mark.asyncio
async def test_a_pod_that_is_down_is_unavailable():
    async with aiohttp.ClientSession() as session:
        client = HttpCommandsClient('http://127.0.0.1:1', session=session)
        with pytest.raises(CommandsUnavailable):
            await client.forward('skip', ENVELOPE)


@pytest.mark.asyncio
async def test_a_pod_slower_than_the_timeout_is_unavailable(monkeypatch):
    monkeypatch.setattr(http_commands_client, 'ACCEPT_TIMEOUT_SECONDS', 0.05)

    async def slow(_request):
        await asyncio.sleep(1)
        return web.json_response({})

    app = web.Application()
    app.router.add_route('POST', command_routes.FORWARD_COMMAND.template, slow)
    async with TestServer(app) as server, aiohttp.ClientSession() as session:
        client = HttpCommandsClient(str(server.make_url('')), session=session)
        with pytest.raises(CommandsUnavailable):
            await client.forward('skip', ENVELOPE)


@pytest.mark.asyncio
async def test_repeated_failures_open_the_breaker_and_later_commands_fail_fast(breaker):
    pod = StubPod(status=500)
    async with _connected(pod) as client:
        for _ in range(2):
            with pytest.raises(CommandsUnavailable):
                await client.forward('skip', ENVELOPE)
        assert breaker.state is CircuitState.OPEN
        with pytest.raises(CommandsUnavailable, match='breaker is open'):
            await client.forward('skip', ENVELOPE)
    assert len(pod.calls) == 2


@pytest.mark.asyncio
async def test_a_reply_that_does_not_fit_the_model_names_the_peer():
    pod = StubPod(body={'status': 'nope'})
    async with _connected(pod) as client:
        with pytest.raises(SeamResponseInvalid):
            await client.forward('skip', ENVELOPE)


def test_the_base_url_loses_its_trailing_slash():
    assert HttpCommandsClient('http://pod:8080/')._base_url == 'http://pod:8080'  # pylint: disable=protected-access
