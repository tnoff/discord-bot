'''HttpStoreBase against a scripted db pod: the envelope, the error mapping and the span helper.'''
from unittest.mock import AsyncMock

import aiohttp
import pytest

from discord_core.clients.http_client_base import SeamResponseInvalid
from discord_core.clients.http_store_base import HttpStoreBase
from discord_core.exceptions import DatabaseUnavailable
from discord_core.routes import database as database_routes
from discord_core.tests.clients.stub_broker import serve
from discord_core.tests.clients.stub_http import ScriptedServer, ok, plain, recording_tracer

LIST_CHANNELS = database_routes.MARKOV.routes['list_channels']
ADD_CHANNEL = database_routes.MARKOV.routes['add_channel']


class _MarkovStore(HttpStoreBase):
    '''A store client on the markov group, exposing the base's helpers.'''
    GROUP = database_routes.MARKOV
    SPAN_PREFIX = 'markov_store'

    async def run(self, route, body=None):
        '''Forward to the base class call.'''
        return await self._call(route, body)

    def open_span(self, route, attributes=None):
        '''Forward to the base class span.'''
        return self._span(route, attributes)


class _UngroupedStore(HttpStoreBase):
    '''A store that never names a group.'''


@pytest.fixture(name='no_sleep')
def fixture_no_sleep(mocker):
    '''Make the retry ladder instant.'''
    return mocker.patch('discord_core.utils.retry.async_sleep', new=AsyncMock())


def test_group_derives_the_prefix_and_the_routes_called():
    '''A subclass naming a group gets that group's prefix and exactly its routes.'''
    assert _MarkovStore.ROUTE_PREFIX == '/database/markov'
    assert _MarkovStore.ROUTES_CALLED == database_routes.MARKOV.all
    assert _MarkovStore.SEAM == 'database'


def test_a_subclass_without_a_group_keeps_the_empty_prefix():
    '''No GROUP means no derived prefix and no routes to demand of the peer.'''
    assert _UngroupedStore.ROUTE_PREFIX == ''
    assert not _UngroupedStore.ROUTES_CALLED


@pytest.mark.asyncio
async def test_trailing_slashes_are_stripped_from_the_base_url():
    '''The base url is normalised so route urls never carry a double slash.'''
    server = ScriptedServer(database_routes.MARKOV.all)
    server.script(LIST_CHANNELS, ok({'result': []}))
    async with serve(server) as (url, session):
        await _MarkovStore(url.rstrip('/') + '///', session=session).run('list_channels')
    assert server.calls_to(LIST_CHANNELS)[0]['path'] == '/database/markov/list_channels'


@pytest.mark.asyncio
async def test_call_posts_the_body_and_returns_the_result():
    '''A route call POSTs its body under the group prefix and unwraps the envelope result.'''
    server = ScriptedServer(database_routes.MARKOV.all)
    server.script(ADD_CHANNEL, ok({'result': {'channel_id': 7}, 'error': None}))
    async with serve(server) as (url, session):
        result = await _MarkovStore(url, session=session).run('add_channel', {'channel_id': 7})
    assert result == {'channel_id': 7}
    call = server.calls_to(ADD_CHANNEL)[0]
    assert (call['method'], call['path'], call['body']) == ('POST', '/database/markov/add_channel', {'channel_id': 7})


@pytest.mark.asyncio
async def test_call_without_a_body_sends_an_empty_object():
    '''Routes that take no arguments still POST a JSON object, never null.'''
    server = ScriptedServer(database_routes.MARKOV.all)
    server.script(LIST_CHANNELS, ok({'result': [1, 2]}))
    async with serve(server) as (url, session):
        assert await _MarkovStore(url, session=session).run('list_channels') == [1, 2]
    assert server.calls_to(LIST_CHANNELS)[0]['body'] == {}


@pytest.mark.asyncio
async def test_an_error_in_the_envelope_raises_without_retrying():
    '''An error inside a 200 is the pod's own exhausted failure: raised once, never re-run.'''
    server = ScriptedServer(database_routes.MARKOV.all)
    server.script(LIST_CHANNELS, ok({'error': {'detail': 'OperationalError: gone'}}))
    async with serve(server) as (url, session):
        with pytest.raises(DatabaseUnavailable, match='OperationalError: gone'):
            await _MarkovStore(url, session=session).run('list_channels')
    assert len(server.calls_to(LIST_CHANNELS)) == 1


@pytest.mark.asyncio
async def test_an_unknown_route_name_fails_before_any_request():
    '''A route the group does not define is a KeyError at the call site, not a 404.'''
    server = ScriptedServer(database_routes.MARKOV.all)
    async with serve(server) as (url, session):
        with pytest.raises(KeyError):
            await _MarkovStore(url, session=session).run('no_such_route')
    assert not server.calls


@pytest.mark.asyncio
async def test_a_malformed_envelope_names_the_sender():
    '''A body that is not an envelope surfaces as a seam error carrying the seam and prefix.'''
    server = ScriptedServer(database_routes.MARKOV.all)
    server.script(LIST_CHANNELS, ok({'error': {'unexpected': True}}))
    async with serve(server) as (url, session):
        with pytest.raises(SeamResponseInvalid) as caught:
            await _MarkovStore(url, session=session).run('list_channels')
    assert 'database' in str(caught.value)
    assert '/database/markov' in str(caught.value)


@pytest.mark.asyncio
async def test_a_non_json_reply_is_no_envelope_at_all():
    '''A 200 that is not JSON parses as no payload, which the envelope check rejects.'''
    server = ScriptedServer(database_routes.MARKOV.all)
    server.script(LIST_CHANNELS, plain('hello'))
    async with serve(server) as (url, session):
        with pytest.raises(SeamResponseInvalid):
            await _MarkovStore(url, session=session).run('list_channels')


@pytest.mark.asyncio
async def test_a_client_error_status_propagates_on_the_first_attempt():
    '''A 4xx will not change on retry, so it surfaces immediately.'''
    server = ScriptedServer(database_routes.MARKOV.all)
    server.script(LIST_CHANNELS, ok({'detail': 'nope'}, status=422))
    async with serve(server) as (url, session):
        with pytest.raises(aiohttp.ClientResponseError) as caught:
            await _MarkovStore(url, session=session).run('list_channels')
    assert caught.value.status == 422
    assert len(server.calls_to(LIST_CHANNELS)) == 1


@pytest.mark.asyncio
async def test_a_server_error_is_retried_then_succeeds(no_sleep):
    '''A 5xx from the pod itself is the one failure the client ladders on.'''
    server = ScriptedServer(database_routes.MARKOV.all)
    server.script(LIST_CHANNELS, ok(None, status=503), ok(None, status=503), ok({'result': ['ok']}))
    async with serve(server) as (url, session):
        assert await _MarkovStore(url, session=session).run('list_channels') == ['ok']
    assert len(server.calls_to(LIST_CHANNELS)) == 3
    assert [step.args[0] for step in no_sleep.await_args_list] == [1, 2]


@pytest.mark.asyncio
async def test_a_persistent_server_error_gives_up_after_the_retry_budget(no_sleep):
    '''Three retries after the first attempt, then the 5xx is raised.'''
    server = ScriptedServer(database_routes.MARKOV.all)
    server.script(LIST_CHANNELS, ok(None, status=500))
    async with serve(server) as (url, session):
        with pytest.raises(aiohttp.ClientResponseError) as caught:
            await _MarkovStore(url, session=session).run('list_channels')
    assert caught.value.status == 500
    assert len(server.calls_to(LIST_CHANNELS)) == 4
    assert no_sleep.await_count == 3


@pytest.mark.asyncio
async def test_the_span_is_named_for_the_prefix_and_route(mocker):
    '''_span opens a client span called <SPAN_PREFIX>.<route> carrying the attributes.'''
    tracer, exporter = recording_tracer()
    mocker.patch('discord_core.utils.otel.TRACER', tracer)
    async with _MarkovStore('http://db').open_span('get_channel', {'discord.guild': 5}):
        pass
    (span,) = exporter.get_finished_spans()
    assert span.name == 'markov_store.get_channel'
    assert span.attributes['discord.guild'] == 5
    assert span.kind.name == 'CLIENT'
