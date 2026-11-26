'''HttpBrokerClient request lifecycle, results, cache and bundle calls against a scripted broker.'''
from pathlib import Path
from unittest.mock import AsyncMock

import aiohttp
import pytest

from discord_core.clients.http_broker_client import HttpBrokerClient
from discord_core.clients.http_client_base import SeamResponseInvalid
from discord_core.routes import broker as broker_routes
from discord_core.tests.clients.stub_broker import serve
from discord_core.tests.clients.stub_http import ScriptedServer, ok, plain, recording_tracer
from discord_core.types.checkout_result import CheckoutResult
from discord_core.types.download import DownloadResult, DownloadStatus, LifecycleEvent, LifecycleStatusUpdate
from discord_core.types.media_download import MediaDownload, media_download_to_dict
from discord_core.types.media_request import MediaRequest
from discord_core.types.search_resolution import SearchResolution

R = broker_routes
REQUEST_UUID = 'request.11111111'


def _server() -> ScriptedServer:
    '''A scripted broker serving the whole broker seam.'''
    return ScriptedServer(R.ALL)


def _request(url: str = 'https://example.org/track') -> MediaRequest:
    '''A direct-link request with a fixed uuid.'''
    return MediaRequest.model_validate({
        'uuid': REQUEST_UUID,
        'guild_id': 77, 'channel_id': 78, 'requester_name': 'someone', 'requester_id': 79,
        'search_result': {'search_type': 'direct', 'raw_search_string': url},
    })


def _download(request: MediaRequest) -> MediaDownload:
    '''A finished download of the request.'''
    return MediaDownload(Path('cache/one.mp3'), {
        'id': 'one', 'title': 'Track One', 'webpage_url': 'https://example.org/one',
        'uploader': 'Band', 'duration': 61, 'extractor': 'generic',
    }, request)


def _result(request: MediaRequest) -> DownloadResult:
    '''A successful download result for the request.'''
    return DownloadResult(status=DownloadStatus(success=True), media_request=request,
                          ytdlp_data=None, file_name=None)


@pytest.fixture(name='no_sleep')
def fixture_no_sleep(mocker):
    '''Make the retry ladder instant.'''
    return mocker.patch('discord_core.utils.retry.async_sleep', new=AsyncMock())


# ---------------------------------------------------------------------------
# request lifecycle
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_register_request_puts_the_request_under_its_uuid():
    '''register_request POSTs the dumped request to /requests/{uuid}.'''
    server, request = _server(), _request()
    server.script(R.REGISTER_REQUEST, ok({'status': 'ok'}, status=201))
    async with serve(server) as (url, session):
        assert await HttpBrokerClient(url, session=session).register_request(request) is None
    call = server.calls_to(R.REGISTER_REQUEST)[0]
    assert (call['method'], call['path']) == ('POST', f'/requests/{REQUEST_UUID}')
    assert call['body'] == request.model_dump(mode='json')


@pytest.mark.asyncio
async def test_update_request_status_sends_the_event():
    '''update_request_status PUTs the lifecycle update to /requests/{uuid}/status.'''
    server = _server()
    server.script(R.UPDATE_STATUS, ok({'status': 'ok'}))
    update = LifecycleStatusUpdate(event=LifecycleEvent.BACKOFF, backoff_seconds=12)
    async with serve(server) as (url, session):
        await HttpBrokerClient(url, session=session).update_request_status(REQUEST_UUID, update)
    call = server.calls_to(R.UPDATE_STATUS)[0]
    assert (call['method'], call['path']) == ('PUT', f'/requests/{REQUEST_UUID}/status')
    assert call['body']['event'] == 'backoff'
    assert call['body']['backoff_seconds'] == 12


@pytest.mark.asyncio
async def test_register_download_result_posts_and_returns_none():
    '''register_download_result forwards the result to /downloads and hands nothing back.'''
    server, request = _server(), _request()
    server.script(R.REGISTER_DOWNLOAD, ok({'status': 'ok'}, status=202))
    async with serve(server) as (url, session):
        client = HttpBrokerClient(url, session=session)
        assert await client.register_download_result(_result(request)) is None
    body = server.calls_to(R.REGISTER_DOWNLOAD)[0]['body']
    assert body['status']['success'] is True
    assert body['media_request']['uuid'] == REQUEST_UUID


@pytest.mark.asyncio
@pytest.mark.parametrize('method_name', ['release', 'remove', 'discard'])
async def test_uuid_only_calls_post_to_their_own_route(method_name):
    '''release, remove and discard each POST an empty body to a distinct /requests/{uuid}/... path.'''
    route = {'release': R.RELEASE, 'remove': R.REMOVE, 'discard': R.DISCARD}[method_name]
    server = _server()
    server.script(route, ok({'status': 'ok'}))
    async with serve(server) as (url, session):
        await getattr(HttpBrokerClient(url, session=session), method_name)(REQUEST_UUID)
    call = server.calls_to(route)[0]
    assert (call['method'], call['path']) == ('POST', f'/requests/{REQUEST_UUID}/{method_name}')
    assert call['body'] is None
    assert len(server.calls) == 1


@pytest.mark.asyncio
async def test_register_download_sends_the_serialised_download():
    '''register_download POSTs media_download_to_dict to /downloads/register.'''
    server, request = _server(), _request()
    download = _download(request)
    server.script(R.REGISTER_DOWNLOAD_DIRECT, ok({'status': 'ok'}))
    async with serve(server) as (url, session):
        await HttpBrokerClient(url, session=session).register_download(download)
    call = server.calls_to(R.REGISTER_DOWNLOAD_DIRECT)[0]
    assert call['path'] == '/downloads/register'
    assert call['body'] == media_download_to_dict(download)
    assert call['body']['ytdl_data']['title'] == 'Track One'


@pytest.mark.asyncio
async def test_a_client_error_is_raised_without_retry():
    '''A 4xx from the broker surfaces at once as a ClientResponseError.'''
    server = _server()
    server.script(R.RELEASE, ok({'detail': 'bad'}, status=400))
    async with serve(server) as (url, session):
        with pytest.raises(aiohttp.ClientResponseError) as caught:
            await HttpBrokerClient(url, session=session).release(REQUEST_UUID)
    assert caught.value.status == 400
    assert len(server.calls) == 1


@pytest.mark.asyncio
async def test_a_server_error_is_retried_until_it_clears(no_sleep):
    '''A transient 502 is retried with backoff and the call then succeeds.'''
    server = _server()
    server.script(R.REMOVE, ok(None, status=502), ok({'status': 'ok'}))
    async with serve(server) as (url, session):
        await HttpBrokerClient(url, session=session).remove(REQUEST_UUID)
    assert len(server.calls) == 2
    assert no_sleep.await_count == 1


# ---------------------------------------------------------------------------
# polling the bot-ready queues
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_next_result_parses_a_ready_result():
    '''next_result GETs /results/next and parses the body into a DownloadResult.'''
    server, request = _server(), _request()
    server.script(R.NEXT_RESULT, ok(_result(request).model_dump(mode='json')))
    async with serve(server) as (url, session):
        result = await HttpBrokerClient(url, session=session).next_result()
    assert isinstance(result, DownloadResult)
    assert result.media_request.uuid == REQUEST_UUID
    assert (server.calls[0]['method'], server.calls[0]['path']) == ('GET', '/results/next')


@pytest.mark.asyncio
@pytest.mark.parametrize('status', [204, 404])
async def test_next_result_treats_empty_and_missing_route_as_nothing(status, caplog):
    '''A 204 is an empty queue and a 404 a not-yet-upgraded broker; both read as None.'''
    server = _server()
    server.script(R.NEXT_RESULT, ok(None, status=status))
    async with serve(server) as (url, session):
        assert await HttpBrokerClient(url, session=session).next_result() is None
    assert ('peer not upgraded' in caplog.text) == (status == 404)


@pytest.mark.asyncio
async def test_next_result_raises_on_a_server_error():
    '''Any other failing status is raised rather than hidden as an empty queue.'''
    server = _server()
    server.script(R.NEXT_RESULT, ok(None, status=500))
    async with serve(server) as (url, session):
        with pytest.raises(aiohttp.ClientResponseError) as caught:
            await HttpBrokerClient(url, session=session).next_result()
    assert caught.value.status == 500


@pytest.mark.asyncio
async def test_next_result_rejects_a_body_that_is_not_a_result():
    '''A result body this build cannot parse raises a seam error naming the broker.'''
    server = _server()
    server.script(R.NEXT_RESULT, ok({'unexpected': 1}))
    async with serve(server) as (url, session):
        with pytest.raises(SeamResponseInvalid):
            await HttpBrokerClient(url, session=session).next_result()


@pytest.mark.asyncio
async def test_next_result_opens_a_span_only_for_a_real_result(mocker):
    '''The idle poll mints no span; parsing a result does.'''
    tracer, exporter = recording_tracer()
    mocker.patch('discord_core.utils.otel.TRACER', tracer)
    server, request = _server(), _request()
    server.script(R.NEXT_RESULT, ok(None, status=204), ok(_result(request).model_dump(mode='json')))
    async with serve(server) as (url, session):
        client = HttpBrokerClient(url, session=session)
        await client.next_result()
        assert not exporter.get_finished_spans()
        await client.next_result()
    assert [span.name for span in exporter.get_finished_spans()] == ['broker.next_result']


@pytest.mark.asyncio
async def test_register_search_result_posts_the_resolution():
    '''register_search_result POSTs the resolution to /search-results.'''
    server, request = _server(), _request()
    resolution = SearchResolution(media_request=request)
    server.script(R.REGISTER_SEARCH_RESULT, ok({'status': 'ok'}, status=202))
    async with serve(server) as (url, session):
        await HttpBrokerClient(url, session=session).register_search_result(resolution)
    assert server.calls[0]['body']['media_request']['uuid'] == REQUEST_UUID


@pytest.mark.asyncio
async def test_register_search_result_drops_the_resolution_on_a_404(caplog):
    '''A broker that predates the route is tolerated: the resolution is dropped with a warning.'''
    server, request = _server(), _request()
    server.script(R.REGISTER_SEARCH_RESULT, ok(None, status=404))
    async with serve(server) as (url, session):
        client = HttpBrokerClient(url, session=session)
        assert await client.register_search_result(SearchResolution(media_request=request)) is None
    assert REQUEST_UUID in caplog.text
    assert 'peer not upgraded' in caplog.text


@pytest.mark.asyncio
async def test_register_search_result_raises_other_client_errors():
    '''Only a 404 is swallowed; a 409 still propagates.'''
    server, request = _server(), _request()
    server.script(R.REGISTER_SEARCH_RESULT, ok(None, status=409))
    async with serve(server) as (url, session):
        with pytest.raises(aiohttp.ClientResponseError) as caught:
            await HttpBrokerClient(url, session=session).register_search_result(
                SearchResolution(media_request=request))
    assert caught.value.status == 409


@pytest.mark.asyncio
async def test_next_search_result_parses_a_resolution():
    '''next_search_result returns the resolution the broker had queued.'''
    server, request = _server(), _request()
    server.script(R.NEXT_SEARCH_RESULT, ok(SearchResolution(media_request=request).model_dump(mode='json')))
    async with serve(server) as (url, session):
        resolution = await HttpBrokerClient(url, session=session).next_search_result()
    assert isinstance(resolution, SearchResolution)
    assert resolution.media_request.uuid == REQUEST_UUID


@pytest.mark.asyncio
@pytest.mark.parametrize('status', [204, 404])
async def test_next_search_result_reads_empty_and_missing_as_none(status):
    '''204 and 404 both mean there is nothing to consume yet.'''
    server = _server()
    server.script(R.NEXT_SEARCH_RESULT, ok(None, status=status))
    async with serve(server) as (url, session):
        assert await HttpBrokerClient(url, session=session).next_search_result() is None


@pytest.mark.asyncio
async def test_next_search_result_raises_on_a_gateway_error():
    '''A 503 is a real failure and is raised.'''
    server = _server()
    server.script(R.NEXT_SEARCH_RESULT, ok(None, status=503))
    async with serve(server) as (url, session):
        with pytest.raises(aiohttp.ClientResponseError):
            await HttpBrokerClient(url, session=session).next_search_result()


@pytest.mark.asyncio
async def test_next_search_result_rejects_an_unparseable_body():
    '''A resolution this build cannot parse raises a seam error.'''
    server = _server()
    server.script(R.NEXT_SEARCH_RESULT, ok({'nothing': 'useful'}))
    async with serve(server) as (url, session):
        with pytest.raises(SeamResponseInvalid):
            await HttpBrokerClient(url, session=session).next_search_result()


@pytest.mark.asyncio
async def test_next_search_result_span_only_on_a_real_resolution(mocker):
    '''No span for the idle poll; one named for the call once a resolution is parsed.'''
    tracer, exporter = recording_tracer()
    mocker.patch('discord_core.utils.otel.TRACER', tracer)
    server, request = _server(), _request()
    server.script(R.NEXT_SEARCH_RESULT, ok(None, status=204),
                  ok(SearchResolution(media_request=request).model_dump(mode='json')))
    async with serve(server) as (url, session):
        client = HttpBrokerClient(url, session=session)
        await client.next_search_result()
        assert not exporter.get_finished_spans()
        await client.next_search_result()
    assert [span.name for span in exporter.get_finished_spans()] == ['broker.next_search_result']


# ---------------------------------------------------------------------------
# checkout
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_checkout_hit_carries_the_clients_own_bucket():
    '''A hit becomes a CheckoutResult whose bucket comes from the client config.'''
    server = _server()
    server.script(R.CHECKOUT, ok({'s3_key': 'k/one.mp3'}))
    async with serve(server) as (url, session):
        client = HttpBrokerClient(url, bucket_name='my-bucket', session=session)
        result = await client.checkout(REQUEST_UUID, 5)
    assert result == CheckoutResult(s3_key='k/one.mp3', bucket_name='my-bucket')
    assert server.calls[0]['body'] == {'guild_id': 5}


@pytest.mark.asyncio
@pytest.mark.parametrize('answer', [None, {}])
async def test_checkout_miss_is_none(answer):
    '''An empty body and an empty object both read as nothing to check out.'''
    server = _server()
    server.script(R.CHECKOUT, ok(answer))
    async with serve(server) as (url, session):
        assert await HttpBrokerClient(url, session=session).checkout(REQUEST_UUID, 5) is None


@pytest.mark.asyncio
async def test_checkout_miss_with_a_stale_field_is_still_none():
    '''A miss from an older broker carries an unused key, which is ignored.'''
    server = _server()
    server.script(R.CHECKOUT, ok({'guild_file_path': None}))
    async with serve(server) as (url, session):
        assert await HttpBrokerClient(url, session=session).checkout(REQUEST_UUID, 5) is None


# ---------------------------------------------------------------------------
# video cache
# ---------------------------------------------------------------------------

def _cache_hit(request: MediaRequest) -> dict:
    '''A cache-hit body for the request.'''
    return {'hit': True, 'download': {
        'request': request.model_dump(mode='json'), 'file_path': 'cache/one.mp3',
        'file_size_bytes': 2048, 'cache_hit': True,
        'ytdl_data': {'id': 'one', 'title': 'Cached One', 'duration': 30},
    }}


@pytest.mark.asyncio
async def test_check_cache_hit_rebuilds_the_download_for_the_callers_request():
    '''A hit is rebuilt into a MediaDownload attached to the request the caller already holds.'''
    server, request = _server(), _request()
    server.script(R.CHECK_CACHE, ok(_cache_hit(request)))
    async with serve(server) as (url, session):
        download = await HttpBrokerClient(url, session=session).check_cache(request)
    assert download.media_request is request
    assert (download.title, download.file_path, download.file_size_bytes) == ('Cached One', Path('cache/one.mp3'), 2048)
    assert download.cache_hit is True
    assert server.calls[0]['body']['uuid'] == REQUEST_UUID


@pytest.mark.asyncio
@pytest.mark.parametrize('answer', [None, {'hit': False}])
async def test_check_cache_miss_is_none(answer):
    '''A miss, or an answerless 200, is a cache miss.'''
    server, request = _server(), _request()
    server.script(R.CHECK_CACHE, ok(answer))
    async with serve(server) as (url, session):
        assert await HttpBrokerClient(url, session=session).check_cache(request) is None


@pytest.mark.asyncio
async def test_check_cache_hit_without_a_download_is_a_seam_error():
    '''A hit that lacks its download record is rejected, naming the peer.'''
    server, request = _server(), _request()
    server.script(R.CHECK_CACHE, ok({'hit': True}))
    async with serve(server) as (url, session):
        with pytest.raises(SeamResponseInvalid):
            await HttpBrokerClient(url, session=session).check_cache(request)


@pytest.mark.asyncio
async def test_cache_cleanup_reports_whether_anything_was_removed():
    '''cache_cleanup returns the removed flag, and False when the body is empty.'''
    server = _server()
    server.script(R.CACHE_CLEANUP, ok({'removed': True}), ok(None), ok({'removed': False}))
    async with serve(server) as (url, session):
        client = HttpBrokerClient(url, session=session)
        assert await client.cache_cleanup() is True
        assert await client.cache_cleanup() is False
        assert await client.cache_cleanup() is False
    assert server.calls[0]['path'] == '/cache/cleanup'


@pytest.mark.asyncio
async def test_cache_cleanup_rejects_a_body_without_the_flag():
    '''A body that is not a cleanup response is a seam error.'''
    server = _server()
    server.script(R.CACHE_CLEANUP, ok({'removed': 'nope'}))
    async with serve(server) as (url, session):
        with pytest.raises(SeamResponseInvalid):
            await HttpBrokerClient(url, session=session).cache_cleanup()


@pytest.mark.asyncio
async def test_get_cache_count_returns_the_count_or_zero():
    '''get_cache_count GETs /cache/count and treats an empty answer as zero.'''
    server = _server()
    server.script(R.CACHE_COUNT, ok({'count': 41}), ok(None))
    async with serve(server) as (url, session):
        client = HttpBrokerClient(url, session=session)
        assert await client.get_cache_count() == 41
        assert await client.get_cache_count() == 0
    assert server.calls[0]['method'] == 'GET'


@pytest.mark.asyncio
async def test_get_cache_count_rejects_a_non_numeric_count():
    '''A count that is not an integer is a seam error.'''
    server = _server()
    server.script(R.CACHE_COUNT, ok({'count': 'many'}))
    async with serve(server) as (url, session):
        with pytest.raises(SeamResponseInvalid):
            await HttpBrokerClient(url, session=session).get_cache_count()


# ---------------------------------------------------------------------------
# bundles
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_bundle_sends_its_arguments_and_returns_the_uuid():
    '''create_bundle POSTs the guild, channel and banner flags and returns the new uuid.'''
    server = _server()
    server.script(R.CREATE_BUNDLE, ok({'uuid': 'bundle.9'}, status=201))
    async with serve(server) as (url, session):
        bundle = await HttpBrokerClient(url, session=session).create_bundle(
            3, 4, input_string='lofi', has_search_banner=True)
    assert bundle == 'bundle.9'
    assert server.calls[0]['body'] == {'guild_id': 3, 'channel_id': 4,
                                       'input_string': 'lofi', 'has_search_banner': True}


@pytest.mark.asyncio
@pytest.mark.parametrize('answer', [None, {'other': 1}])
async def test_create_bundle_without_a_uuid_is_an_error(answer):
    '''No uuid in the answer cannot be papered over: the caller needs the bundle.'''
    server = _server()
    server.script(R.CREATE_BUNDLE, ok(answer))
    async with serve(server) as (url, session):
        with pytest.raises(RuntimeError, match='no uuid'):
            await HttpBrokerClient(url, session=session).create_bundle(3, 4)


@pytest.mark.asyncio
async def test_create_bundle_with_a_non_string_uuid_is_a_seam_error():
    '''A uuid of the wrong type is rejected naming the peer.'''
    server = _server()
    server.script(R.CREATE_BUNDLE, ok({'uuid': {'nested': 1}}))
    async with serve(server) as (url, session):
        with pytest.raises(SeamResponseInvalid):
            await HttpBrokerClient(url, session=session).create_bundle(3, 4)


@pytest.mark.asyncio
async def test_finalize_and_delete_bundle_use_their_own_verbs():
    '''finalize POSTs /bundles/{uuid}/finalize and delete issues DELETE /bundles/{uuid}.'''
    server = _server()
    server.script(R.FINALIZE_BUNDLE, ok({'status': 'ok'}))
    server.script(R.DELETE_BUNDLE, ok({'status': 'ok'}))
    async with serve(server) as (url, session):
        client = HttpBrokerClient(url, session=session)
        await client.finalize_bundle('bundle.9')
        await client.delete_bundle('bundle.9')
    assert [(c['method'], c['path']) for c in server.calls] == [
        ('POST', '/bundles/bundle.9/finalize'), ('DELETE', '/bundles/bundle.9')]


@pytest.mark.asyncio
async def test_list_bundles_passes_the_guild_in_the_query():
    '''list_bundles_for_guild GETs /bundles?guild_id=N and returns the uuid list.'''
    server = _server()
    server.script(R.LIST_BUNDLES, ok({'uuids': ['a', 'b']}), ok(None), ok({}))
    async with serve(server) as (url, session):
        client = HttpBrokerClient(url, session=session)
        assert await client.list_bundles_for_guild(12) == ['a', 'b']
        assert await client.list_bundles_for_guild(12) == []
        assert await client.list_bundles_for_guild(12) == []
    assert server.calls[0]['query'] == {'guild_id': '12'}
    assert server.calls[0]['method'] == 'GET'


# ---------------------------------------------------------------------------
# transport details
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_a_non_json_answer_reads_as_no_payload():
    '''A success whose content type is not JSON is treated as an empty answer.'''
    server = _server()
    server.script(R.CACHE_COUNT, plain('42'))
    async with serve(server) as (url, session):
        assert await HttpBrokerClient(url, session=session).get_cache_count() == 0


@pytest.mark.asyncio
async def test_the_live_trace_context_is_sent_on(mocker):
    '''A call made inside a recording span carries a sampled traceparent for that trace.'''
    tracer, _ = recording_tracer()
    mocker.patch('discord_core.utils.otel.TRACER', tracer)
    server = _server()
    server.script(R.RELEASE, ok({'status': 'ok'}))
    async with serve(server) as (url, session):
        with tracer.start_as_current_span('caller') as span:
            await HttpBrokerClient(url, session=session).release(REQUEST_UUID)
            trace_id = format(span.get_span_context().trace_id, '032x')
    version, sent_trace, _, flags = server.calls[0]['traceparent'].split('-')
    assert (version, sent_trace) == ('00', trace_id)
    assert int(flags, 16) & 1 == 1


@pytest.mark.asyncio
async def test_the_client_builds_its_own_session_when_none_is_given():
    '''Without an injected session the mixin creates one lazily and close() releases it.'''
    server = _server()
    server.script(R.CACHE_COUNT, ok({'count': 1}))
    async with serve(server) as (url, _):
        client = HttpBrokerClient(url + '/')
        assert await client.get_cache_count() == 1
        await client.close()
    assert server.calls[0]['path'] == '/cache/count'
