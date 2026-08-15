'''
Tests for BrokerHttpServer — the aiohttp HTTP server wrapping MediaBroker.
'''
import asyncio
import json
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, MagicMock, PropertyMock

import aiohttp
import pytest
from aiohttp.test_utils import TestClient, TestServer

from discord_bot.workers.asyncio_broker import AsyncioBroker as MediaBroker
from discord_bot.servers.broker_server import BrokerHttpServer, _QueueItemProxy
from discord_bot.types.download import LifecycleEvent, DownloadResult, DownloadStatus
from discord_bot.types.playlist_add_request import PlaylistAddRequest

from tests.helpers import fake_source_dict, fake_media_download, generate_fake_context


def _make_broker() -> MediaBroker:
    return MediaBroker()


def _make_request():
    return fake_source_dict(generate_fake_context())


def _make_server(broker: MediaBroker) -> BrokerHttpServer:
    return BrokerHttpServer(broker)


class TestQueueItemProxy:
    def test_media_request_returns_self(self):
        proxy = _QueueItemProxy(uuid='test-uuid')
        assert proxy.media_request is proxy

    def test_uuid_accessible_via_media_request(self):
        proxy = _QueueItemProxy(uuid='abc-123')
        assert proxy.media_request.uuid == 'abc-123'


class TestHeartbeatObservations:
    def test_reports_zero_before_serving(self):
        '''A freshly-built server is not serving yet, so heartbeat is 0.'''
        server = _make_server(_make_broker())
        (observation,) = server.heartbeat_observations()
        assert observation.value == 0
        assert observation.attributes == {'background_job': 'broker'}

    def test_reports_one_while_serving(self, mocker):
        '''While the server reports serving, heartbeat is 1 under background_job="broker".'''
        server = _make_server(_make_broker())
        mocker.patch.object(BrokerHttpServer, 'is_serving',
                            new_callable=PropertyMock, return_value=True)
        (observation,) = server.heartbeat_observations()
        assert observation.value == 1
        assert observation.attributes == {'background_job': 'broker'}


@pytest.mark.asyncio
class TestNextResultCounter:
    async def test_empty_increments_empty_outcome(self, mocker):
        '''GET /results/next with nothing queued returns 204 and counts "empty".'''
        counter = mocker.patch('discord_bot.servers.broker_server._RESULT_FETCH_COUNTER')
        queue = MagicMock()
        queue.get_nowait = AsyncMock(return_value=None)
        server = BrokerHttpServer(_make_broker(), result_queue=queue)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.get('/results/next')
            assert resp.status == 204
        counter.add.assert_called_once_with(1, {'outcome': 'empty'})

    async def test_hit_increments_hit_outcome(self, mocker):
        '''GET /results/next with a result queued returns 200 and counts "hit".'''
        counter = mocker.patch('discord_bot.servers.broker_server._RESULT_FETCH_COUNTER')
        result = MagicMock()
        result.model_dump.return_value = {'ok': True}
        queue = MagicMock()
        queue.get_nowait = AsyncMock(return_value=result)
        server = BrokerHttpServer(_make_broker(), result_queue=queue)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.get('/results/next')
            assert resp.status == 200
        counter.add.assert_called_once_with(1, {'outcome': 'hit'})


@pytest.mark.asyncio
class TestNextSearchResultCounter:
    async def test_empty_increments_empty_outcome(self, mocker):
        '''GET /search-results/next with nothing queued returns 204 and counts "empty".'''
        counter = mocker.patch('discord_bot.servers.broker_server._SEARCH_RESULT_FETCH_COUNTER')
        queue = MagicMock()
        queue.get_nowait = AsyncMock(return_value=None)
        server = BrokerHttpServer(_make_broker(), search_result_queue=queue)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.get('/search-results/next')
            assert resp.status == 204
        counter.add.assert_called_once_with(1, {'outcome': 'empty'})

    async def test_hit_increments_hit_outcome(self, mocker):
        '''GET /search-results/next with a resolution queued returns 200 and counts "hit".'''
        counter = mocker.patch('discord_bot.servers.broker_server._SEARCH_RESULT_FETCH_COUNTER')
        resolution = MagicMock()
        resolution.model_dump.return_value = {'ok': True}
        queue = MagicMock()
        queue.get_nowait = AsyncMock(return_value=resolution)
        server = BrokerHttpServer(_make_broker(), search_result_queue=queue)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.get('/search-results/next')
            assert resp.status == 200
        counter.add.assert_called_once_with(1, {'outcome': 'hit'})


@pytest.mark.asyncio
class TestRegisterSearchResult:
    async def test_invalid_body_returns_422(self):
        '''A body that isn't a valid SearchResolution is rejected with 422.'''
        server = _make_server(_make_broker())
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post('/search-results', json={'not_a': 'resolution'})
            assert resp.status == 422


@pytest.mark.asyncio
class TestServe:
    async def test_serve_starts_and_responds(self):
        '''serve() starts the aiohttp server and handles requests until cancelled.'''
        broker = _make_broker()
        server = BrokerHttpServer(broker, host='127.0.0.1', port=19200)
        task = asyncio.create_task(server.serve())
        # Wait for the port to be ready
        deadline = asyncio.get_event_loop().time() + 5.0
        while True:
            try:
                _, writer = await asyncio.open_connection('127.0.0.1', 19200)
                writer.close()
                try:
                    await writer.wait_closed()
                except OSError:
                    pass
                break
            except OSError:
                if asyncio.get_event_loop().time() >= deadline:
                    raise
                await asyncio.sleep(0.02)
        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.post(
                    'http://127.0.0.1:19200/requests/unknown/release'
                )
                assert resp.status == 200
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass


@pytest.mark.asyncio
class TestRegisterRequest:
    async def test_invalid_body_returns_422(self):
        broker = _make_broker()
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post(
                '/requests/some-uuid',
                json={'not_a': 'valid_media_request'},
            )
            assert resp.status == 422

    async def test_malformed_json_returns_422(self):
        '''Non-JSON request bodies are rejected by AiohttpServerBase._read_body
        (request.json() raises) before any handler-specific parsing.'''
        broker = _make_broker()
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post(
                '/requests/some-uuid',
                data='this is not json{',
                headers={'Content-Type': 'application/json'},
            )
            assert resp.status == 422

    async def test_accepts_playlist_add_request_body(self):
        '''A PlaylistAddRequest serializes to download_file=false; the broker
        must accept and persist it as PlaylistAddRequest so playlist_id
        survives the round-trip (otherwise !playlist item-add fails with 422).'''
        broker = _make_broker()
        ctx = generate_fake_context()
        base = fake_source_dict(ctx)
        par = PlaylistAddRequest(
            guild_id=base.guild_id, channel_id=base.channel_id,
            requester_id=base.requester_id, requester_name=base.requester_name,
            search_result=base.search_result, playlist_id=42,
        )
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post(
                f'/requests/{par.uuid}', json=par.model_dump(mode='json'),
            )
            assert resp.status == 201
        # Stored entry preserves the PlaylistAddRequest type and its playlist_id.
        entry = await broker.get_entry(str(par.uuid))
        assert entry is not None
        assert isinstance(entry.request, PlaylistAddRequest)
        assert entry.request.playlist_id == 42


@pytest.mark.asyncio
class TestUpdateStatus:
    async def test_valid_request_calls_broker(self):
        broker = _make_broker()
        mr = _make_request()
        await broker.register_request(mr)
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.put(
                f'/requests/{mr.uuid}/status',
                json={'event': LifecycleEvent.IN_PROGRESS.value},
            )
            assert resp.status == 200
            data = await resp.json()
            assert data['status'] == 'ok'

    async def test_retry_event_with_detail(self):
        broker = _make_broker()
        mr = _make_request()
        await broker.register_request(mr)
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.put(
                f'/requests/{mr.uuid}/status',
                json={
                    'event': LifecycleEvent.RETRY.value,
                    'error_detail': 'bot flagged',
                    'backoff_seconds': 30,
                },
            )
            assert resp.status == 200

    async def test_invalid_body_returns_422(self):
        broker = _make_broker()
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.put(
                '/requests/some-uuid/status',
                json={'event': 'not_a_valid_event'},
            )
            assert resp.status == 422

    async def test_unknown_uuid_still_returns_200(self):
        # Matches current MediaBroker.update_request_status behavior: warns and continues
        broker = _make_broker()
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.put(
                '/requests/unknown-uuid/status',
                json={'event': LifecycleEvent.IN_PROGRESS.value},
            )
            assert resp.status == 200


@pytest.mark.asyncio
class TestRegisterDownload:
    async def test_valid_download_result_accepted(self):
        broker = _make_broker()
        mr = _make_request()
        await broker.register_request(mr)
        server = _make_server(broker)
        with TemporaryDirectory() as tmp_dir:
            with fake_media_download(tmp_dir, media_request=mr) as md:
                result = DownloadResult(
                    status=DownloadStatus(success=True),
                    media_request=mr,
                    ytdlp_data={'id': 'abc', 'title': 'Test', 'webpage_url': 'http://example.com',
                                'uploader': 'tester', 'duration': 120, 'extractor': 'youtube'},
                    file_name=md.file_path,
                )
                async with TestClient(TestServer(server.build_app())) as client:
                    resp = await client.post(
                        '/downloads',
                        json=result.model_dump(mode='json'),
                    )
                    assert resp.status == 202

    async def test_result_enqueued_when_result_queue_provided(self):
        broker = _make_broker()
        mr = _make_request()
        await broker.register_request(mr)
        result_queue: asyncio.Queue = asyncio.Queue()
        server = BrokerHttpServer(broker, result_queue=result_queue)
        with TemporaryDirectory() as tmp_dir:
            with fake_media_download(tmp_dir, media_request=mr) as md:
                result = DownloadResult(
                    status=DownloadStatus(success=True),
                    media_request=mr,
                    ytdlp_data={'id': 'abc', 'title': 'Test', 'webpage_url': 'http://example.com',
                                'uploader': 'tester', 'duration': 120, 'extractor': 'youtube'},
                    file_name=md.file_path,
                )
                async with TestClient(TestServer(server.build_app())) as client:
                    resp = await client.post(
                        '/downloads',
                        json=result.model_dump(mode='json'),
                    )
                    assert resp.status == 202
        assert not result_queue.empty()
        queued = result_queue.get_nowait()
        assert str(queued.media_request.uuid) == str(mr.uuid)
        # Successful downloads are also persisted in the broker registry so
        # player checkout can find them.
        entry = await broker.get_entry(str(mr.uuid))
        assert entry.download is not None
        assert str(entry.download.media_request.uuid) == str(mr.uuid)

    async def test_invalid_body_returns_422(self):
        broker = _make_broker()
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post('/downloads', json={'not': 'a download result'})
            assert resp.status == 422


@pytest.mark.asyncio
class TestCheckout:
    async def test_checkout_returns_none_for_unknown_uuid(self):
        broker = _make_broker()
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post(
                '/requests/unknown-uuid/checkout',
                json={'guild_id': 123},
            )
            assert resp.status == 200
            data = await resp.json()
            assert data['guild_file_path'] is None

    async def test_checkout_with_valid_entry(self):
        broker = _make_broker()
        mr = _make_request()
        server = _make_server(broker)
        with TemporaryDirectory() as tmp_dir:
            with fake_media_download(tmp_dir, media_request=mr) as md:
                await broker.register_download(md)
                with TemporaryDirectory() as guild_dir:
                    async with TestClient(TestServer(server.build_app())) as client:
                        resp = await client.post(
                            f'/requests/{mr.uuid}/checkout',
                            json={'guild_id': 123, 'guild_path': guild_dir},
                        )
                        assert resp.status == 200
                        data = await resp.json()
                        assert data['guild_file_path'] is not None

    async def test_invalid_body_returns_422(self):
        broker = _make_broker()
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post('/requests/some-uuid/checkout', json={})
            assert resp.status == 422


@pytest.mark.asyncio
class TestRelease:
    async def test_release_known_entry(self):
        broker = _make_broker()
        mr = _make_request()
        await broker.register_request(mr)
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post(f'/requests/{mr.uuid}/release')
            assert resp.status == 200
            data = await resp.json()
            assert data['status'] == 'ok'
        # Entry should be gone after release
        assert await broker.get_entry(str(mr.uuid)) is None

    async def test_release_unknown_uuid_is_no_op(self):
        broker = _make_broker()
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post('/requests/nonexistent/release')
            assert resp.status == 200


@pytest.mark.asyncio
class TestRemove:
    async def test_remove_known_entry(self):
        broker = _make_broker()
        mr = _make_request()
        await broker.register_request(mr)
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post(f'/requests/{mr.uuid}/remove')
            assert resp.status == 200
            data = await resp.json()
            assert data['status'] == 'ok'
        assert await broker.get_entry(str(mr.uuid)) is None

    async def test_remove_unknown_uuid_is_no_op(self):
        broker = _make_broker()
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post('/requests/nonexistent/remove')
            assert resp.status == 200


@pytest.mark.asyncio
class TestDiscard:
    async def test_discard_drops_entry(self):
        broker = _make_broker()
        mr = _make_request()
        await broker.register_request(mr)
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post(f'/requests/{mr.uuid}/discard')
            assert resp.status == 200
        assert await broker.get_entry(str(mr.uuid)) is None


@pytest.mark.asyncio
class TestRegisterDownloadDirect:
    async def test_register_download_persists_entry_as_available(self):
        broker = _make_broker()
        mr = _make_request()
        await broker.register_request(mr)
        server = _make_server(broker)
        with TemporaryDirectory() as tmp_dir:
            with fake_media_download(tmp_dir, media_request=mr) as md:
                payload = {
                    'request': mr.model_dump(mode='json'),
                    'file_path': str(md.file_path),
                    'file_size_bytes': md.file_size_bytes,
                    'cache_hit': False,
                    'ytdl_data': {
                        'id': md.id, 'title': md.title,
                        'webpage_url': md.webpage_url, 'uploader': md.uploader,
                        'duration': md.duration, 'extractor': md.extractor,
                    },
                }
                async with TestClient(TestServer(server.build_app())) as client:
                    resp = await client.post('/downloads/register', json=payload)
                    assert resp.status == 200
        entry = await broker.get_entry(str(mr.uuid))
        assert entry is not None
        assert entry.zone.value == 'available'

    async def test_register_download_invalid_body_returns_422(self):
        broker = _make_broker()
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post('/downloads/register', json={'no_request': True})
            assert resp.status == 422


@pytest.mark.asyncio
class TestCacheEndpoints:
    async def test_check_cache_miss_returns_hit_false(self):
        broker = _make_broker()  # no video_cache wired -> always miss
        mr = _make_request()
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post('/cache/check', json=mr.model_dump(mode='json'))
            assert resp.status == 200
            assert (await resp.json()) == {'hit': False}

    async def test_check_cache_hit_returns_serialised_download(self):
        broker = _make_broker()
        mr = _make_request()
        with TemporaryDirectory() as tmp_dir:
            with fake_media_download(tmp_dir, media_request=mr) as md:
                broker.check_cache = AsyncMock(return_value=md)
                server = _make_server(broker)
                async with TestClient(TestServer(server.build_app())) as client:
                    resp = await client.post('/cache/check', json=mr.model_dump(mode='json'))
                    assert resp.status == 200
                    payload = await resp.json()
                    assert payload['hit'] is True
                    assert payload['download']['file_path'] == str(md.file_path)

    async def test_check_cache_invalid_body_returns_422(self):
        '''Garbage payload to /cache/check should fail validation cleanly.'''
        broker = _make_broker()
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post('/cache/check', json={'not': 'a media request'})
            assert resp.status == 422

    async def test_cache_cleanup_returns_removed_flag(self):
        broker = _make_broker()
        broker.cache_cleanup = AsyncMock(return_value=True)
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post('/cache/cleanup')
            assert resp.status == 200
            assert (await resp.json()) == {'removed': True}

    async def test_get_cache_count_returns_int(self):
        broker = _make_broker()
        broker.get_cache_count = AsyncMock(return_value=7)
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.get('/cache/count')
            assert resp.status == 200
            assert (await resp.json()) == {'count': 7}


async def _health_request(port: int) -> dict:
    '''Connect to a BaseHealthServer port and return the parsed JSON response body.'''
    reader, writer = await asyncio.open_connection('127.0.0.1', port)
    try:
        writer.write(b'GET / HTTP/1.0\r\nHost: localhost\r\n\r\n')
        await writer.drain()
        response = await reader.read(4096)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except OSError:
            pass
    body_bytes = response.split(b'\r\n\r\n', 1)[1]
    return json.loads(body_bytes)


async def _wait_for_tcp(port: int, timeout: float = 5.0) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        try:
            _, writer = await asyncio.open_connection('127.0.0.1', port)
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass
            return
        except OSError:
            if asyncio.get_event_loop().time() >= deadline:
                raise
            await asyncio.sleep(0.02)


@pytest.mark.asyncio


@pytest.mark.asyncio
class TestPrefetch:
    async def test_prefetch_with_uuids(self):
        broker = _make_broker()
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post(
                '/prefetch',
                json={
                    'uuids': ['uuid-1', 'uuid-2'],
                    'guild_id': 123,
                    'guild_path': None,
                    'limit': 3,
                },
            )
            assert resp.status == 200
            data = await resp.json()
            assert data['status'] == 'ok'

    async def test_invalid_body_returns_422(self):
        broker = _make_broker()
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post('/prefetch', json={'missing': 'required_fields'})
            assert resp.status == 422


@pytest.mark.asyncio
class TestBundleEndpoints:
    '''POST /bundles, POST /bundles/{uuid}/finalize, DELETE /bundles/{uuid}.'''

    async def test_create_bundle_returns_201_with_uuid(self):
        broker = _make_broker()
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post('/bundles', json={
                'guild_id': 100, 'channel_id': 200,
                'input_string': 'multi-track', 'has_search_banner': True,
            })
            assert resp.status == 201
            payload = await resp.json()
            assert 'uuid' in payload
            stored = broker.get_bundle_state(payload['uuid'])
            assert stored is not None
            assert stored.has_search_banner is True

    async def test_finalize_bundle_marks_all_requests_enqueued(self):
        broker = _make_broker()
        bundle_uuid = await broker.create_bundle(100, 200)
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post(f'/bundles/{bundle_uuid}/finalize')
            assert resp.status == 200
        assert broker.get_bundle_state(bundle_uuid).all_requests_enqueued is True

    async def test_delete_bundle_drops_state(self):
        broker = _make_broker()
        bundle_uuid = await broker.create_bundle(100, 200)
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.delete(f'/bundles/{bundle_uuid}')
            assert resp.status == 200
        assert broker.get_bundle_state(bundle_uuid) is None

    async def test_create_bundle_invalid_body_returns_422(self):
        broker = _make_broker()
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.post('/bundles', json={'no_required_fields': True})
            assert resp.status == 422

    async def test_list_bundles_for_guild_filters_by_guild(self):
        broker = _make_broker()
        a = await broker.create_bundle(100, 200)
        b = await broker.create_bundle(100, 201)
        other = await broker.create_bundle(999, 200)
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.get('/bundles?guild_id=100')
            assert resp.status == 200
            payload = await resp.json()
            assert set(payload['uuids']) == {a, b}
            resp = await client.get('/bundles?guild_id=999')
            assert (await resp.json())['uuids'] == [other]
            resp = await client.get('/bundles?guild_id=4242')
            assert (await resp.json())['uuids'] == []

    async def test_list_bundles_for_guild_missing_param_returns_422(self):
        broker = _make_broker()
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.get('/bundles')
            assert resp.status == 422


@pytest.mark.asyncio
class TestPlayerSessionEndpoints:
    '''GET /sessions, PUT /sessions/{guild_id}, DELETE /sessions/{guild_id}.'''

    @staticmethod
    def _payload(guild_id: int = 100, voice_channel_id: int = 300,
                 text_channel_id: int = 200, was_playing: bool = True) -> dict:
        return {
            'guild_id': guild_id,
            'voice_channel_id': voice_channel_id,
            'text_channel_id': text_channel_id,
            'queue': [],
            'was_playing': was_playing,
        }

    async def test_save_returns_201_and_stores(self):
        broker = _make_broker()
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.put('/sessions/100', json=self._payload())
            assert resp.status == 201
        stored = await broker.list_player_sessions()
        assert [s.guild_id for s in stored] == [100]
        assert stored[0].voice_channel_id == 300

    async def test_list_returns_stored_sessions(self):
        broker = _make_broker()
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            await client.put('/sessions/100', json=self._payload())
            await client.put('/sessions/101', json=self._payload(guild_id=101))
            resp = await client.get('/sessions')
            assert resp.status == 200
            payload = await resp.json()
        assert sorted(s['guild_id'] for s in payload['sessions']) == [100, 101]

    async def test_list_empty_returns_empty_list(self):
        server = _make_server(_make_broker())
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.get('/sessions')
            assert resp.status == 200
            assert (await resp.json())['sessions'] == []

    async def test_delete_drops_session(self):
        broker = _make_broker()
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            await client.put('/sessions/100', json=self._payload())
            resp = await client.delete('/sessions/100')
            assert resp.status == 200
        assert await broker.list_player_sessions() == []

    async def test_save_invalid_body_returns_422(self):
        server = _make_server(_make_broker())
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.put('/sessions/100', json={'no_required_fields': True})
            assert resp.status == 422

    async def test_save_guild_id_mismatch_returns_422(self):
        '''The path segment is the record's identity; a body that disagrees is a
        caller bug rather than something to resolve in favour of either side.'''
        broker = _make_broker()
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.put('/sessions/100', json=self._payload(guild_id=999))
            assert resp.status == 422
        assert await broker.list_player_sessions() == []

    async def test_non_integer_guild_id_returns_422(self):
        server = _make_server(_make_broker())
        async with TestClient(TestServer(server.build_app())) as client:
            assert (await client.put('/sessions/abc', json=self._payload())).status == 422
            assert (await client.delete('/sessions/abc')).status == 422
