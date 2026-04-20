'''
Tests for BrokerHttpServer — the aiohttp HTTP server wrapping MediaBroker.
'''
import asyncio
from tempfile import TemporaryDirectory

import aiohttp
import pytest
from aiohttp.test_utils import TestClient, TestServer

from discord_bot.cogs.music_helpers.media_broker import MediaBroker
from discord_bot.servers.broker_server import BrokerHttpServer, _QueueItemProxy
from discord_bot.types.download import DownloadEvent, DownloadResult, DownloadStatus

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
class TestUpdateStatus:
    async def test_valid_request_calls_broker(self):
        broker = _make_broker()
        mr = _make_request()
        await broker.register_request(mr)
        server = _make_server(broker)
        async with TestClient(TestServer(server.build_app())) as client:
            resp = await client.put(
                f'/requests/{mr.uuid}/status',
                json={'event': DownloadEvent.IN_PROGRESS.value},
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
                    'event': DownloadEvent.RETRY.value,
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
                json={'event': DownloadEvent.IN_PROGRESS.value},
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
        # broker registry should NOT have a completed download (queue path skips broker.register_download_result)
        assert (await broker.get_entry(str(mr.uuid))).download is None

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
