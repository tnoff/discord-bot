'''
Tests for BrokerHttpServer — the aiohttp HTTP server wrapping MediaBroker.
'''
import asyncio
import json
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import fakeredis.aioredis
import pytest
from aiohttp.test_utils import TestClient, TestServer

from discord_bot.clients.redis_client import RedisManager
from discord_bot.workers.asyncio_broker import AsyncioBroker as MediaBroker
from discord_bot.servers.broker_server import BrokerHealthServer, BrokerHttpServer, _QueueItemProxy
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
class TestBrokerHealthServer:
    async def test_health_with_db_engine_ok(self):
        '''Response includes "db": "ok" when db_engine is provided and ping succeeds.'''
        redis_client = fakeredis.aioredis.FakeRedis()
        redis_manager = RedisManager.from_client(redis_client)
        db_engine = MagicMock()
        server = BrokerHealthServer(redis_manager, port=19303, db_engine=db_engine)
        with patch.object(server, '_db_ping', new=AsyncMock(return_value=True)):
            serve_task = asyncio.create_task(server.serve())
            await _wait_for_tcp(19303)
            try:
                body = await _health_request(19303)
            finally:
                serve_task.cancel()
                try:
                    await serve_task
                except asyncio.CancelledError:
                    pass
        assert body.get('db') == 'ok'
        assert body.get('redis') == 'ok'

    async def test_health_with_db_engine_unavailable(self):
        '''Response includes "db": "unavailable" when db ping fails.'''
        redis_client = fakeredis.aioredis.FakeRedis()
        redis_manager = RedisManager.from_client(redis_client)
        db_engine = MagicMock()
        server = BrokerHealthServer(redis_manager, port=19304, db_engine=db_engine)
        with patch.object(server, '_db_ping', new=AsyncMock(return_value=False)):
            serve_task = asyncio.create_task(server.serve())
            await _wait_for_tcp(19304)
            try:
                body = await _health_request(19304)
            finally:
                serve_task.cancel()
                try:
                    await serve_task
                except asyncio.CancelledError:
                    pass
        assert body.get('db') == 'unavailable'


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
