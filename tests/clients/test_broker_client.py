'''
Tests for InMemoryBrokerClient and HttpBrokerClient.
'''
import asyncio
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, patch

import pytest
from aiohttp.test_utils import TestClient, TestServer

import aiohttp
from discord_bot.cogs.music_helpers.common import MediaRequestLifecycleStage
from discord_bot.workers.asyncio_broker import AsyncioBroker as MediaBroker
from discord_bot.servers.broker_server import BrokerHttpServer
from discord_bot.types.download import DownloadEvent, DownloadResult, DownloadStatus, DownloadStatusUpdate
from discord_bot.clients.broker_client import CheckoutResult, HttpBrokerClient, InMemoryBrokerClient

from tests.helpers import fake_source_dict, fake_media_download, generate_fake_context


def _make_broker() -> MediaBroker:
    return MediaBroker()


def _make_request():
    return fake_source_dict(generate_fake_context())


# ---------------------------------------------------------------------------
# InMemoryBrokerClient
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestInMemoryBrokerClient:
    async def test_register_request_delegates(self):
        broker = _make_broker()
        mr = _make_request()
        client = InMemoryBrokerClient(broker, asyncio.Queue())
        await client.register_request(mr)
        entry = await broker.get_entry(str(mr.uuid))
        assert entry is not None

    async def test_update_request_status_delegates(self):
        broker = _make_broker()
        mr = _make_request()
        await broker.register_request(mr)
        client = InMemoryBrokerClient(broker, asyncio.Queue())
        await client.update_request_status(
            str(mr.uuid), DownloadStatusUpdate(event=DownloadEvent.IN_PROGRESS)
        )
        entry = await broker.get_entry(str(mr.uuid))
        assert entry.request.lifecycle_stage == MediaRequestLifecycleStage.IN_PROGRESS

    async def test_register_download_result_enqueues_and_returns_none(self):
        broker = _make_broker()
        mr = _make_request()
        await broker.register_request(mr)
        result_queue: asyncio.Queue = asyncio.Queue()
        client = InMemoryBrokerClient(broker, result_queue)
        with TemporaryDirectory() as tmp_dir:
            with fake_media_download(tmp_dir, media_request=mr) as md:
                result = DownloadResult(
                    status=DownloadStatus(success=True),
                    media_request=mr,
                    ytdlp_data={'id': 'abc', 'title': 'Test', 'webpage_url': 'http://example.com',
                                'uploader': 'tester', 'duration': 120, 'extractor': 'youtube'},
                    file_name=md.file_path,
                )
                returned = await client.register_download_result(result)
        assert returned is None
        assert not result_queue.empty()
        queued = result_queue.get_nowait()
        assert queued.media_request is mr

    async def test_checkout_returns_local_path(self):
        broker = _make_broker()
        mr = _make_request()
        client = InMemoryBrokerClient(broker, asyncio.Queue())
        with TemporaryDirectory() as tmp_dir:
            with fake_media_download(tmp_dir, media_request=mr) as md:
                await broker.register_download(md)
                with TemporaryDirectory() as guild_dir:
                    result = await client.checkout(str(mr.uuid), 123, guild_dir)
        assert isinstance(result, CheckoutResult)
        assert result.local_path is not None
        assert result.s3_key is None

    async def test_checkout_returns_none_for_unknown(self):
        broker = _make_broker()
        client = InMemoryBrokerClient(broker, asyncio.Queue())
        result = await client.checkout('nonexistent', 123)
        assert result is None

    async def test_checkout_no_guild_path(self):
        broker = _make_broker()
        mr = _make_request()
        client = InMemoryBrokerClient(broker, asyncio.Queue())
        with TemporaryDirectory() as tmp_dir:
            with fake_media_download(tmp_dir, media_request=mr) as md:
                await broker.register_download(md)
                result = await client.checkout(str(mr.uuid), 123)
        # No guild_path means no staging — checkout marks CHECKED_OUT and returns None
        assert result is None

    async def test_release_delegates(self):
        broker = _make_broker()
        mr = _make_request()
        await broker.register_request(mr)
        client = InMemoryBrokerClient(broker, asyncio.Queue())
        await client.release(str(mr.uuid))
        assert await broker.get_entry(str(mr.uuid)) is None

    async def test_prefetch_delegates(self):
        broker = _make_broker()
        mr = _make_request()
        await broker.register_request(mr)
        client = InMemoryBrokerClient(broker, asyncio.Queue())
        # prefetch is a no-op in local mode (no S3)
        await client.prefetch([], 123, None, 5)


# ---------------------------------------------------------------------------
# HttpBrokerClient — tested against a real BrokerHttpServer
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestHttpBrokerClient:
    async def test_register_request(self):
        broker = _make_broker()
        mr = _make_request()
        server = BrokerHttpServer(broker)
        async with TestClient(TestServer(server.build_app())) as tc:
            hc = HttpBrokerClient(str(tc.make_url('')), session=tc.session)
            await hc.register_request(mr)
        entry = await broker.get_entry(str(mr.uuid))
        assert entry is not None

    async def test_update_request_status(self):
        broker = _make_broker()
        mr = _make_request()
        await broker.register_request(mr)
        server = BrokerHttpServer(broker)
        async with TestClient(TestServer(server.build_app())) as tc:
            hc = HttpBrokerClient(str(tc.make_url('')), session=tc.session)
            await hc.update_request_status(
                str(mr.uuid), DownloadStatusUpdate(event=DownloadEvent.IN_PROGRESS)
            )
        entry = await broker.get_entry(str(mr.uuid))
        assert entry.request.lifecycle_stage == MediaRequestLifecycleStage.IN_PROGRESS

    async def test_register_download_result_enqueues_on_server_queue(self):
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
                async with TestClient(TestServer(server.build_app())) as tc:
                    hc = HttpBrokerClient(str(tc.make_url('')), session=tc.session)
                    returned = await hc.register_download_result(result)
        assert returned is None
        assert not result_queue.empty()
        queued = result_queue.get_nowait()
        assert str(queued.media_request.uuid) == str(mr.uuid)

    async def test_checkout_unknown_returns_none(self):
        broker = _make_broker()
        server = BrokerHttpServer(broker)
        async with TestClient(TestServer(server.build_app())) as tc:
            hc = HttpBrokerClient(str(tc.make_url('')), session=tc.session)
            result = await hc.checkout('nonexistent', 123)
        assert result is None

    async def test_checkout_with_valid_entry_returns_local_path(self):
        broker = _make_broker()
        mr = _make_request()
        server = BrokerHttpServer(broker)
        with TemporaryDirectory() as tmp_dir:
            with fake_media_download(tmp_dir, media_request=mr) as md:
                await broker.register_download(md)
                with TemporaryDirectory() as guild_dir:
                    async with TestClient(TestServer(server.build_app())) as tc:
                        hc = HttpBrokerClient(str(tc.make_url('')), session=tc.session)
                        result = await hc.checkout(str(mr.uuid), 123, guild_dir)
        assert isinstance(result, CheckoutResult)
        assert result.local_path is not None
        assert result.s3_key is None

    async def test_checkout_ha_mode_returns_s3_key(self):
        '''In HA mode the broker returns s3_key; HttpBrokerClient wraps it without downloading.'''
        broker = _make_broker()
        mr = _make_request()
        server = BrokerHttpServer(broker, ha_mode=True)
        with TemporaryDirectory() as tmp_dir:
            with fake_media_download(tmp_dir, media_request=mr) as md:
                await broker.register_download(md)
                with TemporaryDirectory() as guild_dir:
                    async with TestClient(TestServer(server.build_app())) as tc:
                        hc = HttpBrokerClient(str(tc.make_url('')), session=tc.session)
                        result = await hc.checkout(str(mr.uuid), 123, guild_dir)
        assert isinstance(result, CheckoutResult)
        assert result.s3_key is not None
        assert result.local_path is None

    async def test_release(self):
        broker = _make_broker()
        mr = _make_request()
        await broker.register_request(mr)
        server = BrokerHttpServer(broker)
        async with TestClient(TestServer(server.build_app())) as tc:
            hc = HttpBrokerClient(str(tc.make_url('')), session=tc.session)
            await hc.release(str(mr.uuid))
        assert await broker.get_entry(str(mr.uuid)) is None

    async def test_prefetch(self):
        '''prefetch with empty list is a no-op that does not raise.'''
        broker = _make_broker()
        server = BrokerHttpServer(broker)
        queue_items: list = []
        async with TestClient(TestServer(server.build_app())) as tc:
            hc = HttpBrokerClient(str(tc.make_url('')), session=tc.session)
            await hc.prefetch(queue_items, 123, None, 5)

    async def test_close_session(self):
        '''close() closes the underlying aiohttp session.'''
        session = aiohttp.ClientSession()
        hc = HttpBrokerClient('http://localhost:9999', session=session)
        await hc.close()
        assert session.closed

    async def test_close_without_session_is_safe(self):
        '''close() does not raise when no session has been created yet.'''
        hc = HttpBrokerClient('http://localhost:9999')
        await hc.close()  # should not raise

    async def test_checkout_returns_none_when_http_returns_none(self):
        '''checkout returns None when the HTTP layer returns None (non-JSON response).'''
        hc = HttpBrokerClient('http://localhost:9999')
        with patch.object(hc, '_http', new=AsyncMock(return_value=None)):
            result = await hc.checkout('some-uuid', 123)
        assert result is None

    async def test_remove(self):
        '''remove calls POST /requests/{uuid}/remove and the entry is deleted.'''
        broker = _make_broker()
        mr = _make_request()
        await broker.register_request(mr)
        server = BrokerHttpServer(broker)
        async with TestClient(TestServer(server.build_app())) as tc:
            hc = HttpBrokerClient(str(tc.make_url('')), session=tc.session)
            await hc.remove(str(mr.uuid))
        assert await broker.get_entry(str(mr.uuid)) is None
