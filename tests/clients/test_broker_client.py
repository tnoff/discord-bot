'''
Tests for InMemoryBrokerClient and HttpBrokerClient.
'''
from tempfile import TemporaryDirectory

import pytest
from aiohttp.test_utils import TestClient, TestServer

import aiohttp
from aiohttp import web
from discord_bot.cogs.music_helpers.common import MediaRequestLifecycleStage
from discord_bot.interfaces.broker_protocols import CheckoutResult
from discord_bot.workers.asyncio_broker import AsyncioBroker
from discord_bot.servers.broker_server import BrokerHttpServer
from discord_bot.types.download import LifecycleEvent, DownloadResult, DownloadStatus, LifecycleStatusUpdate
from discord_bot.clients.broker_client import HttpBrokerClient, InMemoryBrokerClient

from tests.helpers import fake_source_dict, fake_media_download, generate_fake_context


def _make_broker() -> AsyncioBroker:
    return AsyncioBroker()


def _make_request():
    return fake_source_dict(generate_fake_context())


# ---------------------------------------------------------------------------
# InMemoryBrokerClient
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestInMemoryBrokerClient:
    async def test_update_request_status_delegates(self):
        broker = _make_broker()
        mr = _make_request()
        await broker.register_request(mr)
        client = InMemoryBrokerClient(broker)
        await client.update_request_status(
            str(mr.uuid), LifecycleStatusUpdate(event=LifecycleEvent.IN_PROGRESS)
        )
        entry = await broker.get_entry(str(mr.uuid))
        assert entry.request.lifecycle_stage == MediaRequestLifecycleStage.IN_PROGRESS

    async def test_register_download_result_returns_media_download(self):
        broker = _make_broker()
        mr = _make_request()
        await broker.register_request(mr)
        client = InMemoryBrokerClient(broker)
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
        assert returned is not None
        assert returned.media_request is mr

    async def test_checkout_returns_local_path_checkout_result(self):
        broker = _make_broker()
        mr = _make_request()
        client = InMemoryBrokerClient(broker)
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
        client = InMemoryBrokerClient(broker)
        result = await client.checkout('nonexistent', 123)
        assert result is None

    async def test_checkout_no_guild_path(self):
        broker = _make_broker()
        mr = _make_request()
        client = InMemoryBrokerClient(broker)
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
        client = InMemoryBrokerClient(broker)
        await client.release(str(mr.uuid))
        assert await broker.get_entry(str(mr.uuid)) is None

    async def test_prefetch_delegates(self):
        broker = _make_broker()
        mr = _make_request()
        await broker.register_request(mr)
        client = InMemoryBrokerClient(broker)
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
                str(mr.uuid), LifecycleStatusUpdate(event=LifecycleEvent.IN_PROGRESS)
            )
        entry = await broker.get_entry(str(mr.uuid))
        assert entry.request.lifecycle_stage == MediaRequestLifecycleStage.IN_PROGRESS

    async def test_register_download_result_returns_none(self):
        broker = _make_broker()
        mr = _make_request()
        await broker.register_request(mr)
        server = BrokerHttpServer(broker)
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

    async def test_checkout_s3_key_response_returns_s3_checkout_result(self):
        '''An HA broker that responds with an s3_key yields CheckoutResult(s3_key,
        bucket_name) so the player downloads the file from S3 itself.'''
        async def _s3_checkout(_request):
            return web.json_response({'s3_key': 'guilds/1/track.mp3'})
        app = web.Application()
        app.router.add_post('/requests/{uuid}/checkout', _s3_checkout)
        async with TestClient(TestServer(app)) as tc:
            hc = HttpBrokerClient(str(tc.make_url('')), bucket_name='my-bucket', session=tc.session)
            result = await hc.checkout('abc', 123, 'gdir')
        assert isinstance(result, CheckoutResult)
        assert result.s3_key == 'guilds/1/track.mp3'
        assert result.bucket_name == 'my-bucket'
        assert result.local_path is None

    async def test_checkout_empty_response_returns_none(self):
        '''An empty checkout response body yields None (the if-not-data guard).'''
        async def _empty_checkout(_request):
            return web.json_response({})
        app = web.Application()
        app.router.add_post('/requests/{uuid}/checkout', _empty_checkout)
        async with TestClient(TestServer(app)) as tc:
            hc = HttpBrokerClient(str(tc.make_url('')), session=tc.session)
            assert await hc.checkout('abc', 123) is None

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

    async def test_lazy_session_creation(self):
        '''_get_session() creates a session lazily on first call.'''
        hc = HttpBrokerClient('http://localhost:9999')
        session = hc._get_session()  # pylint:disable=protected-access
        assert session is not None
        await hc.close()
