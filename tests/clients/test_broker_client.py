'''
Tests for InMemoryBrokerClient and HttpBrokerClient.
'''
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, patch

import pytest
from aiohttp.test_utils import TestClient, TestServer
from opentelemetry.trace import SpanKind

import aiohttp
from discord_bot.cogs.music_helpers.common import MediaRequestLifecycleStage
from discord_bot.workers.asyncio_broker import AsyncioBroker as MediaBroker
from discord_bot.servers.broker_server import BrokerHttpServer
from discord_bot.types.download import LifecycleEvent, DownloadResult, DownloadStatus, LifecycleStatusUpdate
from discord_bot.clients import broker_client as broker_client_module
from discord_bot.clients.broker_client import CheckoutResult, HttpBrokerClient, InMemoryBrokerClient
from discord_bot.workers.asyncio_queues import AsyncioDownloadResultQueue

from tests.helpers import fake_source_dict, fake_media_download, generate_fake_context


def _dl_result(mr, file_name=None):
    return DownloadResult(
        status=DownloadStatus(success=True), media_request=mr,
        ytdlp_data={'id': 'abc', 'title': 'T', 'webpage_url': 'http://e/v',
                    'uploader': 'u', 'duration': 1, 'extractor': 'youtube'},
        file_name=file_name,
    )


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
            str(mr.uuid), LifecycleStatusUpdate(event=LifecycleEvent.IN_PROGRESS)
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
        # No guild_path means no local staging — the entry is marked CHECKED_OUT
        # and the CheckoutResult carries no local_path/s3_key.
        assert result.local_path is None
        assert result.s3_key is None

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
                str(mr.uuid), LifecycleStatusUpdate(event=LifecycleEvent.IN_PROGRESS)
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

    async def test_checkout_with_path_guild_dir_serializes(self):
        '''Regression: the player passes self.file_dir (a Path) as guild_path.
        A PosixPath is not JSON-serialisable, so without str()-ing it the real
        aiohttp request raised TypeError before reaching the broker (prod music
        wouldn't play after the HA cutover). Drive a real Path through the real
        session — must round-trip, not raise.'''
        broker = _make_broker()
        mr = _make_request()
        server = BrokerHttpServer(broker)
        with TemporaryDirectory() as tmp_dir:
            with fake_media_download(tmp_dir, media_request=mr) as md:
                await broker.register_download(md)
                with TemporaryDirectory() as guild_dir:
                    async with TestClient(TestServer(server.build_app())) as tc:
                        hc = HttpBrokerClient(str(tc.make_url('')), session=tc.session)
                        result = await hc.checkout(str(mr.uuid), 123, Path(guild_dir))
        assert isinstance(result, CheckoutResult)
        assert result.local_path is not None

    async def test_checkout_ha_broker_returns_s3_key(self):
        '''An HA broker returns CheckoutResult(s3_key); the server serialises it as
        s3_key and HttpBrokerClient surfaces it (with bucket_name) without downloading.'''
        broker = _make_broker()
        broker.checkout = AsyncMock(return_value=CheckoutResult(s3_key='guilds/1/x.mp3'))
        server = BrokerHttpServer(broker)
        async with TestClient(TestServer(server.build_app())) as tc:
            hc = HttpBrokerClient(str(tc.make_url('')), bucket_name='my-bucket', session=tc.session)
            result = await hc.checkout('abc', 123, 'gdir')
        assert isinstance(result, CheckoutResult)
        assert result.s3_key == 'guilds/1/x.mp3'
        assert result.bucket_name == 'my-bucket'
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

    async def test_prefetch_with_path_guild_dir_serializes(self):
        '''Regression (same PosixPath bug as checkout): the player passes a Path
        guild_path to prefetch. Must str()-serialise over HTTP rather than raise.'''
        broker = _make_broker()
        server = BrokerHttpServer(broker)
        with TemporaryDirectory() as guild_dir:
            async with TestClient(TestServer(server.build_app())) as tc:
                hc = HttpBrokerClient(str(tc.make_url('')), session=tc.session)
                await hc.prefetch([], 123, Path(guild_dir), 5)

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

    async def test_discard(self):
        '''discard calls POST /requests/{uuid}/discard and drops the entry.'''
        broker = _make_broker()
        mr = _make_request()
        await broker.register_request(mr)
        server = BrokerHttpServer(broker)
        async with TestClient(TestServer(server.build_app())) as tc:
            hc = HttpBrokerClient(str(tc.make_url('')), session=tc.session)
            await hc.discard(str(mr.uuid))
        assert await broker.get_entry(str(mr.uuid)) is None

    async def test_register_download_round_trip(self):
        '''register_download serialises a MediaDownload over the wire and
        the entry lands in the broker as AVAILABLE.'''
        broker = _make_broker()
        mr = _make_request()
        await broker.register_request(mr)
        server = BrokerHttpServer(broker)
        with TemporaryDirectory() as tmp_dir:
            with fake_media_download(tmp_dir, media_request=mr) as md:
                async with TestClient(TestServer(server.build_app())) as tc:
                    hc = HttpBrokerClient(str(tc.make_url('')), session=tc.session)
                    await hc.register_download(md)
        entry = await broker.get_entry(str(mr.uuid))
        assert entry is not None
        assert entry.zone.value == 'available'

    async def test_check_cache_miss(self):
        '''check_cache returns None when the broker has no cache hit.'''
        broker = _make_broker()
        mr = _make_request()
        server = BrokerHttpServer(broker)
        async with TestClient(TestServer(server.build_app())) as tc:
            hc = HttpBrokerClient(str(tc.make_url('')), session=tc.session)
            assert await hc.check_cache(mr) is None


@pytest.mark.asyncio
class TestHttpBrokerClientCacheAndQueue:
    '''Cache + result-queue endpoints — broken out from TestHttpBrokerClient
    so the parent class stays under pylint's too-many-public-methods limit.'''

    async def test_check_cache_hit_round_trips_media_download(self):
        '''check_cache returns a MediaDownload when the broker reports a hit.'''
        broker = _make_broker()
        mr = _make_request()
        with TemporaryDirectory() as tmp_dir:
            with fake_media_download(tmp_dir, media_request=mr) as md:
                broker.check_cache = AsyncMock(return_value=md)
                server = BrokerHttpServer(broker)
                async with TestClient(TestServer(server.build_app())) as tc:
                    hc = HttpBrokerClient(str(tc.make_url('')), session=tc.session)
                    result = await hc.check_cache(mr)
                assert result is not None
                assert str(result.file_path) == str(md.file_path)
                assert result.webpage_url == md.webpage_url

    async def test_next_result_returns_payload_when_queue_has_one(self, mocker):
        '''next_result decodes the JSON payload into a DownloadResult AND opens the
        broker.next_result span only on the result path.'''
        broker = _make_broker()
        mr = _make_request()
        await broker.register_request(mr)
        with TemporaryDirectory() as tmp_dir:
            with fake_media_download(tmp_dir, media_request=mr) as md:
                result = DownloadResult(
                    status=DownloadStatus(success=True),
                    media_request=mr,
                    ytdlp_data={'id': 'abc', 'title': 'Test', 'webpage_url': 'http://e.com',
                                'uploader': 'tester', 'duration': 120, 'extractor': 'youtube'},
                    file_name=md.file_path,
                )
                server = BrokerHttpServer(broker)
                async with TestClient(TestServer(server.build_app())) as tc:
                    hc = HttpBrokerClient(str(tc.make_url('')), session=tc.session)
                    await hc.register_download_result(result)
                    # Spy only around next_result (register_download_result opens
                    # its own span). wraps= keeps the real span behaviour intact.
                    span_spy = mocker.patch.object(
                        broker_client_module, 'async_otel_span_wrapper',
                        wraps=broker_client_module.async_otel_span_wrapper,
                    )
                    popped = await hc.next_result()
        assert popped is not None
        assert str(popped.media_request.uuid) == str(mr.uuid)
        span_spy.assert_called_once_with('broker.next_result', kind=SpanKind.CLIENT)

    async def test_next_result_returns_none_on_204(self, mocker):
        '''next_result returns None when the broker has nothing queued, and mints
        NO span on the idle 204 path (the OOM churn fix).'''
        broker = _make_broker()
        server = BrokerHttpServer(broker)
        async with TestClient(TestServer(server.build_app())) as tc:
            hc = HttpBrokerClient(str(tc.make_url('')), session=tc.session)
            span_spy = mocker.patch.object(
                broker_client_module, 'async_otel_span_wrapper',
                wraps=broker_client_module.async_otel_span_wrapper,
            )
            assert await hc.next_result() is None
        span_spy.assert_not_called()

    async def test_create_bundle_raises_when_payload_missing_uuid(self):
        '''create_bundle raises if the broker response lacks the uuid field.'''
        hc = HttpBrokerClient('http://example.invalid')
        with patch.object(hc, '_http', new=AsyncMock(return_value={})):
            with pytest.raises(RuntimeError):
                await hc.create_bundle(guild_id=1, channel_id=2)

    async def test_get_cache_count_returns_zero_when_payload_missing(self):
        '''get_cache_count defaults to 0 when the broker returns no payload.'''
        hc = HttpBrokerClient('http://example.invalid')
        with patch.object(hc, '_http', new=AsyncMock(return_value=None)):
            assert await hc.get_cache_count() == 0

    async def test_list_bundles_for_guild_returns_empty_when_payload_missing(self):
        '''list_bundles_for_guild returns [] when the broker returns no payload.'''
        hc = HttpBrokerClient('http://example.invalid')
        with patch.object(hc, '_http', new=AsyncMock(return_value=None)):
            assert await hc.list_bundles_for_guild(123) == []

    async def test_cache_cleanup(self):
        '''cache_cleanup returns the removed flag from the broker.'''
        broker = _make_broker()
        broker.cache_cleanup = AsyncMock(return_value=True)
        server = BrokerHttpServer(broker)
        async with TestClient(TestServer(server.build_app())) as tc:
            hc = HttpBrokerClient(str(tc.make_url('')), session=tc.session)
            assert await hc.cache_cleanup() is True

    async def test_get_cache_count(self):
        '''get_cache_count round-trips the integer count.'''
        broker = _make_broker()
        broker.get_cache_count = AsyncMock(return_value=42)
        server = BrokerHttpServer(broker)
        async with TestClient(TestServer(server.build_app())) as tc:
            hc = HttpBrokerClient(str(tc.make_url('')), session=tc.session)
            assert await hc.get_cache_count() == 42

    async def test_create_finalize_delete_bundle_round_trip(self):
        '''create_bundle returns a uuid; finalize / delete reach the broker.'''
        broker = _make_broker()
        server = BrokerHttpServer(broker)
        async with TestClient(TestServer(server.build_app())) as tc:
            hc = HttpBrokerClient(str(tc.make_url('')), session=tc.session)
            bundle_uuid = await hc.create_bundle(
                guild_id=100, channel_id=200,
                input_string='multi', has_search_banner=True,
            )
            assert broker.get_bundle_state(bundle_uuid) is not None
            await hc.finalize_bundle(bundle_uuid)
            assert broker.get_bundle_state(bundle_uuid).all_requests_enqueued is True
            await hc.delete_bundle(bundle_uuid)
            assert broker.get_bundle_state(bundle_uuid) is None

    async def test_list_bundles_for_guild_round_trip(self):
        '''GET /bundles?guild_id=N returns the broker's filtered uuids.'''
        broker = _make_broker()
        server = BrokerHttpServer(broker)
        a = await broker.create_bundle(100, 200)
        b = await broker.create_bundle(100, 201)
        await broker.create_bundle(999, 200)
        async with TestClient(TestServer(server.build_app())) as tc:
            hc = HttpBrokerClient(str(tc.make_url('')), session=tc.session)
            uuids = await hc.list_bundles_for_guild(100)
        assert set(uuids) == {a, b}


# ---------------------------------------------------------------------------
# InMemoryBrokerClient bundle delegations
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestInMemoryBrokerClientBundles:
    async def test_create_bundle_delegates_to_broker(self):
        '''InMemoryBrokerClient.create_bundle calls broker.create_bundle.'''
        broker = _make_broker()
        client = InMemoryBrokerClient(broker)
        bundle_uuid = await client.create_bundle(100, 200, input_string='x')
        assert broker.get_bundle_state(bundle_uuid) is not None
        assert broker.get_bundle_state(bundle_uuid).input_string == 'x'

    async def test_finalize_bundle_delegates_to_broker(self):
        broker = _make_broker()
        client = InMemoryBrokerClient(broker)
        bundle_uuid = await client.create_bundle(100, 200)
        await client.finalize_bundle(bundle_uuid)
        assert broker.get_bundle_state(bundle_uuid).all_requests_enqueued is True

    async def test_delete_bundle_delegates_to_broker(self):
        broker = _make_broker()
        client = InMemoryBrokerClient(broker)
        bundle_uuid = await client.create_bundle(100, 200)
        await client.delete_bundle(bundle_uuid)
        assert broker.get_bundle_state(bundle_uuid) is None

    async def test_list_bundles_for_guild_delegates_to_broker(self):
        broker = _make_broker()
        client = InMemoryBrokerClient(broker)
        a = await client.create_bundle(100, 200)
        b = await client.create_bundle(100, 201)
        await client.create_bundle(999, 200)
        assert set(await client.list_bundles_for_guild(100)) == {a, b}


@pytest.mark.asyncio
class TestInMemoryBrokerClientFullSurface:
    '''Cover the InMemory delegations the HTTP round-trip tests don't reach.'''

    async def test_result_queue_and_local_broker_properties(self):
        broker = _make_broker()
        q = AsyncioDownloadResultQueue()
        client = InMemoryBrokerClient(broker, q)
        assert client.result_queue is q
        assert client.local_broker is broker

    async def test_next_result_pops_then_none(self):
        broker = _make_broker()
        mr = _make_request()
        await broker.register_request(mr)
        client = InMemoryBrokerClient(broker, AsyncioDownloadResultQueue())
        with TemporaryDirectory() as tmp_dir:
            with fake_media_download(tmp_dir, media_request=mr) as md:
                await client.register_download_result(_dl_result(mr, md.file_path))
                popped = await client.next_result()
        assert popped.media_request is mr
        assert await client.next_result() is None

    async def test_register_download_remove_discard_delegate(self):
        broker = _make_broker()
        mr = _make_request()
        client = InMemoryBrokerClient(broker, AsyncioDownloadResultQueue())
        with TemporaryDirectory() as tmp_dir:
            with fake_media_download(tmp_dir, media_request=mr) as md:
                await client.register_download(md)
                assert await broker.get_entry(str(mr.uuid)) is not None
                await client.discard(str(mr.uuid))
                assert await broker.get_entry(str(mr.uuid)) is None
        await broker.register_request(mr)
        await client.remove(str(mr.uuid))
        assert await broker.get_entry(str(mr.uuid)) is None

    async def test_cache_methods_delegate(self):
        broker = _make_broker()
        broker.check_cache = AsyncMock(return_value='cached')
        broker.cache_cleanup = AsyncMock(return_value=True)
        broker.get_cache_count = AsyncMock(return_value=5)
        client = InMemoryBrokerClient(broker, AsyncioDownloadResultQueue())
        assert await client.check_cache(_make_request()) == 'cached'
        assert await client.cache_cleanup() is True
        assert await client.get_cache_count() == 5
