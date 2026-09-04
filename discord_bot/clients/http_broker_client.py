'''
HTTP broker client — the HA half of the cog-facing broker surface.

Split out of clients/broker_client.py so worker pods can talk to the broker
without importing InMemoryBrokerClient, which pulls in MediaBrokerBase and with
it the broker ENGINE's dependencies (sqlalchemy via VideoCacheClient, boto3 via
integrations.s3).  The bot and downloader images carry those anyway; the search
pod installs the slim [search] extra and does not, so for it the split is the
difference between starting and an ImportError.  Same discipline that keeps
sqlalchemy off the dispatcher, and the same split interfaces/result_queue.py
already made.

Re-exported from clients/broker_client.py, so `from
discord_bot.clients.broker_client import HttpBrokerClient` keeps working.
'''
import logging
from pathlib import Path

import aiohttp
from opentelemetry.trace import SpanKind

from discord_bot.clients.http_client_base import HttpClientMixin
from discord_bot.clients.http_player_session import HttpPlayerSessionMixin
from discord_bot.types.checkout_result import CheckoutResult
from discord_bot.types.download import DownloadResult, LifecycleStatusUpdate
from discord_bot.types.media_download import (MediaDownload, media_download_from_dict,
                                              media_download_to_dict)
from discord_bot.types.media_request import MediaRequest
from discord_bot.types.search_resolution import SearchResolution
from discord_bot.utils.otel import async_otel_span_wrapper

logger = logging.getLogger(__name__)

# A broker that 404s a queue route is running a build from before that route
# existed — the two pods roll independently, so this is an expected (and
# self-resolving) window during a deploy, not a client error.
_PEER_ROUTE_MISSING_STATUS = 404


class HttpBrokerClient(HttpClientMixin, HttpPlayerSessionMixin):
    '''
    BrokerClient that forwards calls to a remote BrokerHttpServer over HTTP.
    Used when the broker runs in a separate process.

    In HA mode checkout returns a CheckoutResult with s3_key set; the caller
    (MusicPlayer) downloads the file from S3 before playback.

    next_result polls the remote broker for the next bot-ready DownloadResult,
    replacing the local-queue side-channel used in single-process mode.
    '''
    def __init__(self, base_url: str, bucket_name: str | None = None,
                 session: aiohttp.ClientSession | None = None):
        self._base_url = base_url.rstrip('/')
        self._bucket_name = bucket_name
        self._session = session

    async def register_request(self, media_request: MediaRequest) -> None:
        '''POST /requests/{uuid} — register a new MediaRequest with the remote broker.'''
        async with async_otel_span_wrapper(
            'broker.register_request', kind=SpanKind.CLIENT,
            attributes={'music.media_request.uuid': str(media_request.uuid)},
        ):
            await self._http('POST', f'{self._base_url}/requests/{media_request.uuid}',
                             media_request.model_dump(mode='json'))

    async def update_request_status(self, uuid: str, update: LifecycleStatusUpdate) -> None:
        '''PUT /requests/{uuid}/status.'''
        async with async_otel_span_wrapper(
            'broker.update_status', kind=SpanKind.CLIENT,
            attributes={'music.media_request.uuid': uuid},
        ):
            await self._http('PUT', f'{self._base_url}/requests/{uuid}/status', update.model_dump())

    async def register_download_result(self, result: DownloadResult) -> MediaDownload | None:
        '''POST /downloads — the broker stores success entries and pushes every
        result onto its bot-ready queue.  Consumers fetch via next_result.'''
        async with async_otel_span_wrapper('broker.register_download', kind=SpanKind.CLIENT):
            await self._http('POST', f'{self._base_url}/downloads', result.model_dump(mode='json'))
        return None

    def _log_missing_route(self, status: int, route: str) -> None:
        '''Log a peer-not-upgraded 404 once per occurrence, at WARNING.

        Mirrors the download client's status poller, which already logs an
        unreachable peer at WARNING and retries rather than raising.
        '''
        if status == _PEER_ROUTE_MISSING_STATUS:
            logger.warning('Broker has no %s route yet (peer not upgraded); treating as empty', route)

    async def next_result(self) -> DownloadResult | None:
        '''GET /results/next — returns the next ready DownloadResult, or None
        when the broker has nothing in the bot-ready queue (HTTP 204).

        A 404 is treated as empty for the same reason as next_search_result: the
        broker and this pod roll independently, so a route the peer doesn't have
        yet is a deploy skew, not a fatal error.

        The empty (204) path intentionally mints NO span: this endpoint is polled
        ~1/second even while idle, and a span per empty poll churns ~86k
        allocations/day (glibc arena fragmentation → OOM). The broker.next_result
        span is only opened once an actual result is being parsed.'''
        session = self._get_session()
        async with session.get(f'{self._base_url}/results/next',
                               headers=self._trace_headers()) as resp:
            if resp.status in (204, _PEER_ROUTE_MISSING_STATUS):
                self._log_missing_route(resp.status, '/results/next')
                return None
            resp.raise_for_status()
            payload = await resp.json()
            async with async_otel_span_wrapper('broker.next_result', kind=SpanKind.CLIENT):
                return DownloadResult.model_validate(payload)

    async def register_search_result(self, resolution: SearchResolution) -> None:
        '''POST /search-results — the broker pushes the resolution onto its
        bot-ready search-result queue.  Consumers fetch via next_search_result.

        A 404 means the broker peer predates this route (mid-rolling-deploy) and
        is treated as a no-op, not an error — see next_search_result.'''
        async with async_otel_span_wrapper('broker.register_search_result', kind=SpanKind.CLIENT):
            try:
                await self._http('POST', f'{self._base_url}/search-results',
                                 resolution.model_dump(mode='json'))
            except aiohttp.ClientResponseError as error:
                if error.status != _PEER_ROUTE_MISSING_STATUS:
                    raise
                logger.warning('Broker has no /search-results route (peer not upgraded yet); '
                               'dropping resolution for %s', resolution.media_request.uuid)

    async def next_search_result(self) -> SearchResolution | None:
        '''GET /search-results/next — returns the next ready SearchResolution, or
        None when the broker has nothing in the queue (HTTP 204).

        A **404 is treated as "empty"**, not as an error.  The two pods roll
        independently, so during a deploy the Service can still route this GET to
        a broker that predates the route — and the old behaviour (blanket
        raise_for_status) turned that ~20 s skew into five loop errors and a dead
        search-result consumer (docs findings/2026-07-31).  Returning None lets
        the loop idle until the upgraded broker is the only endpoint left.

        Like next_result, the empty (204) path mints NO span: this endpoint is
        polled ~1/second even while idle, and a span per empty poll churns ~86k
        allocations/day (glibc arena fragmentation → OOM). The span is only
        opened once an actual resolution is being parsed.'''
        session = self._get_session()
        async with session.get(f'{self._base_url}/search-results/next',
                               headers=self._trace_headers()) as resp:
            if resp.status in (204, _PEER_ROUTE_MISSING_STATUS):
                self._log_missing_route(resp.status, '/search-results/next')
                return None
            resp.raise_for_status()
            payload = await resp.json()
            async with async_otel_span_wrapper('broker.next_search_result', kind=SpanKind.CLIENT):
                return SearchResolution.model_validate(payload)

    async def checkout(self, uuid: str, guild_id: int, guild_path: str | None = None) -> CheckoutResult | None:
        '''
        POST /requests/{uuid}/checkout — returns a CheckoutResult or None.

        Non-HA brokers stage the file themselves and respond with guild_file_path
        (-> CheckoutResult(local_path=...)). HA brokers respond with an s3_key
        (-> CheckoutResult(s3_key=...)) and leave the S3 download to the caller.
        '''
        body: dict = {'guild_id': guild_id}
        if guild_path:
            # str() it: the player passes self.file_dir (a Path), and aiohttp's
            # json= can't serialise a PosixPath. The broker server takes the
            # string and rebuilds the Path on its side.
            body['guild_path'] = str(guild_path)
        async with async_otel_span_wrapper(
            'broker.checkout', kind=SpanKind.CLIENT,
            attributes={'music.media_request.uuid': uuid, 'music.guild_id': guild_id},
        ):
            data = await self._http('POST', f'{self._base_url}/requests/{uuid}/checkout', body)
            if not data:
                return None
            s3_key = data.get('s3_key')
            if s3_key:
                return CheckoutResult(s3_key=s3_key, bucket_name=self._bucket_name)
            guild_file_path = data.get('guild_file_path')
            if guild_file_path:
                return CheckoutResult(local_path=Path(guild_file_path))
            return None

    async def release(self, uuid: str) -> None:
        '''POST /requests/{uuid}/release.'''
        async with async_otel_span_wrapper(
            'broker.release', kind=SpanKind.CLIENT,
            attributes={'music.media_request.uuid': uuid},
        ):
            await self._http('POST', f'{self._base_url}/requests/{uuid}/release')

    async def remove(self, uuid: str) -> None:
        '''POST /requests/{uuid}/remove.'''
        async with async_otel_span_wrapper(
            'broker.remove', kind=SpanKind.CLIENT,
            attributes={'music.media_request.uuid': uuid},
        ):
            await self._http('POST', f'{self._base_url}/requests/{uuid}/remove')

    async def discard(self, uuid: str) -> None:
        '''POST /requests/{uuid}/discard — drops entry and underlying file.'''
        async with async_otel_span_wrapper(
            'broker.discard', kind=SpanKind.CLIENT,
            attributes={'music.media_request.uuid': uuid},
        ):
            await self._http('POST', f'{self._base_url}/requests/{uuid}/discard')

    async def register_download(self, media_download: MediaDownload) -> None:
        '''POST /downloads/register — persist a downloaded MediaDownload.'''
        async with async_otel_span_wrapper(
            'broker.register_download', kind=SpanKind.CLIENT,
            attributes={'music.media_request.uuid': str(media_download.media_request.uuid)},
        ):
            await self._http(
                'POST', f'{self._base_url}/downloads/register',
                media_download_to_dict(media_download),
            )

    async def check_cache(self, media_request) -> MediaDownload | None:
        '''POST /cache/check — broker looks up its VideoCache for the request URL.'''
        async with async_otel_span_wrapper(
            'broker.check_cache', kind=SpanKind.CLIENT,
            attributes={'music.media_request.uuid': str(media_request.uuid)},
        ):
            payload = await self._http(
                'POST', f'{self._base_url}/cache/check',
                media_request.model_dump(mode='json'),
            )
        if not payload or not payload.get('hit'):
            return None
        return media_download_from_dict(payload['download'], media_request)

    async def cache_cleanup(self) -> bool:
        '''POST /cache/cleanup — broker evicts stale cache entries.'''
        async with async_otel_span_wrapper('broker.cache_cleanup', kind=SpanKind.CLIENT):
            payload = await self._http('POST', f'{self._base_url}/cache/cleanup')
        return bool(payload and payload.get('removed'))

    async def get_cache_count(self) -> int:
        '''GET /cache/count — current entry count in the broker's VideoCache.'''
        async with async_otel_span_wrapper('broker.get_cache_count', kind=SpanKind.CLIENT):
            payload = await self._http('GET', f'{self._base_url}/cache/count')
        if not payload:
            return 0
        return int(payload.get('count', 0))

    async def prefetch(self, queue_items: list, guild_id: int, guild_path: str | None, limit: int) -> None:
        '''POST /prefetch — sends UUIDs extracted from queue_items.'''
        uuids = [str(item.media_request.uuid) for item in queue_items]
        async with async_otel_span_wrapper(
            'broker.prefetch', kind=SpanKind.CLIENT,
            attributes={'music.guild_id': guild_id, 'music.prefetch_limit': limit},
        ):
            await self._http('POST', f'{self._base_url}/prefetch', {
                # str() guild_path — it arrives as a Path (self.file_dir) and a
                # PosixPath isn't JSON-serialisable for the HTTP body.
                'uuids': uuids, 'guild_id': guild_id,
                'guild_path': str(guild_path) if guild_path else None, 'limit': limit,
            })

    async def create_bundle(self, guild_id: int, channel_id: int,
                            input_string: str | None = None,
                            has_search_banner: bool = False) -> str:
        '''POST /bundles — broker creates a new bundle and returns its uuid.'''
        async with async_otel_span_wrapper('broker.create_bundle', kind=SpanKind.CLIENT):
            payload = await self._http('POST', f'{self._base_url}/bundles', {
                'guild_id': guild_id, 'channel_id': channel_id,
                'input_string': input_string, 'has_search_banner': has_search_banner,
            })
        if payload is None or 'uuid' not in payload:
            raise RuntimeError('broker create_bundle returned no uuid')
        return payload['uuid']

    async def finalize_bundle(self, bundle_uuid: str) -> None:
        '''POST /bundles/{uuid}/finalize.'''
        async with async_otel_span_wrapper(
            'broker.finalize_bundle', kind=SpanKind.CLIENT,
            attributes={'music.bundle.uuid': bundle_uuid},
        ):
            await self._http('POST', f'{self._base_url}/bundles/{bundle_uuid}/finalize')

    async def delete_bundle(self, bundle_uuid: str) -> None:
        '''DELETE /bundles/{uuid}.'''
        async with async_otel_span_wrapper(
            'broker.delete_bundle', kind=SpanKind.CLIENT,
            attributes={'music.bundle.uuid': bundle_uuid},
        ):
            await self._http('DELETE', f'{self._base_url}/bundles/{bundle_uuid}')

    async def list_bundles_for_guild(self, guild_id: int) -> list[str]:
        '''GET /bundles?guild_id=N — returns bundle uuids for the guild.'''
        async with async_otel_span_wrapper(
            'broker.list_bundles_for_guild', kind=SpanKind.CLIENT,
            attributes={'music.guild_id': guild_id},
        ):
            payload = await self._http('GET', f'{self._base_url}/bundles?guild_id={guild_id}')
        if payload is None:
            return []
        return list(payload.get('uuids', []))
