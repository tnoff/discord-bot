'''
Bot-side VideoCacheStore that forwards to the db pod.

The mirror of servers/database_server.py's video-cache routes, and the fourth
and last HTTP implementation of a persistence Protocol. Satisfies the same
interfaces.database_protocols.VideoCacheStore that VideoCacheClient does, so the
broker -- which annotates against the Protocol -- selects one or the other with
a constructor change and nothing else.

**Inert.** Nothing constructs this yet; the broker still builds the in-process
client, and will until MR 4's cutover.

**Its own module**, for the reason the three stores before it established: the
in-process client imports SQLAlchemy models at module scope, so co-locating them
would pull SQLAlchemy into whichever process imported this one.

**MediaDownload was the open question, and the answer was already written.**
This group is the only one whose Protocol names a domain object rather than a
view type, which looked like it would need a new wire type or a Protocol change.
It needs neither. MediaDownload stores no raw yt-dlp dict -- `ytdl_data` is an
InitVar consumed in __post_init__ into six scalars -- and
clients/http_broker_client.py had been sending exactly that shape over HTTP
since the broker split. Those two helpers now live beside the type as
media_download_to_dict / media_download_from_dict and are used by both.

**The caller's own MediaRequest is reattached on the way back.** The response
carries the request it was queried with, but a rebuilt copy would be a distinct
object with a fresh `uuid`, and the cog tracks a download by the uuid of the
request it submitted. media_download_from_dict takes the caller's instance for
that reason.

**Config stays on the far side.** `ready_remove` sends no ceiling and
`get_webpage_url_item` sends no storage type, unlike PlaylistStore's
`add_items`, which sends `max_size`. The difference is whose number it is: a
playlist's size limit belongs to the guild making the request, while
`max_cache_files` and `storage_type` describe the cache the persistence tier
owns. Sending them would let two callers disagree about one catalog's policy.
'''
import logging
from typing import List

from discord_bot.clients.http_store_base import HttpStoreBase
from discord_bot.types.media_download import (MediaDownload, media_download_from_dict,
                                              media_download_to_dict)
from discord_bot.types.media_request import MediaRequest
from discord_bot.types.video_cache import VideoCacheEntry
from discord_bot.utils.otel import MusicMediaDownloadNaming

logger = logging.getLogger(__name__)


class HttpVideoCacheStore(HttpStoreBase):
    '''Forwards the video-cache catalog's six calls to a remote db pod.'''

    SPAN_PREFIX = 'video_cache_store'
    ROUTE_PREFIX = '/database/video_cache'

    async def iterate_file(self, media_download: MediaDownload) -> bool:
        '''
        Record a completed download: insert a row, or bump an existing one.

        media_download : The finished download to catalog
        '''
        async with self._span('iterate_file',
                              {MusicMediaDownloadNaming.VIDEO_URL.value: media_download.webpage_url}):
            return await self._call('iterate_file',
                                    {'media_download': media_download_to_dict(media_download)})

    async def get_webpage_url_item(self, media_request: MediaRequest) -> MediaDownload | None:
        '''
        Return a cache hit for the request's resolved URL, or None.

        None is the miss answer and covers both "no such row" and "the row was
        written under a different storage_type" -- the far side flags the latter
        for eviction. Neither is an error, so neither raises: a null result is a
        200 with a null body, not a 404, because 404 is what an unconfigured
        route group answers.

        media_request : The request whose resolved search string to look up
        '''
        async with self._span('get_webpage_url_item',
                              {MusicMediaDownloadNaming.VIDEO_URL.value:
                               media_request.search_result.resolved_search_string}):
            result = await self._call('get_webpage_url_item',
                                      {'media_request': media_request.model_dump(mode='json')})
            if result is None:
                return None
            return media_download_from_dict(result, media_request)

    async def remove_video_cache(self, video_cache_ids: List[int]) -> bool:
        '''
        Delete catalog rows by id.

        The whole list is one request: the caller has already deleted the S3
        objects and is reconciling the catalog to match, so a per-id round trip
        would be one request per evicted file on a path that evicts in batches.

        video_cache_ids : Row ids to delete
        '''
        async with self._span('remove_video_cache', {'video_cache.count': len(video_cache_ids)}):
            return await self._call('remove_video_cache',
                                    {'video_cache_ids': list(video_cache_ids)})

    async def ready_remove(self) -> bool:
        '''
        Apply the eviction policy, flagging excess rows `ready_for_deletion`.

        Marks only. The count-then-mark this runs is two queries against one
        catalog and stays one request, because split across the wire another
        pod's insert could land between them and the mark would be computed from
        a count that is already wrong.
        '''
        async with self._span('ready_remove'):
            return await self._call('ready_remove')

    async def get_deletable_entries(self) -> List[VideoCacheEntry]:
        '''Return the rows currently flagged `ready_for_deletion`.'''
        async with self._span('get_deletable_entries'):
            result = await self._call('get_deletable_entries')
            return [VideoCacheEntry.model_validate(entry) for entry in result]

    async def get_cache_count(self) -> int:
        '''Return the number of rows in the catalog.'''
        async with self._span('get_cache_count'):
            return await self._call('get_cache_count')
