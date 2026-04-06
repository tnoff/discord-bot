from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, List

from opentelemetry.trace import SpanKind


from discord_bot.database import VideoCache
from discord_bot.types.media_download import MediaDownload, media_download_attributes
from discord_bot.types.media_request import MediaRequest, media_request_attributes
from discord_bot.cogs.music_helpers import database_functions
from discord_bot.utils.sql_retry import async_retry_database_commands
from discord_bot.utils.otel import async_otel_span_wrapper, MusicVideoCacheNaming

OTEL_SPAN_PREFIX = 'music.video_cache'


class VideoCacheClient():
    '''
    DB catalog for the video cache.

    Stores and queries VideoCache records (metadata, play counts, eviction
    policy). VideoCache.base_path holds either a local file path or an S3
    object key depending on storage_type. All file operations are handled
    by MediaBroker.
    '''
    def __init__(self, max_cache_files: int, session_generator: Callable,
                 max_cache_size_bytes: int | None = None, storage_type: str = 'local'):
        self.max_cache_files: int = max_cache_files
        self.session_generator: Callable = session_generator
        self.max_cache_size_bytes: int | None = max_cache_size_bytes
        self.storage_type: str = storage_type

    async def iterate_file(self, media_download: MediaDownload) -> bool:
        '''
        Insert or update the VideoCache record for a downloaded file.
        '''
        attributes = media_download_attributes(media_download)
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.iterate_file', kind=SpanKind.INTERNAL, attributes=attributes):
            now = datetime.now(timezone.utc)
            async with self.session_generator() as db_session:
                video_cache = await async_retry_database_commands(db_session, lambda: database_functions.get_video_cache_by_url(db_session, media_download.webpage_url))
                if video_cache:
                    if video_cache.storage_type != self.storage_type:
                        # Storage type changed (or entry pre-dates this column): update
                        # base_path and storage_type to match the freshly downloaded file.
                        video_cache.base_path = str(media_download.file_path)
                        video_cache.storage_type = self.storage_type
                    video_cache.count += 1
                    video_cache.last_iterated_at = now
                    video_cache.ready_for_deletion = False
                    await async_retry_database_commands(db_session, db_session.commit)
                    return True
                cache_item = VideoCache(
                    video_id=media_download.id,
                    video_url=media_download.webpage_url,
                    title=media_download.title,
                    uploader=media_download.uploader,
                    duration=media_download.duration,
                    extractor=media_download.extractor,
                    last_iterated_at=now,
                    created_at=now,
                    base_path=str(media_download.file_path),
                    storage_type=self.storage_type,
                    count=1,
                    ready_for_deletion=False,
                    file_size_bytes=media_download.file_size_bytes,
                )
                db_session.add(cache_item)
                await async_retry_database_commands(db_session, db_session.commit)
                return True

    def __generate_source_download(self, video_cache: VideoCache, media_request: MediaRequest):
        ytdlp_data = {
            'id': video_cache.video_id,
            'title': video_cache.title,
            'uploader': video_cache.uploader,
            'duration': video_cache.duration,
            'extractor': video_cache.extractor,
            'webpage_url': video_cache.video_url,
        }
        md = MediaDownload(Path(str(video_cache.base_path)), ytdlp_data, media_request, cache_hit=True)
        md.file_size_bytes = video_cache.file_size_bytes
        return md

    async def get_webpage_url_item(self, media_request: MediaRequest) -> MediaDownload:
        '''
        Look up a VideoCache record by URL and return a MediaDownload.
        Returns None if not found or if the entry was stored under a different
        storage type (stale entry is marked for deletion so it will be evicted).
        '''
        attributes = media_request_attributes(media_request)
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.get_webpage_url', kind=SpanKind.INTERNAL, attributes=attributes):
            async with self.session_generator() as db_session:
                video_cache = await async_retry_database_commands(db_session, lambda: database_functions.get_video_cache_by_url(db_session, media_request.search_result.resolved_search_string))
                if not video_cache:
                    return None
                if video_cache.storage_type is not None and video_cache.storage_type != self.storage_type:
                    video_cache.ready_for_deletion = True
                    await async_retry_database_commands(db_session, db_session.commit)
                    return None
                return self.__generate_source_download(video_cache, media_request)

    def generate_download_from_existing(self, media_request: MediaRequest, video_cache: VideoCache) -> MediaDownload:
        '''
        Generate a source download from an existing VideoCache record.
        '''
        attributes = media_request_attributes(media_request)
        attributes[MusicVideoCacheNaming.ID.value] = video_cache.id
        return self.__generate_source_download(video_cache, media_request)

    async def remove_video_cache(self, video_cache_ids: List[int]) -> bool:
        '''
        Delete VideoCache DB records for the given IDs.

        S3 object deletion must be performed by the caller (MediaBroker)
        before invoking this method.
        '''
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.remove_video', kind=SpanKind.INTERNAL):
            async with self.session_generator() as db_session:
                for video_cache_id in video_cache_ids:
                    await async_retry_database_commands(db_session, lambda vid=video_cache_id: database_functions.delete_video_cache(db_session, vid))
            return True

    async def ready_remove(self):
        '''
        Mark the oldest excess cache entries ready_for_deletion.
        '''
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.ready_remove', kind=SpanKind.INTERNAL):
            async with self.session_generator() as db_session:
                cache_count = await async_retry_database_commands(db_session, lambda: database_functions.count_video_cache(db_session))
                num_to_remove = cache_count - self.max_cache_files
                if num_to_remove >= 1:
                    await async_retry_database_commands(db_session, lambda: database_functions.video_cache_mark_deletion(db_session, num_to_remove))
            if self.max_cache_size_bytes is not None:
                async with self.session_generator() as db_session:
                    await async_retry_database_commands(db_session, lambda: database_functions.video_cache_mark_deletion_for_size(db_session, self.max_cache_size_bytes))
            return True

    async def get_deletable_entries(self) -> list:
        '''
        Return VideoCache entries marked ready_for_deletion.
        '''
        async with self.session_generator() as db_session:
            return await async_retry_database_commands(db_session, lambda: database_functions.list_video_cache_where_delete_ready(db_session))

    async def get_cache_count(self) -> int:
        '''Return the current number of VideoCache records.'''
        async with self.session_generator() as db_session:
            return await async_retry_database_commands(db_session, lambda: database_functions.count_video_cache(db_session))
