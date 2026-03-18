from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from typing import Callable, List

from opentelemetry.trace import SpanKind


from discord_bot.database import VideoCache
from discord_bot.utils.common import run_commit
from discord_bot.types.media_download import MediaDownload, media_download_attributes
from discord_bot.types.media_request import MediaRequest, media_request_attributes
from discord_bot.cogs.music_helpers import database_functions
from discord_bot.utils.sql_retry import retry_database_commands
from discord_bot.utils.otel import otel_span_wrapper, MusicVideoCacheNaming

OTEL_SPAN_PREFIX = 'music.video_cache'


class VideoCacheClient():
    '''
    DB catalog for the S3 video cache.

    Stores and queries VideoCache records (metadata, play counts, eviction
    policy). VideoCache.base_path holds the S3 object key. All S3 file
    operations are handled by MediaBroker.
    '''
    def __init__(self, max_cache_files: int, session_generator: Callable, max_cache_size_bytes: int | None = None):
        self.max_cache_files: int = max_cache_files
        self.session_generator: Callable = session_generator
        self.max_cache_size_bytes: int | None = max_cache_size_bytes

    def iterate_file(self, media_download: MediaDownload) -> bool:
        '''
        Insert or update the VideoCache record for a downloaded file.
        '''
        attributes = media_download_attributes(media_download)
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.iterate_file', kind=SpanKind.INTERNAL, attributes=attributes):
            now = datetime.now(timezone.utc)
            with self.session_generator() as db_session:
                video_cache = retry_database_commands(db_session, partial(database_functions.get_video_cache_by_url, db_session, media_download.webpage_url))
                if video_cache:
                    video_cache.count += 1
                    video_cache.last_iterated_at = now
                    video_cache.ready_for_deletion = False
                    retry_database_commands(db_session, partial(run_commit, db_session))
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
                    count=1,
                    ready_for_deletion=False,
                    file_size_bytes=media_download.file_size_bytes,
                )
                db_session.add(cache_item)
                retry_database_commands(db_session, partial(run_commit, db_session))
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

    def get_webpage_url_item(self, media_request: MediaRequest) -> MediaDownload:
        '''
        Look up a VideoCache record by URL and return a MediaDownload.
        Returns None if not found.
        '''
        attributes = media_request_attributes(media_request)
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.get_webpage_url', kind=SpanKind.INTERNAL, attributes=attributes):
            with self.session_generator() as db_session:
                video_cache = retry_database_commands(db_session, partial(database_functions.get_video_cache_by_url, db_session, media_request.search_result.resolved_search_string))
                if not video_cache:
                    return None
                return self.__generate_source_download(video_cache, media_request)

    def generate_download_from_existing(self, media_request: MediaRequest, video_cache: VideoCache) -> MediaDownload:
        '''
        Generate a source download from an existing VideoCache record.
        '''
        attributes = media_request_attributes(media_request)
        attributes[MusicVideoCacheNaming.ID.value] = video_cache.id
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.generate_download', kind=SpanKind.INTERNAL, attributes=attributes):
            return self.__generate_source_download(video_cache, media_request)

    def remove_video_cache(self, video_cache_ids: List[int]) -> bool:
        '''
        Delete VideoCache DB records for the given IDs.

        S3 object deletion must be performed by the caller (MediaBroker)
        before invoking this method.
        '''
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.remove_video', kind=SpanKind.INTERNAL):
            with self.session_generator() as db_session:
                for video_cache_id in video_cache_ids:
                    retry_database_commands(db_session, partial(database_functions.delete_video_cache, db_session, video_cache_id))
            return True

    def ready_remove(self):
        '''
        Mark the oldest excess cache entries ready_for_deletion.
        '''
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.ready_remove', kind=SpanKind.INTERNAL):
            with self.session_generator() as db_session:
                cache_count = retry_database_commands(db_session, partial(database_functions.count_video_cache, db_session))
                num_to_remove = cache_count - self.max_cache_files
                if num_to_remove >= 1:
                    retry_database_commands(db_session, partial(database_functions.video_cache_mark_deletion, db_session, num_to_remove))
            if self.max_cache_size_bytes is not None:
                with self.session_generator() as db_session:
                    retry_database_commands(db_session, partial(
                        database_functions.video_cache_mark_deletion_for_size,
                        db_session, self.max_cache_size_bytes
                    ))
            return True

    def get_deletable_entries(self) -> list:
        '''
        Return VideoCache entries marked ready_for_deletion.
        '''
        with self.session_generator() as db_session:
            return retry_database_commands(db_session, partial(database_functions.list_video_cache_where_delete_ready, db_session))
