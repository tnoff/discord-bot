from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from typing import Callable, List

from opentelemetry.trace import SpanKind


from discord_bot.database import VideoCache, VideoCacheBackup
from discord_bot.utils.common import run_commit
from discord_bot.utils.clients.s3 import upload_file, delete_file, get_file
from discord_bot.cogs.music_helpers.common import StorageOptions
from discord_bot.cogs.music_helpers.media_download import MediaDownload, media_download_attributes
from discord_bot.cogs.music_helpers.media_request import MediaRequest, media_request_attributes
from discord_bot.cogs.music_helpers import database_functions
from discord_bot.utils.sql_retry import retry_database_commands
from discord_bot.utils.otel import otel_span_wrapper, MusicVideoCacheNaming, MusicMediaDownloadNaming

OTEL_SPAN_PREFIX = 'music.video_cache'


class VideoCacheClient():
    '''
    Keep cache of local files
    '''
    def __init__(self, download_dir: Path, max_cache_files: int, session_generator: Callable,
                 storage_option: StorageOptions, bucket_name: str, ignore_cleanup_paths: List[str] = None):
        '''
        Create new file cache
        download_dir           :       Dir where files are downloaded
        max_cache_files        :       Maximum number of files to keep in cache
        db_session             :       DB session for cache
        storage_option         :       Storage option for backups
        bucket_name            :       Bucket Name for backups
        ignore_cleanup_paths   :       List of paths to ignore during cleanup (relative to download_dir)
        '''
        self.download_dir: Path = download_dir
        self.max_cache_files: int = max_cache_files
        self.session_generator: Callable = session_generator
        self.storage_option: StorageOptions = storage_option
        self.bucket_name: str = bucket_name
        self.ignore_cleanup_paths: list[Path] = [Path(p) for p in (ignore_cleanup_paths or [])]

    @property
    def object_storage_enabled(self) -> bool:
        '''
        If object storage is enabled
        '''
        return self.bucket_name != None

    def verify_cache(self):
        '''
        Remove files in directory that are not cached
    
        '''
        # Find items that don't exist anymore
        # And get list of ones that do
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.verify_cache', kind=SpanKind.INTERNAL):
            existing_files = set([])
            download_cache_items = []
            with self.session_generator() as db_session:
                for item in retry_database_commands(db_session, partial(database_functions.list_video_cache, db_session)):
                    # If file doesnt exist locally, mark that we need to redownload
                    base_path = Path(str(item.base_path))
                    if not base_path.exists():
                        download_cache_items.append(item.id)
                        continue
                    existing_files.add(base_path)
                # Re-download the files
                self.object_storage_download(download_cache_items)
                # Remove any extra files
                for file_path in self.download_dir.glob('**/*'):
                    # Skip directories (we only care about files)
                    if file_path.is_dir():
                        continue
                    # Skip ignored paths
                    if self._should_ignore_path(file_path):
                        continue
                    # Remove uncached files
                    if file_path not in existing_files:
                        file_path.unlink()

    def _should_ignore_path(self, file_path: Path) -> bool:
        '''
        Check if a path should be ignored during cleanup

        file_path: Path to check (can be relative or absolute)
        '''
        # Convert to relative path from download_dir for comparison
        try:
            relative_path = file_path.relative_to(self.download_dir)
        except ValueError:
            # Path is not relative to download_dir, don't ignore
            return False
        # Exact match only
        return relative_path in self.ignore_cleanup_paths

    def iterate_file(self, media_download: MediaDownload) -> bool:
        '''
        Bump file path
        media_download : All options from media download in ytdlp
        media_request     : Original request that called function
        '''
        attributes = media_download_attributes(media_download)
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.iterate_file', kind=SpanKind.INTERNAL, attributes=attributes):
            now = datetime.now(timezone.utc)
            with self.session_generator() as db_session:
                video_cache = retry_database_commands(db_session, partial(database_functions.get_video_cache_by_url, db_session, media_download.webpage_url))
                if video_cache:
                    video_cache.count += 1
                    video_cache.last_iterated_at = now
                    # Unmark deletion here just in case
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
                    base_path=str(media_download.base_path),
                    count=1,
                    ready_for_deletion=False,
                )
                db_session.add(cache_item)
                retry_database_commands(db_session, partial(run_commit, db_session))
                return True

    def __generate_source_download(self, video_cache: VideoCache, media_request: MediaRequest):
        '''
        Generate source download
        '''
        ytdlp_data = {
            'id': video_cache.video_id,
            'title': video_cache.title,
            'uploader': video_cache.uploader,
            'duration': video_cache.duration,
            'extractor': video_cache.extractor,
            'webpage_url': video_cache.video_url,
        }
        return MediaDownload(Path(str(video_cache.base_path)), ytdlp_data, media_request, cache_hit=True)

    def get_webpage_url_item(self, media_request: MediaRequest) -> MediaDownload:
        '''
        Get item with matching webpage url
        media_request : Media request to create SourceFile with
        '''
        attributes = media_request_attributes(media_request)
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.get_webpage_url', kind=SpanKind.INTERNAL, attributes=attributes):
            with self.session_generator() as db_session:
                video_cache = retry_database_commands(db_session, partial(database_functions.get_video_cache_by_url, db_session, media_request.search_string))
                if not video_cache:
                    return None

                return self.__generate_source_download(video_cache, media_request)

    def generate_download_from_existing(self, media_request: MediaRequest, video_cache: VideoCache) -> MediaDownload:
        '''
        Generate a source download from a file that already exists
        '''
        attributes = media_request_attributes(media_request)
        attributes[MusicVideoCacheNaming.ID.value] = video_cache.id
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.generate_download', kind=SpanKind.INTERNAL, attributes=attributes):
            return self.__generate_source_download(video_cache, media_request)

    def search_existing_file(self, extractor: str, video_id: str) -> VideoCache:
        '''
        Search cache for existing files
        '''
        attributes = {
            MusicMediaDownloadNaming.VIDEO_ID.value: video_id,
            MusicMediaDownloadNaming.EXTRACTOR.value: extractor,
        }
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.search_existing', kind=SpanKind.INTERNAL, attributes=attributes):
            with self.session_generator() as db_session:
                existing = retry_database_commands(db_session, partial(database_functions.get_vide_cache_by_extractor_video_id, db_session, extractor, video_id))
                if existing:
                    return existing
                return None

    def remove_video_cache(self, video_cache_ids: List[int]) -> bool:
        '''
        Remove video cache ids

        video_cache_ids: List of ints
        '''
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.remove_video', kind=SpanKind.INTERNAL):
            with self.session_generator() as db_session:
                for video_cache_id in video_cache_ids:
                    video_cache = retry_database_commands(db_session, partial(database_functions.get_video_cache_by_id, db_session, video_cache_id))
                    base_path = Path(video_cache.base_path)
                    base_path.unlink(missing_ok=True)
                    backup_item = retry_database_commands(db_session, partial(database_functions.get_video_cache_backup, db_session, video_cache_id))
                    if backup_item:
                        delete_file(backup_item.bucket_name, backup_item.object_path)
                        retry_database_commands(db_session, partial(database_functions.delete_video_cache_backup, db_session, backup_item.id))
                    retry_database_commands(db_session, partial(database_functions.delete_video_cache, db_session, video_cache.id))
            return True

    def ready_remove(self):
        '''
        Mark videos in cache for deletion
        '''
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.ready_remove', kind=SpanKind.INTERNAL):
            with self.session_generator() as db_session:
                cache_count = retry_database_commands(db_session, partial(database_functions.count_video_cache, db_session))
                num_to_remove = cache_count - self.max_cache_files
                if num_to_remove < 1:
                    return True
                retry_database_commands(db_session, partial(database_functions.video_cache_mark_deletion, db_session, num_to_remove))
                return True

    def object_storage_download(self, video_cache_ids: List[int], delete_without_backup: bool = True) -> bool:
        '''
        Download all video cache files down from object storage

        video_cache_ids: Files to re-download
        delete_without_backup: If file doesn't have backup, delete
        '''
        if not self.object_storage_enabled:
            if delete_without_backup:
                self.remove_video_cache(video_cache_ids)
            return False
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.object_storage_download', kind=SpanKind.INTERNAL):
            with self.session_generator() as db_session:
                remove_cache_videos = []
                for video_cache_id in video_cache_ids:
                    backup_item = retry_database_commands(db_session, partial(database_functions.get_video_cache_backup, db_session, video_cache_id))
                    if not backup_item and delete_without_backup:
                        remove_cache_videos.append(video_cache_id)
                        continue
                    cache_file = retry_database_commands(db_session, partial(database_functions.get_video_cache_by_id, db_session, video_cache_id))
                    get_file(backup_item.bucket_name, backup_item.object_path, cache_file.base_path)
                self.remove_video_cache(remove_cache_videos)
                return True

    def object_storage_backup(self, video_cache_id: int) -> bool:
        '''
        Object storage backup of video cache id

        bucket_name : Bucket name to upload to
        video_cache_id : ID of video cache file to upload
        '''
        if not self.object_storage_enabled:
            return False
        with self.session_generator() as db_session:
            item_exists = retry_database_commands(db_session, partial(database_functions.get_video_cache_backup, db_session, video_cache_id))
            if item_exists:
                return True
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.object_storage_backup', kind=SpanKind.INTERNAL):
            with self.session_generator() as db_session:
                video_cache_item = retry_database_commands(db_session, partial(database_functions.get_video_cache_by_id, db_session, video_cache_id))
                if not video_cache_item.base_path:
                    return False
                upload_file(self.bucket_name, Path(video_cache_item.base_path))
                video_backup = VideoCacheBackup(video_cache_id=video_cache_id,
                                                storage='s3',
                                                bucket_name=self.bucket_name,
                                                object_path=str(video_cache_item.base_path))
                db_session.add(video_backup)
                retry_database_commands(db_session, partial(run_commit, db_session))
                return True
