from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from typing import Callable, List

from opentelemetry.trace import SpanKind
from sqlalchemy import asc
from sqlalchemy.orm import Session


from discord_bot.database import VideoCache, VideoCacheGuild, Guild, VideoCacheBackup
from discord_bot.utils.common import rm_tree, run_commit
from discord_bot.utils.clients.s3 import upload_file, delete_file, get_file
from discord_bot.cogs.music_helpers.common import StorageOptions
from discord_bot.cogs.music_helpers.source_download import SourceDownload, source_download_attributes
from discord_bot.cogs.music_helpers.source_dict import SourceDict, source_dict_attributes
from discord_bot.utils.sql_retry import retry_database_commands
from discord_bot.utils.otel import otel_span_wrapper, MusicVideoCacheNaming, MusicSourceDownloadNaming

OTEL_SPAN_PREFIX = 'music.video_cache'

def get_guild(db_session: Session, guild_id: str):
    '''
    Get item on id, return none if doesnt exist
    '''
    return db_session.query(Guild).filter(Guild.server_id == str(guild_id)).first()

def add_guild(db_session: Session, new_guild: Guild):
    '''
    Add new item
    '''
    db_session.add(new_guild)
    db_session.commit()

def get_video_cache_guild(db_session: Session, video_cache_id: int, guild_id: int):
    '''
    Get video cache item if exists
    '''
    return db_session.query(VideoCacheGuild).\
            filter(VideoCacheGuild.video_cache_id == video_cache_id).\
            filter(VideoCacheGuild.guild_id == guild_id).first()

def add_video_cache_guild(db_session: Session, video_cache_guild: VideoCacheGuild):
    '''
    Add new item
    '''
    db_session.add(video_cache_guild)
    db_session.commit()

def get_all_video_cache(db_session: Session):
    '''
    Get all items in table
    '''
    return db_session.query(VideoCache).all()

def get_video_cache(db_session: Session, webpage_url: str):
    '''
    Get video cache by url
    '''
    return db_session.query(VideoCache).filter(VideoCache.video_url == webpage_url).first()

def add_video_cache(db_session: Session, video_cache: VideoCache):
    '''
    Add new item
    '''
    db_session.add(video_cache)
    db_session.commit()

def query_existing_files(db_session: Session, extractor: str, video_id: str):
    '''
    Get files based on expected path
    '''
    return db_session.query(VideoCache).\
        filter(VideoCache.extractor == extractor).\
        filter(VideoCache.video_id == video_id).first()

def get_video_cache_by_id(db_session: Session, video_cache_id: int):
    '''
    Get video cache by id
    '''
    return db_session.get(VideoCache, video_cache_id)

def remove_video_cache(db_session: Session, video_cache: VideoCache):
    '''
    Remove video cache with guild caches
    '''
    for video_cache_guild in db_session.query(VideoCacheGuild).filter(VideoCacheGuild.video_cache_id == video_cache.id):
        db_session.delete(video_cache_guild)
    db_session.commit()
    db_session.delete(video_cache)
    db_session.commit()

def video_cache_count(db_session: Session):
    '''
    Get raw video count cache
    '''
    return db_session.query(VideoCache).count()

def video_cache_mark_deletion(db_session: Session, num_to_remove: int):
    '''
    Mark items for deletion based on last iterated timestamp
    '''
    for video_cache in db_session.query(VideoCache).order_by(asc(VideoCache.last_iterated_at)).limit(num_to_remove):
        video_cache.ready_for_deletion = True
    db_session.commit()

def check_video_backup_exists(db_session: Session, video_cache_id: int):
    '''
    Check if video backup exists for id
    '''
    video_backup = db_session.query(VideoCacheBackup).\
        filter(VideoCacheBackup.video_cache_id == video_cache_id).first()
    return video_backup

def create_video_cache_backup(db_session: Session, video_cache_id: int, storage: str,
                              bucket_name: str, object_path: str):
    '''
    Create video cache backup entry
    '''
    video_backup = VideoCacheBackup(video_cache_id=video_cache_id,
                                    storage=storage,
                                    bucket_name=bucket_name,
                                    object_path=object_path)
    db_session.add(video_backup)
    db_session.commit()
    return True

def delete_backup_item(db_session: Session, backup_item_id: int):
    '''
    Delete backup item
    '''
    item = db_session.query(VideoCacheBackup).get(backup_item_id)
    if not item:
        return False
    db_session.delete(item)
    db_session.commit()
    return True

class VideoCacheClient():
    '''
    Keep cache of local files
    '''
    def __init__(self, download_dir: Path, max_cache_files: int, session_generator: Callable,
                 storage_option: StorageOptions, bucket_name: str):
        '''
        Create new file cache
        download_dir    :       Dir where files are downloaded
        max_cache_files :       Maximum number of files to keep in cache
        db_session      :       DB session for cache
        storage_option  :       Storage option for backups
        bucket_name     :       Bucket Name for backups
        '''
        self.download_dir = download_dir
        self.max_cache_files = max_cache_files
        self.session_generator = session_generator
        self.storage_option = storage_option
        self.bucket_name = bucket_name

    def __ensure_guild(self, db_session: Session, guild_id: str):
        '''
        Create or find guild with id
        guild_id    : Guild(Server) ID
        '''
        guild = retry_database_commands(db_session, partial(get_guild, db_session, str(guild_id)))
        if guild:
            return guild
        guild = Guild(
            server_id=str(guild_id),
        )
        retry_database_commands(db_session, partial(add_guild, db_session, guild))
        return guild

    def __ensure_guild_video(self, db_session: Session, guild: Guild, video_cache: VideoCache):
        '''
        Ensure video cache association
        guild       : Guild object
        video_cache : Video Cache object
        '''
        video_guild_cache = retry_database_commands(db_session, partial(get_video_cache_guild, db_session, video_cache.id, guild.id))
        if video_guild_cache:
            return video_guild_cache
        video_guild_cache = VideoCacheGuild(
            video_cache_id=video_cache.id,
            guild_id=guild.id,
        )
        retry_database_commands(db_session, partial(add_video_cache_guild, db_session, video_guild_cache))
        return video_guild_cache

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
                for item in retry_database_commands(db_session, partial(get_all_video_cache, db_session)):
                    # If file doesnt exist locally, mark that we need to redownload
                    base_path = Path(str(item.base_path))
                    if not base_path.exists():
                        download_cache_items.append(item.id)
                        continue
                    existing_files.add(base_path)
                # Re-download the files
                self.object_storage_download(download_cache_items)
                # Remove any extra files
                for file_path in self.download_dir.glob('*'):
                    if file_path.is_dir():
                        rm_tree(file_path)
                        continue
                    if file_path not in existing_files:
                        file_path.unlink()

    def iterate_file(self, source_download: SourceDownload) -> bool:
        '''
        Bump file path
        source_download : All options from source download in ytdlp
        source_dict     : Original dict that called function
        '''
        attributes = source_download_attributes(source_download)
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.iterate_file', kind=SpanKind.INTERNAL, attributes=attributes):
            now = datetime.now(timezone.utc)
            with self.session_generator() as db_session:
                video_cache = retry_database_commands(db_session, partial(get_video_cache, db_session, source_download.webpage_url))
                if video_cache:
                    video_cache.count += 1
                    video_cache.last_iterated_at = now
                    # Unmark deletion here just in case
                    video_cache.ready_for_deletion = False
                    self.__ensure_guild_video(db_session, self.__ensure_guild(db_session, source_download.source_dict.guild_id), video_cache)
                    retry_database_commands(db_session, partial(run_commit, db_session))
                    return True
                cache_item = VideoCache(
                    video_id=source_download.id,
                    video_url=source_download.webpage_url,
                    title=source_download.title,
                    uploader=source_download.uploader,
                    duration=source_download.duration,
                    extractor=source_download.extractor,
                    last_iterated_at=now,
                    created_at=now,
                    base_path=str(source_download.base_path),
                    count=1,
                    ready_for_deletion=False,
                )
                retry_database_commands(db_session, partial(add_video_cache, db_session, cache_item))
                self.__ensure_guild_video(db_session, self.__ensure_guild(db_session, source_download.source_dict.guild_id), cache_item)
                return True

    def __generate_source_download(self, video_cache: VideoCache, source_dict: SourceDict):
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
        return SourceDownload(Path(str(video_cache.base_path)), ytdlp_data, source_dict)

    def get_webpage_url_item(self, source_dict: SourceDict) -> SourceDownload:
        '''
        Get item with matching webpage url
        source_dict : Source dict to create SourceFile with
        '''
        attributes = source_dict_attributes(source_dict)
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.get_webpage_url', kind=SpanKind.INTERNAL, attributes=attributes):
            with self.session_generator() as db_session:
                video_cache = retry_database_commands(db_session, partial(get_video_cache, db_session, source_dict.search_string))
                if not video_cache:
                    return None

                return self.__generate_source_download(video_cache, source_dict)

    def generate_download_from_existing(self, source_dict: SourceDict, video_cache: VideoCache) -> SourceDownload:
        '''
        Generate a source download from a file that already exists
        '''
        attributes = source_dict_attributes(source_dict)
        attributes[MusicVideoCacheNaming.ID.value] = video_cache.id
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.generate_download', kind=SpanKind.INTERNAL, attributes=attributes):
            return self.__generate_source_download(video_cache, source_dict)

    def search_existing_file(self, extractor: str, video_id: str) -> VideoCache:
        '''
        Search cache for existing files
        '''
        attributes = {
            MusicSourceDownloadNaming.VIDEO_ID.value: video_id,
            MusicSourceDownloadNaming.EXTRACTOR.value: extractor,
        }
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.search_existing', kind=SpanKind.INTERNAL, attributes=attributes):
            with self.session_generator() as db_session:
                existing = retry_database_commands(db_session, partial(query_existing_files, db_session, extractor, video_id))
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
                    video_cache = retry_database_commands(db_session, partial(get_video_cache_by_id, db_session, video_cache_id))
                    base_path = Path(video_cache.base_path)
                    base_path.unlink(missing_ok=True)
                    backup_item = retry_database_commands(db_session, partial(check_video_backup_exists, db_session, video_cache_id))
                    if backup_item:
                        delete_file(backup_item.bucket_name, backup_item.object_path)
                        retry_database_commands(db_session, partial(delete_backup_item, db_session, backup_item.id))
                    retry_database_commands(db_session, partial(remove_video_cache, db_session, video_cache))
            return True

    def ready_remove(self):
        '''
        Mark videos in cache for deletion
        '''
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.ready_remove', kind=SpanKind.INTERNAL):
            with self.session_generator() as db_session:
                cache_count = retry_database_commands(db_session, partial(video_cache_count, db_session))
                num_to_remove = cache_count - self.max_cache_files
                if num_to_remove < 1:
                    return True
                retry_database_commands(db_session, partial(video_cache_mark_deletion, db_session, num_to_remove))
                return True

    def object_storage_download(self, video_cache_ids: List[int], delete_without_backup: bool = True) -> bool:
        '''
        Download all video cache files down from object storage

        video_cache_ids: Files to re-download
        delete_without_backup: If file doesn't have backup, delete
        '''
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.object_storage_download', kind=SpanKind.INTERNAL):
            if self.storage_option not in [el.value for el in StorageOptions]:
                if delete_without_backup:
                    self.remove_video_cache(video_cache_ids)
                return False
            with self.session_generator() as db_session:
                remove_cache_videos = []
                for video_cache_id in video_cache_ids:
                    backup_item = retry_database_commands(db_session, partial(check_video_backup_exists, db_session, video_cache_id))
                    if not backup_item and delete_without_backup:
                        remove_cache_videos.append(video_cache_id)
                        continue
                    cache_file = retry_database_commands(db_session, partial(get_video_cache_by_id, db_session, video_cache_id))
                    get_file(backup_item.bucket_name, backup_item.object_path, cache_file.base_path)
                self.remove_video_cache(remove_cache_videos)
                return True

    def object_storage_backup(self, video_cache_id: int) -> bool:
        '''
        Object storage backup of video cache id

        bucket_name : Bucket name to upload to
        video_cache_id : ID of video cache file to upload
        '''
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.object_storage_backup', kind=SpanKind.INTERNAL):
            if self.storage_option not in [el.value for el in StorageOptions]:
                return False
            with self.session_generator() as db_session:
                item_exists = retry_database_commands(db_session, partial(check_video_backup_exists, db_session, video_cache_id))
                if item_exists:
                    return True
                video_cache_item = retry_database_commands(db_session, partial(get_video_cache_by_id, db_session, video_cache_id))
                if not video_cache_item.base_path:
                    return False
                upload_file(self.bucket_name, Path(video_cache_item.base_path))
                retry_database_commands(db_session, partial(create_video_cache_backup, db_session, video_cache_id, 's3', self.bucket_name, str(video_cache_item.base_path)))
                return True
