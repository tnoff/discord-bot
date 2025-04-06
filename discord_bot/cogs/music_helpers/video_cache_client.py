from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from typing import Callable, List

from sqlalchemy import asc
from sqlalchemy.orm import Session


from discord_bot.database import VideoCache, VideoCacheGuild, Guild
from discord_bot.utils.common import rm_tree, run_commit
from discord_bot.cogs.music_helpers.source_download import SourceDownload
from discord_bot.cogs.music_helpers.source_dict import SourceDict
from discord_bot.utils.sql_retry import retry_database_commands

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

class VideoCacheClient():
    '''
    Keep cache of local files
    '''
    def __init__(self, download_dir: Path, max_cache_files: int, session_generator: Callable):
        '''
        Create new file cache
        download_dir    :       Dir where files are downloaded
        max_cache_files :       Maximum number of files to keep in cache
        db_session      :       DB session for cache
        '''
        self.download_dir = download_dir
        self.max_cache_files = max_cache_files
        self.session_generator = session_generator

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
        existing_files = set([])
        remove_cache_items = []
        with self.session_generator() as db_session:
            for item in retry_database_commands(db_session, partial(get_all_video_cache, db_session)):
                base_path = Path(str(item.base_path))
                if not base_path.exists():
                    remove_cache_items.append(item.id)
                    continue
                existing_files.add(base_path)
            # Remove cache files that don't exist anymore
            self.remove_video_cache(remove_cache_items)
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
        with self.session_generator() as db_session:
            video_cache = retry_database_commands(db_session, partial(get_video_cache, db_session, source_dict.search_string))
            if not video_cache:
                return None

            return self.__generate_source_download(video_cache, source_dict)

    def generate_download_from_existing(self, source_dict: SourceDict, video_cache: VideoCache) -> SourceDownload:
        '''
        Generate a source download from a file that already exists
        '''
        return self.__generate_source_download(video_cache, source_dict)

    def search_existing_file(self, extractor: str, video_id: str) -> VideoCache:
        '''
        Search cache for existing files
        '''
        # NOTE This assumes the current ytdlp extractor path
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
        with self.session_generator() as db_session:
            for video_cache_id in video_cache_ids:
                video_cache = retry_database_commands(db_session, partial(get_video_cache_by_id, db_session, video_cache_id))
                base_path = Path(video_cache.base_path)
                base_path.unlink(missing_ok=True)
                retry_database_commands(db_session, partial(remove_video_cache, db_session, video_cache))
        return True

    def ready_remove(self):
        '''
        Mark videos in cache for deletion
        '''
        with self.session_generator() as db_session:
            cache_count = retry_database_commands(db_session, partial(video_cache_count, db_session))
            num_to_remove = cache_count - self.max_cache_files
            if num_to_remove < 1:
                return True
            retry_database_commands(db_session, partial(video_cache_mark_deletion, db_session, num_to_remove))
            return True
