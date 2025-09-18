"""
Database functions for music-related operations.
Centralized location for all database functions that take db_session: Session as first parameter.
"""
from datetime import datetime, timezone

from sqlalchemy import asc
from sqlalchemy.orm import Session

from discord_bot.cogs.music_helpers.common import PLAYHISTORY_PREFIX

from discord_bot.database import (
    VideoCache, VideoCacheGuild, Guild, VideoCacheBackup,
    Playlist, PlaylistItem
)


# Video Cache Functions
def get_guild(db_session: Session, guild_id: str):
    """Get guild by id, return none if doesn't exist"""
    return db_session.query(Guild).filter(Guild.server_id == str(guild_id)).first()


def add_guild(db_session: Session, new_guild: Guild):
    """Add new guild"""
    db_session.add(new_guild)
    db_session.commit()


def get_video_cache_guild(db_session: Session, video_cache_id: int, guild_id: int):
    """Get video cache guild association if exists"""
    return db_session.query(VideoCacheGuild).\
            filter(VideoCacheGuild.video_cache_id == video_cache_id).\
            filter(VideoCacheGuild.guild_id == guild_id).first()


def add_video_cache_guild(db_session: Session, video_cache_guild: VideoCacheGuild):
    """Add new video cache guild association"""
    db_session.add(video_cache_guild)
    db_session.commit()


def get_all_video_cache(db_session: Session):
    """Get all video cache items"""
    return db_session.query(VideoCache).all()


def get_video_cache(db_session: Session, webpage_url: str):
    """Get video cache by url"""
    return db_session.query(VideoCache).filter(VideoCache.video_url == webpage_url).first()


def add_video_cache(db_session: Session, video_cache: VideoCache):
    """Add new video cache item"""
    db_session.add(video_cache)
    db_session.commit()


def query_existing_files(db_session: Session, extractor: str, video_id: str):
    """Get files based on extractor and video_id"""
    return db_session.query(VideoCache).\
        filter(VideoCache.extractor == extractor).\
        filter(VideoCache.video_id == video_id).first()


def get_video_cache_by_id(db_session: Session, video_cache_id: int):
    """Get video cache by id"""
    return db_session.get(VideoCache, video_cache_id)


def remove_video_cache(db_session: Session, video_cache: VideoCache):
    """Remove video cache with guild associations"""
    for video_cache_guild in db_session.query(VideoCacheGuild).filter(VideoCacheGuild.video_cache_id == video_cache.id):
        db_session.delete(video_cache_guild)
    db_session.commit()
    db_session.delete(video_cache)
    db_session.commit()


def video_cache_count(db_session: Session):
    """Get video cache count"""
    return db_session.query(VideoCache).count()


def video_cache_mark_deletion(db_session: Session, num_to_remove: int):
    """Mark items for deletion based on last iterated timestamp"""
    for video_cache in db_session.query(VideoCache).order_by(asc(VideoCache.last_iterated_at)).limit(num_to_remove):
        video_cache.ready_for_deletion = True
    db_session.commit()


def check_video_backup_exists(db_session: Session, video_cache_id: int):
    """Check if video backup exists for id"""
    return db_session.query(VideoCacheBackup).\
        filter(VideoCacheBackup.video_cache_id == video_cache_id).first()


def create_video_cache_backup(db_session: Session, video_cache_id: int, storage: str,
                              bucket_name: str, object_path: str):
    """Create video cache backup entry"""
    video_backup = VideoCacheBackup(video_cache_id=video_cache_id,
                                    storage=storage,
                                    bucket_name=bucket_name,
                                    object_path=object_path)
    db_session.add(video_backup)
    db_session.commit()
    return True


def delete_backup_item(db_session: Session, backup_item_id: int):
    """Delete backup item"""
    item = db_session.get(VideoCacheBackup, backup_item_id)
    if not item:
        return False
    db_session.delete(item)
    db_session.commit()
    return True

def list_ready_cache_files(db_session: Session):
    """List cache files ready for processing"""
    return db_session.query(VideoCache).filter(VideoCache.ready_for_deletion == True).all()


def list_non_backup_files(db_session: Session):
    """List cache files that don't have backups"""
    cache_ids_with_backup = db_session.query(VideoCacheBackup.video_cache_id).all()
    backup_ids = [row[0] for row in cache_ids_with_backup]
    return db_session.query(VideoCache).filter(~VideoCache.id.in_(backup_ids)).all()


# Playlist Functions
def find_history_playlist(db_session: Session, guild_id: str):
    """Find history playlist for guild"""
    return db_session.query(Playlist).\
        filter(Playlist.server_id == str(guild_id)).\
        filter(Playlist.is_history == True).first()


def create_history_playlist(db_session: Session, guild_id: str):
    """Create history playlist for guild"""
    playlist = Playlist(
        server_id=str(guild_id),
        name=f'{PLAYHISTORY_PREFIX}{guild_id}_{datetime.now(timezone.utc).timestamp()}',
        is_history=True,
    )
    db_session.add(playlist)
    db_session.commit()
    return playlist


def get_playlist(db_session: Session, playlist_id: int):
    """Get playlist by id"""
    return db_session.get(Playlist, playlist_id)


def list_playlists(db_session: Session, guild_id: str):
    """List all playlists for guild"""
    return db_session.query(Playlist).\
        filter(Playlist.server_id == str(guild_id)).\
        order_by(Playlist.created_at.asc())


def check_playlist_count(db_session: Session, guild_id: str):
    """Check playlist count for guild"""
    return db_session.query(Playlist).\
        filter(Playlist.server_id == str(guild_id)).\
        filter(Playlist.is_history == False).count()

def list_non_history_playlists(db_session: Session, guild_id: str, offset: int):
    """List non-history playlists for guild with offset"""
    return db_session.query(Playlist).\
        filter(Playlist.server_id == str(guild_id)).\
        filter(Playlist.is_history == False).\
        order_by(Playlist.created_at.asc()).\
        offset(offset).all()


def check_for_playlist(db_session: Session, name: str, guild_id: str):
    """Check if playlist exists with name in guild"""
    return db_session.query(Playlist).\
        filter(Playlist.name == name).\
        filter(Playlist.server_id == str(guild_id)).first()


def create_playlist(db_session: Session, name: str, guild_id: str):
    """Create new playlist"""
    playlist = Playlist(
        name=name,
        server_id=str(guild_id),
        is_history=False,
    )
    db_session.add(playlist)
    db_session.commit()
    return playlist


def delete_existing_item(db_session: Session, webpage_url: str, playlist_id: int):
    """Delete existing playlist item by URL"""
    item = db_session.query(PlaylistItem).\
        filter(PlaylistItem.video_url == webpage_url).\
        filter(PlaylistItem.playlist_id == playlist_id).first()
    if item:
        db_session.delete(item)
        db_session.commit()


def get_playlist_size(db_session: Session, playlist_id: int):
    """Get playlist size"""
    return db_session.query(PlaylistItem).\
        filter(PlaylistItem.playlist_id == playlist_id).count()


def delete_extra_items(db_session: Session, playlist_id: int, delta: int):
    """Delete extra items from playlist"""
    for item in db_session.query(PlaylistItem).\
            filter(PlaylistItem.playlist_id == playlist_id).\
            order_by(PlaylistItem.created_at.asc()).\
            limit(delta):
        db_session.delete(item)
    db_session.commit()


# Playlist Item Functions
def get_non_history_playlists(db_session: Session, guild_id: str):
    """Get non-history playlists for guild (renamed from get_playlist_items_by_guild for clarity)"""
    return db_session.query(Playlist).\
        filter(Playlist.server_id == str(guild_id)).\
        filter(Playlist.is_history == False).\
        order_by(Playlist.created_at.asc())


def get_playlist_items(db_session: Session, playlist_id: int):
    """Get playlist items by playlist id"""
    return db_session.query(PlaylistItem).\
        filter(PlaylistItem.playlist_id == playlist_id).\
        order_by(PlaylistItem.created_at.asc())

def check_existing_item(db_session: Session, playlist_id: int, video_url: str):
    """Check if item exists in playlist"""
    return db_session.query(PlaylistItem).\
        filter(PlaylistItem.playlist_id == playlist_id).\
        filter(PlaylistItem.video_url == video_url).first()


def create_new_item(db_session: Session, video_title: str, video_url: str, video_uploader: str, playlist_id: int):
    """Create new playlist item"""
    item = PlaylistItem(
        title=video_title,
        video_url=video_url,
        uploader=video_uploader,
        playlist_id=playlist_id,
    )
    db_session.add(item)
    db_session.commit()
    return item


def remove_playlist_item_remove(db_session: Session, playlist_id: int, index_id: int):
    """Remove playlist item by index"""
    items = db_session.query(PlaylistItem).\
        filter(PlaylistItem.playlist_id == playlist_id).\
        order_by(PlaylistItem.created_at.asc()).all()
    if 0 <= index_id < len(items):
        item_to_delete = items[index_id]
        db_session.delete(item_to_delete)
        db_session.commit()
        return item_to_delete
    return None


def delete_playlist(db_session: Session, playlist_id: int):
    """Delete playlist and all its items"""
    # Delete all playlist items first
    for item in db_session.query(PlaylistItem).filter(PlaylistItem.playlist_id == playlist_id):
        db_session.delete(item)

    # Delete the playlist
    playlist = db_session.get(Playlist, playlist_id)
    if playlist:
        db_session.delete(playlist)

    db_session.commit()


def rename_playlist(db_session: Session, playlist_id: int, playlist_name: str):
    """Rename playlist"""
    playlist = db_session.get(Playlist, playlist_id)
    if playlist:
        playlist.name = playlist_name
        db_session.commit()
        return True
    return False

def get_playlist_name(db_session: Session, playlist_id: int):
    """Get playlist name"""
    playlist = db_session.get(Playlist, playlist_id)
    return playlist.name if playlist else None


def playlist_update_queued(db_session: Session, playlist_id: int):
    """Update playlist as queued"""
    playlist = db_session.get(Playlist, playlist_id)
    if playlist:
        playlist.last_queued_at = datetime.now(timezone.utc)
        db_session.commit()
