"""
Database functions for music-related operations.
Centralized location for all database functions that take db_session: Session as first parameter.
"""
from datetime import datetime, timezone

from sqlalchemy import asc
from sqlalchemy.orm import Session

from discord_bot.database import (
    VideoCache, VideoCacheGuild, Guild, VideoCacheBackup,
    Playlist, PlaylistItem, GuildVideoAnalytics
)

#
# Guild Analytics Functions
#

def ensure_guild_video_analytics(db_session: Session, guild_id: str):
    '''
    Ensure guild video analytics table exists
    '''
    guild = ensure_guild(db_session, guild_id)
    existing = db_session.query(GuildVideoAnalytics).filter(GuildVideoAnalytics.guild_id == guild.id).first()
    if existing:
        return existing
    now_timestamp = datetime.now(timezone.utc)
    new_row = GuildVideoAnalytics(
        guild_id=guild.id,
        total_plays=0,
        cached_plays=0,
        total_duration_seconds=0,
        created_at=now_timestamp,
        updated_at=now_timestamp,
    )
    db_session.add(new_row)
    db_session.commit()
    return new_row

#
# Guild Functions
#

def ensure_guild(db_session: Session, guild_id: str):
    '''
    Find existing guild or create new one
    '''
    existing = db_session.query(Guild).filter(Guild.server_id == str(guild_id)).first()
    if existing:
        return existing
    new_guild = Guild(server_id=guild_id)
    db_session.add(new_guild)
    db_session.commit()
    return new_guild


#
# VideoCacheGuild Functions
#

def ensure_video_cache_guild(db_session: Session, guild_id: str, video_cache: VideoCache):
    '''
    Find existing video cache or create new one
    '''
    guild = ensure_guild(db_session, guild_id)
    video_cache_guild = db_session.query(VideoCacheGuild).\
                            filter(VideoCacheGuild.guild_id == guild.id).\
                            filter(VideoCacheGuild.video_cache_id == video_cache.id).first()
    if video_cache_guild:
        return video_cache_guild
    video_cache_guild = VideoCacheGuild(video_cache_id=video_cache.id, guild_id=guild.id)
    db_session.add(video_cache_guild)
    db_session.commit()
    return video_cache_guild

#
# VideoCache Functions
#

def list_video_cache(db_session: Session):
    """Get all video cache items"""
    return db_session.query(VideoCache).all()

def list_video_cache_where_delete_ready(db_session: Session):
    """List cache files ready for processing"""
    return db_session.query(VideoCache).filter(VideoCache.ready_for_deletion == True).all()

def get_video_cache_by_url(db_session: Session, webpage_url: str):
    """Get video cache by url"""
    return db_session.query(VideoCache).filter(VideoCache.video_url == webpage_url).first()

def get_vide_cache_by_extractor_video_id(db_session: Session, extractor: str, video_id: str):
    """Get files based on extractor and video_id"""
    return db_session.query(VideoCache).\
        filter(VideoCache.extractor == extractor).\
        filter(VideoCache.video_id == video_id).first()


def get_video_cache_by_id(db_session: Session, video_cache_id: int):
    """Get video cache by id"""
    return db_session.get(VideoCache, video_cache_id)

def delete_video_cache(db_session: Session, video_cache: VideoCache):
    """Remove video cache with guild associations"""
    db_session.query(VideoCacheGuild).filter(VideoCacheGuild.video_cache_id == video_cache.id).delete()
    db_session.commit()
    db_session.delete(video_cache)
    db_session.commit()


def count_video_cache(db_session: Session):
    """Get video cache count"""
    return db_session.query(VideoCache).count()


def video_cache_mark_deletion(db_session: Session, num_to_remove: int):
    """Mark items for deletion based on last iterated timestamp"""
    for video_cache in db_session.query(VideoCache).order_by(asc(VideoCache.last_iterated_at)).limit(num_to_remove):
        video_cache.ready_for_deletion = True
    db_session.commit()


#
# VideoCacheBackup Functions
#


def list_video_cache_where_no_backup(db_session: Session):
    """List cache files that don't have backups"""
    cache_ids_with_backup = db_session.query(VideoCacheBackup.video_cache_id).all()
    backup_ids = [row[0] for row in cache_ids_with_backup]
    return db_session.query(VideoCache).filter(~VideoCache.id.in_(backup_ids)).all()

def get_video_cache_backup(db_session: Session, video_cache_id: int):
    """Check if video backup exists for id"""
    return db_session.query(VideoCacheBackup).\
        filter(VideoCacheBackup.video_cache_id == video_cache_id).first()


def delete_video_cache_backup(db_session: Session, backup_item_id: int):
    """Delete backup item"""
    item = db_session.get(VideoCacheBackup, backup_item_id)
    if not item:
        return False
    db_session.delete(item)
    db_session.commit()
    return True


#
# Playlist Functions
#


# History Functions

def get_history_playlist(db_session: Session, guild_id: str):
    """Find history playlist for guild"""
    return db_session.query(Playlist).\
        filter(Playlist.server_id == str(guild_id)).\
        filter(Playlist.is_history == True).first()


# Regular Playlist Functions

def list_playlist_non_history(db_session: Session, guild_id: str, offset: int):
    """List non-history playlists for guild with offset"""
    return db_session.query(Playlist).\
        filter(Playlist.server_id == str(guild_id)).\
        filter(Playlist.is_history == False).\
        order_by(Playlist.created_at.asc()).\
        offset(offset).all()

def get_playlist(db_session: Session, playlist_id: int):
    """Get playlist by id"""
    return db_session.get(Playlist, playlist_id)

def playlist_count(db_session: Session, guild_id: str):
    """Check playlist count for guild"""
    return db_session.query(Playlist).\
        filter(Playlist.server_id == str(guild_id)).\
        filter(Playlist.is_history == False).count()

def get_playlist_by_name_and_guild(db_session: Session, name: str, guild_id: str):
    """Check if playlist exists with name in guild"""
    return db_session.query(Playlist).\
        filter(Playlist.name == name).\
        filter(Playlist.server_id == str(guild_id)).first()

def get_playlist_size(db_session: Session, playlist_id: int):
    """Get playlist size"""
    return db_session.query(PlaylistItem).\
        filter(PlaylistItem.playlist_id == playlist_id).count()

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


def update_playlist_queued_at(db_session: Session, playlist_id: int):
    """Update playlist as queued"""
    playlist = db_session.get(Playlist, playlist_id)
    if playlist:
        playlist.last_queued_at = datetime.now(timezone.utc)
        db_session.commit()

#
# PlaylistItem Functions
#


def delete_playlist_item_by_url(db_session: Session, webpage_url: str, playlist_id: int):
    """Delete existing playlist item by URL"""
    item = db_session.query(PlaylistItem).\
        filter(PlaylistItem.video_url == webpage_url).\
        filter(PlaylistItem.playlist_id == playlist_id).first()
    if item:
        db_session.delete(item)
        db_session.commit()


def delete_playlist_item_limit(db_session: Session, playlist_id: int, delta: int):
    """Delete extra items from playlist"""
    # Cant delete directly with limit called
    for item in db_session.query(PlaylistItem).\
            filter(PlaylistItem.playlist_id == playlist_id).\
            order_by(PlaylistItem.created_at.asc()).\
            limit(delta):
        db_session.delete(item)
    db_session.commit()

def list_playlist_items(db_session: Session, playlist_id: int):
    """Get playlist items by playlist id"""
    return db_session.query(PlaylistItem).\
        filter(PlaylistItem.playlist_id == playlist_id).\
        order_by(PlaylistItem.created_at.asc())

def get_playlist_item_by_url(db_session: Session, playlist_id: int, video_url: str):
    """Check if item exists in playlist"""
    return db_session.query(PlaylistItem).\
        filter(PlaylistItem.playlist_id == playlist_id).\
        filter(PlaylistItem.video_url == video_url).first()

def delete_playlist_item_by_index(db_session: Session, playlist_id: int, index_id: int):
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
