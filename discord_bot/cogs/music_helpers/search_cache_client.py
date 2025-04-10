from datetime import datetime, timezone
from functools import partial
from typing import Callable

from sqlalchemy import asc
from sqlalchemy.orm import Session

from discord_bot.database import SearchString
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.cogs.music_helpers.source_dict import SourceDict
from discord_bot.cogs.music_helpers.source_download import SourceDownload
from discord_bot.utils.sql_retry import retry_database_commands

VALID_CACHE_TYPES = [SearchType.SPOTIFY]

def find_existing_cache_item(db_session: Session, search_string: str):
    '''
    Find existing cache items and return
    '''
    return db_session.query(SearchString).\
        filter(SearchString.search_string == search_string).first()

def update_existing_cache(db_session: Session, search: SearchString, source_download: SourceDownload):
    '''
    Update cache items
    '''
    now = datetime.now(timezone.utc)
    search.video_url = source_download.webpage_url
    search.last_iterated_at = now
    db_session.commit()
    return True

def add_new_search(db_session: Session, search: SearchString):
    '''
    Add new items
    '''
    db_session.add(search)
    db_session.commit()

def get_cache_count(db_session: Session):
    '''
    Get basic count
    '''
    return db_session.query(SearchString).count()

def remove_old_cache(db_session: Session, num_to_remove: int):
    '''
    Remove cache items based on last iterated at time
    '''
    for item in db_session.query(SearchString).order_by(asc(SearchString.last_iterated_at)).limit(num_to_remove):
        db_session.delete(item)
    db_session.commit()

class SearchCacheClient():
    '''
    Track search strings
    '''
    def __init__(self, session_generator: Callable, max_search_cache: int):
        self.session_generator = session_generator
        self.max_search_cache = max_search_cache

    def iterate(self, source_download: SourceDownload) -> bool:
        '''
        Iterate cache with download

        source_download : source_download
        '''
        if source_download.source_dict.search_type not in VALID_CACHE_TYPES:
            return False
        now = datetime.now(timezone.utc)
        with self.session_generator() as db_session:
            existing = retry_database_commands(db_session, partial(find_existing_cache_item, db_session, source_download.source_dict.original_search_string))
            if existing:
                return retry_database_commands(db_session, partial(update_existing_cache, db_session, existing, source_download))
            search = SearchString(
                search_string=source_download.source_dict.original_search_string,
                video_url=source_download.webpage_url,
                created_at=now,
                last_iterated_at=now,
            )
            retry_database_commands(db_session, partial(add_new_search, db_session, search))
            return True

    def check_cache(self, source_dict: SourceDict) -> str:
        '''
        Get existing video url from source dict
        source_dict : Original source dict
        '''
        if source_dict.search_type not in VALID_CACHE_TYPES:
            return None
        with self.session_generator() as db_session:
            existing = retry_database_commands(db_session, partial(find_existing_cache_item, db_session, source_dict.original_search_string))
            if existing:
                return existing.video_url
            return None

    def remove(self) -> bool:
        '''
        Remove older files when possible
        '''
        with self.session_generator() as db_session:
            current_count = retry_database_commands(db_session, partial(get_cache_count, db_session))
            num_to_remove = current_count - self.max_search_cache
            if num_to_remove < 1:
                return True
            retry_database_commands(db_session, partial(remove_old_cache, db_session, num_to_remove))
            return True
