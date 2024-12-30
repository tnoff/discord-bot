from datetime import datetime, timezone

from sqlalchemy import asc
from sqlalchemy.orm import Session

from discord_bot.database import SearchString
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.cogs.music_helpers.source_dict import SourceDict
from discord_bot.cogs.music_helpers.source_download import SourceDownload

VALID_CACHE_TYPES = [SearchType.SPOTIFY]

class SearchCacheClient():
    '''
    Track search strings
    '''
    def __init__(self, session: Session, max_search_cache: int):
        self.db_session = session
        self.max_search_cache = max_search_cache

    def iterate(self, source_download: SourceDownload) -> bool:
        '''
        Iterate cache with download

        source_download : source_download
        '''
        if source_download.source_dict.search_type not in VALID_CACHE_TYPES:
            return False
        now = datetime.now(timezone.utc)
        existing = self.db_session.query(SearchString).\
            filter(SearchString.search_string == source_download.source_dict.original_search_string).first()
        if existing:
            existing.video_url = source_download.webpage_url
            existing.last_iterated_at = now
            self.db_session.commit()
            return True
        search = SearchString(
            search_string=source_download.source_dict.original_search_string,
            video_url=source_download.webpage_url,
            created_at=now,
            last_iterated_at=now,
        )
        self.db_session.add(search)
        self.db_session.commit()
        return True

    def check_cache(self, source_dict: SourceDict) -> str:
        '''
        Get existing video url from source dict
        source_dict : Original source dict
        '''
        if source_dict.search_type not in VALID_CACHE_TYPES:
            return None
        existing = self.db_session.query(SearchString).\
            filter(SearchString.search_string == source_dict.original_search_string).first()
        if existing:
            return existing.video_url
        return None

    def remove(self) -> bool:
        '''
        Remove older files when possible
        '''
        current_count = self.db_session.query(SearchString).count()
        num_to_remove = current_count - self.max_search_cache
        if num_to_remove < 1:
            return True
        for item in self.db_session.query(SearchString).order_by(asc(SearchString.last_iterated_at)).limit(num_to_remove):
            self.db_session.delete(item)
        self.db_session.commit()
        return True
