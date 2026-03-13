from dataclasses import dataclass

from discord_bot.cogs.music_helpers.common import SearchType


@dataclass
class SearchResult():
    '''
    SearchClient search results
    '''
    search_type: SearchType
    # Original search string given before any processing
    raw_search_string: str
    # If from an api source where we know a better name for processing
    proper_name: str = None
    # Search string after youtube music search, if given
    youtube_music_search_string: str = None

    def add_youtube_music_result(self, youtube_music_result: str):
        '''
        Add result from youtube music
        '''
        self.youtube_music_search_string = youtube_music_result

    @property
    def resolved_search_string(self):
        '''
        Either youtube music or original search string
        '''
        if self.youtube_music_search_string:
            return self.youtube_music_search_string
        return self.raw_search_string

@dataclass
class SearchCollection():
    '''
    Collection of Search Results
    '''
    search_results: list[SearchResult]
    collection_name: str = None
