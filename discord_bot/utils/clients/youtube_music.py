from opentelemetry.trace import SpanKind
from ytmusicapi import YTMusic

from discord_bot.utils.otel import otel_span_wrapper, ThirdPartyNaming

class YoutubeMusicClient():
    '''
    Generate results from youtube music api
    '''
    def __init__(self):
        self.client = YTMusic()

    def search(self, search_string: str) -> str:
        '''
        Search for string

        search_string : Original search string
        '''
        with otel_span_wrapper('youtube_music.search', attributes={ThirdPartyNaming.YOUTUBE_MUSIC_SEARCH.value: search_string}, kind=SpanKind.CLIENT):
            results = self.client.search(search_string, filter='songs')
            try:
                return results[0]['videoId']
            except (KeyError, IndexError):
                return None
