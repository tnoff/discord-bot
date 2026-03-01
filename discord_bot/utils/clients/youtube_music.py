from opentelemetry.trace import SpanKind
from ytmusicapi import YTMusic
from ytmusicapi.exceptions import YTMusicServerError
from discord_bot.utils.otel import otel_span_wrapper, ThirdPartyNaming

class YoutubeMusicRetryException(Exception):
    '''Retry youtube music'''

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
            try:
                results = self.client.search(search_string, filter='songs')
            except YTMusicServerError as error:
                if '429' in str(error):
                    raise YoutubeMusicRetryException('429 Exhaust Limit Hit') from error
                raise error
            try:
                return results[0]['videoId']
            except (KeyError, IndexError):
                return None
