'''
The real ytmusicapi wrapper — the ONLY module in the tree that imports ytmusicapi.

Split out of youtube_music.py so that importing ytmusicapi belongs at top level
HERE, while the public module next door stays free of it and resolves this one on
first use. Nothing should import this module directly: go through
`utils.integrations.youtube_music`, which is the boundary that keeps the
dependency out of processes that never build a client (the HA bot pod — the
search pod owns the client, and tests/cogs/test_music.py guards it).

Same shape as cli/_lib/cog_registry.py being separate from cli/_lib/common.py:
the heavy import lives in its own module so lighter consumers can skip it.
'''
from opentelemetry.trace import SpanKind
from ytmusicapi import YTMusic
from ytmusicapi.exceptions import YTMusicServerError
from discord_bot.exceptions import YoutubeMusicRetryException
from discord_bot.utils.otel import otel_span_wrapper, ThirdPartyNaming

__all__ = ['YoutubeMusicClient', 'YoutubeMusicRetryException']

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
