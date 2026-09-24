'''
A YouTube-Music client that answers without leaving the process.

`tests/helpers.attach_in_process_search` used to default to a REAL
`youtube_music.YoutubeMusicClient()`, so every one of its 65 call sites built a
worker holding a live client, and any test that reached `driver.run_once` issued
a genuine search against YouTube Music. That passes while YouTube answers and
fails when it does not: CI saw
`YTMusicServerError: Server returned HTTP 400` on two of four interpreter
versions while the other two went green, which is the signature of a live
dependency rather than a version bug.

`_youtube_music_impl.search` only converts a **429** into a
`YoutubeMusicRetryException`; every other `YTMusicServerError` propagates raw,
so a 400 is an outright test failure with no retry and no backoff.

The worker stays real -- driving the real `AsyncioYoutubeMusicSearchWorker` is
the point of that helper, and the reason it exists rather than standing up an
aiohttp server. Only the network boundary is replaced.
'''


class StubYoutubeMusicClient:
    '''Returns a fixed videoId for any search, and records what it was asked.'''

    def __init__(self, video_id: str = 'stub-video-id'):
        self.video_id = video_id
        #: Every search string this client was handed, in order. Tests that care
        #: what was searched can assert on it instead of on YouTube's opinion.
        self.searches: list[str] = []

    def search(self, search_string: str) -> str:
        '''Mirror `YoutubeMusicClient.search`: a search string in, a videoId out.'''
        self.searches.append(search_string)
        return self.video_id
