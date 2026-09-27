'''
Span attribute names for the third-party clients.

Split out of `utils/otel.py` on 2026-09-18. Every name here is set by
`integrations/spotify.py`, `integrations/youtube.py` or
`integrations/_youtube_music_impl.py`, all of which only the search image
reaches — so this enum went from forcing six rebuilds to forcing one.

Lives inside `utils/integrations/` rather than beside `otel.py` because the
package `__init__` is empty: importing `integrations.spotify` does not drag this
in, and nothing outside the package has a reason to.
'''
from enum import Enum


class ThirdPartyNaming(Enum):
    '''
    Third party client naming
    '''
    SPOTIFY_PLAYLIST = 'spotify.playlist.id'
    SPOTIFY_ALBUM = 'spotify.album.id'
    SPOTIFY_TRACK = 'spotify.track.id'
    YOUTUBE_PLAYLIST = 'youtube.playlist.id'
    YOUTUBE_MUSIC_SEARCH = 'youtube_music.search_string'
