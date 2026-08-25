'''
Cog-facing MediaSearchClient Protocol — source expansion behind one handle.

Two provider calls, both returning a CatalogResponse: a Spotify playlist / album
/ track, and a YouTube playlist. They are the only parts of SearchClient that
touch a third-party SDK; everything else it does (the six-way regex dispatch, the
shuffle, the SearchResult mapping, the user-facing messages) is pure Python on
the strings the user typed.

That is why the seam is here and not around check_source as a whole. Only two of
check_source's six input shapes reach a provider at all — the other four,
including the plain-text search that is the common !play, are re.match and
nothing more. Putting the seam around the whole function would send every !play
across the network to do string matching.

Implementations:

  InMemoryMediaSearchClient (clients/media_search_client.py) — wraps a
  SpotifyClient / YoutubeClient and offloads their blocking calls to a thread.
  This is what the bot runs today.

  HttpMediaSearchClient (clients/http_media_search_client.py, not yet written) —
  forwards to the search pod's /search/spotify and /search/youtube routes. It
  gets its own module rather than sharing this one's, because the in-memory
  client imports spotipy and googleapiclient at module scope and the search pod
  must not: the same split, and the same reason, as HttpBrokerClient moving out
  of clients/broker_client.py.

Both raise MediaSearchError on failure. Neither writes a user-facing message --
see MediaSearchError's docstring for why the Discord copy stays in the cog.
'''
from typing import Protocol, runtime_checkable

from discord_bot.types.catalog import CatalogResponse


@runtime_checkable
class MediaSearchClient(Protocol):
    '''
    Expand a third-party URL into a catalog of search strings.

    Async on both implementations, but for different reasons: the in-memory one
    is offloading a blocking SDK call to a thread, the HTTP one is making a
    request. Neither takes an event loop -- the in-memory client reads the
    running one itself, which is what the cog was passing in anyway.
    '''

    async def spotify_source(self, playlist_id: str = None, album_id: str = None,
                             track_id: str = None) -> CatalogResponse:
        '''
        Expand exactly one of a Spotify playlist, album or track.

        playlist_id : Playlist id
        album_id : Album id
        track_id : Track id
        '''

    async def youtube_source(self, playlist_id: str) -> CatalogResponse:
        '''
        Expand a YouTube playlist.

        playlist_id : ID of youtube playlist
        '''
