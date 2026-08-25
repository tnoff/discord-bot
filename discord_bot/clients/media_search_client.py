'''
In-process MediaSearchClient — the source-expansion providers, run locally.

Wraps a SpotifyClient and a YoutubeClient, offloads their blocking SDK calls to
a thread, and translates every provider failure into a MediaSearchError. It is
what the bot runs today; HttpMediaSearchClient will forward the same two calls to
the search pod.

The translation is the reason this module exists. Before it, the cog caught
SpotifyOauthError, SpotifyException and googleapiclient's HttpError directly,
which meant cogs/music_helpers/search_client.py imported spotipy and
googleapiclient purely to name exception types. Those imports are the whole
reason the two heavy providers cannot leave the bot image, so the seam has to
absorb them rather than pass them through.
'''
import asyncio
from functools import partial

from googleapiclient.errors import HttpError
from spotipy.exceptions import SpotifyException, SpotifyOauthError

from discord_bot.exceptions import MediaSearchError
from discord_bot.types.catalog import CatalogResponse
from discord_bot.utils.integrations.spotify import SpotifyClient
from discord_bot.utils.integrations.youtube import YoutubeClient

SPOTIFY = MediaSearchError.SPOTIFY
YOUTUBE = MediaSearchError.YOUTUBE


class InMemoryMediaSearchClient:
    '''
    MediaSearchClient backed by in-process provider SDKs.

    Either client may be None, which is how the bot runs when its credentials are
    unset; the corresponding call then raises MediaSearchError with reason
    MISSING_CREDENTIALS rather than AttributeError on None. That check used to sit
    in the cog next to the regexes, but it belongs with the provider it describes
    -- the cog cannot answer "do we have Spotify credentials" once the providers
    are remote, and the pod can.
    '''

    def __init__(self, spotify_client: SpotifyClient = None,
                 youtube_client: YoutubeClient = None):
        '''
        spotify_client : Spotify Client, or None when no credentials are configured
        youtube_client : Youtube Client, or None when no api key is configured
        '''
        self.spotify_client: SpotifyClient | None = spotify_client
        self.youtube_client: YoutubeClient | None = youtube_client

    @staticmethod
    async def _offload(func) -> CatalogResponse:
        '''
        Run a blocking provider call on the default executor.

        Reads the running loop rather than taking one as an argument: inside a cog
        command they are the same loop, and an explicit parameter would have to be
        carried through the Protocol into the HTTP client, which has no use for it.
        '''
        return await asyncio.get_running_loop().run_in_executor(None, func)

    async def spotify_source(self, playlist_id: str = None, album_id: str = None,
                             track_id: str = None) -> CatalogResponse:
        '''
        Expand exactly one of a Spotify playlist, album or track.

        playlist_id : Playlist id
        album_id : Album id
        track_id : Track id
        '''
        if not (playlist_id or album_id or track_id):
            raise ValueError('Playlist, album, or track id must be passed')
        if not self.spotify_client:
            raise MediaSearchError(SPOTIFY, MediaSearchError.MISSING_CREDENTIALS,
                                   'Missing spotify creds')
        if playlist_id:
            call = partial(self.spotify_client.playlist_get, playlist_id)
        elif album_id:
            call = partial(self.spotify_client.album_get, album_id)
        else:
            call = partial(self.spotify_client.track_get, track_id)
        try:
            return await self._offload(call)
        except SpotifyOauthError as exc:
            raise MediaSearchError(SPOTIFY, MediaSearchError.AUTH_ERROR,
                                   'Spotify credentials rejected') from exc
        except SpotifyException as exc:
            reason = (MediaSearchError.NOT_FOUND if exc.http_status == 404
                      else MediaSearchError.API_ERROR)
            raise MediaSearchError(SPOTIFY, reason, 'Spotify API call failed',
                                   http_status=exc.http_status) from exc

    async def youtube_source(self, playlist_id: str) -> CatalogResponse:
        '''
        Expand a YouTube playlist.

        playlist_id : ID of youtube playlist
        '''
        if not self.youtube_client:
            raise MediaSearchError(YOUTUBE, MediaSearchError.MISSING_CREDENTIALS,
                                   'Missing youtube creds')
        try:
            return await self._offload(partial(self.youtube_client.playlist_get, playlist_id))
        except HttpError as exc:
            # No http_status: googleapiclient's resp is a duck-typed object whose
            # shape varies by transport, and nothing downstream branches on a
            # YouTube status the way the Spotify 404 message does.
            raise MediaSearchError(YOUTUBE, MediaSearchError.API_ERROR,
                                   'Youtube API call failed') from exc
