'''
The media-search seam's contract: expanding a Spotify or YouTube URL into a list of searches.

A provider that says "no such playlist" is an answer, so it comes back as a 200 with ``error``
set; only a malformed request is a 4xx.

Both ends read this: the search pod's server is proved against it (tests/routes/specs/media_search.py runs
every reply), and the commands pod's client is tested against a stub that serves it
(discord_commands/tests/clients/test_http_media_search_client.py), so neither needs the other pod.
'''
from pydantic import BaseModel, model_validator

from discord_core.routes import media_search as media_search_routes
from discord_core.types.media_search import MediaSearchResponse

from testing.contracts.model import Contract, reply


class SpotifyBody(BaseModel):
    '''POST /search/spotify: exactly one of the three ids.'''
    playlist_id: str | None = None
    album_id: str | None = None
    track_id: str | None = None

    @model_validator(mode='after')
    def exactly_one(self) -> 'SpotifyBody':
        '''The pod refuses none and refuses several.'''
        if sum(value is not None for value in (self.playlist_id, self.album_id, self.track_id)) != 1:
            raise ValueError('give exactly one of playlist_id, album_id and track_id')
        return self


class YoutubeBody(BaseModel):
    '''POST /search/youtube'''
    playlist_id: str


CONTRACTS = {
    media_search_routes.SPOTIFY: Contract(
        'Expand a Spotify playlist, album or track.', request=SpotifyBody,
        replies=(reply(200, MediaSearchResponse, doc='A `catalog`, or an `error` from the provider.'),
                 reply(422, doc='Not exactly one id.'))),
    media_search_routes.YOUTUBE: Contract(
        'Expand a YouTube playlist.', request=YoutubeBody,
        replies=(reply(200, MediaSearchResponse, doc='A `catalog`, or an `error` from the provider.'),
                 reply(422, doc='No playlist_id.'))),
}
