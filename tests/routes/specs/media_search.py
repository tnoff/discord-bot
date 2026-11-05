'''
The media-search seam: expanding a Spotify or YouTube URL into a list of searches.

A provider that says "no such playlist" is an answer, so it comes back as a 200 with ``error``
set; only a malformed request is a 4xx.
'''
from unittest.mock import AsyncMock

from pydantic import BaseModel, model_validator

from discord_core.exceptions import MediaSearchError
from discord_core.routes import media_search as media_search_routes
from discord_core.types.catalog import CatalogItem, CatalogResponse
from discord_core.types.media_search import MediaSearchResponse

from discord_search.servers.media_search_server import MediaSearchHttpServer

from tests.routes.specs._model import Call, Operation, reply, serving


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


CATALOG = CatalogResponse(items=[CatalogItem(search_string='a song', title='A Song')],
                          collection_name='a list')


def environment():
    '''A media-search pod over a client that always finds the catalog above.'''
    client = AsyncMock()
    client.spotify_source.return_value = CATALOG
    client.youtube_source.return_value = CATALOG
    return serving(MediaSearchHttpServer(client), client=client)


async def provider_fails(env) -> dict:
    '''A provider that rejects both lookups.'''
    error = MediaSearchError('provider', 'not_found', 'no such list', http_status=404)
    env.parts.client.spotify_source.side_effect = error
    env.parts.client.youtube_source.side_effect = error
    return {}


def _calls(body: dict) -> tuple:
    return (Call(200, body=body, variant=MediaSearchResponse),
            Call(200, body=body, arrange=provider_fails, variant=MediaSearchResponse),
            Call(422, body={}))


OPERATIONS = {
    media_search_routes.SPOTIFY: Operation(
        'Expand a Spotify playlist, album or track.', request=SpotifyBody,
        replies=(reply(200, MediaSearchResponse, doc='A `catalog`, or an `error` from the provider.'),
                 reply(422, doc='Not exactly one id.')),
        calls=_calls({'playlist_id': 'abc'})),
    media_search_routes.YOUTUBE: Operation(
        'Expand a YouTube playlist.', request=YoutubeBody,
        replies=(reply(200, MediaSearchResponse, doc='A `catalog`, or an `error` from the provider.'),
                 reply(422, doc='No playlist_id.')),
        calls=_calls({'playlist_id': 'abc'})),
}
