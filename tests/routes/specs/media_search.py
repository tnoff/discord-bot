'''
The media-search seam, proved against the search pod's real server.

What each route takes and answers is the shared contract (`testing/contracts/media_search.py`, which the
commands pod's client tests read too); this module adds what only the serving pod can: a live server over a
provider fake, and the calls that make it produce every declared reply.
'''
from unittest.mock import AsyncMock

from discord_core.exceptions import MediaSearchError
from discord_core.routes import media_search as media_search_routes
from discord_core.types.catalog import CatalogItem, CatalogResponse
from discord_core.types.media_search import MediaSearchResponse

from discord_search.servers.media_search_server import MediaSearchHttpServer

from testing.contracts.media_search import CONTRACTS
from tests.routes.specs._model import Call, operation, serving

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
    media_search_routes.SPOTIFY: operation(CONTRACTS[media_search_routes.SPOTIFY], _calls({'playlist_id': 'abc'})),
    media_search_routes.YOUTUBE: operation(CONTRACTS[media_search_routes.YOUTUBE], _calls({'playlist_id': 'abc'})),
}
