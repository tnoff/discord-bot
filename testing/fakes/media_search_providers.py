'''A MediaSearchClient stand-in for the media-search server: records what it is asked and answers from a canned catalog.'''
from discord_core.types.catalog import CatalogItem, CatalogResponse


class FakeProviders:
    '''Records every spotify and youtube lookup; raises ``error`` instead of answering when one is given.'''

    def __init__(self, *, catalog=None, error=None):
        self.catalog = catalog or CatalogResponse(
            items=[CatalogItem(search_string='a track an artist', title='a track')],
            collection_name='A Collection')
        self.error = error
        self.spotify_calls = []
        self.youtube_calls = []

    async def spotify_source(self, playlist_id=None, album_id=None, track_id=None):
        '''One Spotify lookup, recorded with all three ids.'''
        self.spotify_calls.append({'playlist_id': playlist_id, 'album_id': album_id, 'track_id': track_id})
        if self.error:
            raise self.error
        return self.catalog

    async def youtube_source(self, playlist_id):
        '''One YouTube lookup, recorded.'''
        self.youtube_calls.append(playlist_id)
        if self.error:
            raise self.error
        return self.catalog
