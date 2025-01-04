from typing import List

from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials

class SpotifyClient():
    '''
    Spotify client for API
    '''
    def __init__(self, client_id: str, client_secret: str):
        auth_manager = SpotifyClientCredentials(client_id=client_id,
                                                client_secret=client_secret)
        self.client = Spotify(auth_manager=auth_manager)


    def __get_response_items(self, resp: List[dict]) -> List[dict]:
        '''
        Get items from spotify response

        resp : Response from spotify client
        '''
        items = []
        for item in resp:
            # Depending on type of result, may include 'track' key or may not
            try:
                track = item['track']
            except KeyError:
                track = item

            items.append({
                'track_name': track['name'],
                'track_artists': ', '.join(i['name'] for i in track['artists'])
            })
        return items


    def playlist_get(self, playlist_id: str,
                     pagination_limit: int = 50) -> List[dict]:
        '''
        Get all playlist tracks

        playlist_id : Playlist id from spotify
        pagination_limit : Limit of each API call
        '''
        offset = 0
        items = []
        while True:
            resp = self.client.playlist_tracks(playlist_id, limit=pagination_limit, offset=offset)
            items += self.__get_response_items(resp['items'])
            try:
                if not resp['next']:
                    return items
            except KeyError:
                return items
            offset += pagination_limit

    def album_get(self, album_id: str,
                  pagination_limit: int = 50) -> List[dict]:
        '''
        Get all album tracks
        
        album_id : Album id from spotify
        pagination_limit : Limit of each API call
        '''

        offset = 0
        items = []
        while True:
            resp = self.client.album_tracks(album_id, limit=pagination_limit, offset=offset)
            items += self.__get_response_items(resp['items'])
            if not resp['next']:
                return items
            offset += pagination_limit

    def track_get(self, track_id: str) -> List[dict]:
        '''
        Get single track

        track_id : Track id from spotify
        '''
        resp = self.client.track(track_id)
        return self.__get_response_items([resp])
