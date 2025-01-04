from typing import List

from googleapiclient.discovery import build

class YoutubeClient():
    '''
    Youtube API Functions
    '''
    def __init__(self, google_api_token: str):
        self.google_api_token = google_api_token
        self.client = build('youtube', 'v3', developerKey=self.google_api_token)

    def playlist_get(self, playlist_id: str, pagination_limit: int = 50) -> List[str]:
        '''
        Youtube Playlist Get

        playlist_id : ID of youtube playlist
        pagination_limit : Pagination limit for each API call
        '''
        items = []
        page_token = None
        while True:
            data_inputs = {
                'part': 'snippet',
                'playlistId': playlist_id,
                'maxResults': pagination_limit,
                'pageToken': page_token 
            }
            req = self.client.playlistItems().list(**data_inputs).execute() #pylint:disable=no-member
            for item in req['items']:
                items.append(item['snippet']['resourceId']['videoId'])
            try:
                if req['nextPageToken'] is None:
                    return items
                page_token = req['nextPageToken']
            except KeyError:
                return items
