from ytmusicapi import YTMusic

class YoutubeMusicClient():
    '''
    Generate results from youtube music api
    '''
    def __init__(self):
        self.client = YTMusic()

    def search(self, search_string: str) -> str:
        '''
        Search for string

        search_string : Original search string
        '''
        results = self.client.search(search_string, filter='songs')
        try:
            return results[0]['videoId']
        except (KeyError, IndexError):
            return None
