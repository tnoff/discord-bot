from datetime import datetime, timezone

from discord_search.utils.integrations.youtube import YoutubeClient

class MockYoutubeRequest():
    def __init__(self, with_page_token=False):
        self.with_page_token = with_page_token

    def execute(self):
        data = {
            'items': [
                {
                    'snippet': {
                        'title': 'Episode 0',
                        'publishedAt': datetime.now(timezone.utc).isoformat(),
                        'description': 'foo bar',
                        'resourceId': {
                            'videoId': '1234ABC',
                        }
                    }
                },
                {
                    'snippet': {
                        'title': 'Episode 1',
                        'publishedAt': datetime.now(timezone.utc).isoformat(),
                        'description': 'foo bar 2',
                        'resourceId': {
                            'videoId': '1234ABCDEFG',
                        },
                    }
                },
            ],
        }
        if self.with_page_token:
            data['nextPageToken'] = None
        return data

class MockYoutubePlaylistItems():
    def __init__(self, with_next_page=False):
        self.with_next_page = with_next_page

    def list(self, **_):
        return MockYoutubeRequest(with_page_token=self.with_next_page)

    def list_next(self, *_, **__):
        return None


class MockYoutubePlaylist():
    def list(self, **_):
        return MockYoutubePlaylistRequest()

class MockYoutubePlaylistRequest():
    def execute(self):
        return {
            'items': [{
                'snippet': {
                    'title': 'Example Playlist'
                }
            }]
        }

class MockYoutube():
    def __init__(self, with_page_token=False):
        self.with_page_token = with_page_token

    def playlistItems(self): #pylint: disable=invalid-name
        return MockYoutubePlaylistItems(with_next_page=self.with_page_token)

    def playlists(self):
        return MockYoutubePlaylist()

def google_api_build(_typer, _version, developerKey=None): #pylint:disable=invalid-name,unused-argument
    return MockYoutube()

def google_api_build_with_page(_typer, _version, developerKey=None): #pylint:disable=invalid-name,unused-argument
    return MockYoutube(with_page_token=True)

def test_youtube_playlist_get(mocker):
    mocker.patch('discord_search.utils.integrations.youtube.build', side_effect=google_api_build)
    x = YoutubeClient('foo')
    result = x.playlist_get('1234')
    assert len(result.items) == 2
    assert '1234ABC' in result.items[0].search_string
    assert result.items[0].title == 'Episode 0'
    assert result.collection_name == 'Example Playlist'

def test_youtube_playlist_get_with_page_token(mocker):
    mocker.patch('discord_search.utils.integrations.youtube.build', side_effect=google_api_build_with_page)
    x = YoutubeClient('foo')
    result = x.playlist_get('1234')
    assert len(result.items) == 2
    assert '1234ABC' in result.items[0].search_string
    assert result.items[0].title == 'Episode 0'
    assert result.collection_name == 'Example Playlist'

class MockPagedPlaylistItems():
    '''Serves two pages, recording the pageToken each request asked for.'''
    def __init__(self):
        self.page_tokens = []

    def list(self, **kwargs):
        self.page_tokens.append(kwargs['pageToken'])
        last_page = kwargs['pageToken'] is not None
        return MockPagedRequest(video_id='second' if last_page else 'first',
                                next_token=None if last_page else 'page-2')

class MockPagedRequest():
    def __init__(self, video_id, next_token):
        self.video_id = video_id
        self.next_token = next_token

    def execute(self):
        return {
            'items': [{'snippet': {'title': self.video_id, 'resourceId': {'videoId': self.video_id}}}],
            'nextPageToken': self.next_token,
        }

class MockPagedYoutube():
    def __init__(self):
        self.items = MockPagedPlaylistItems()

    def playlistItems(self): #pylint: disable=invalid-name
        return self.items

    def playlists(self):
        return MockYoutubePlaylist()

def test_youtube_playlist_get_follows_next_page_token(mocker):
    youtube = MockPagedYoutube()
    mocker.patch('discord_search.utils.integrations.youtube.build', return_value=youtube)
    result = YoutubeClient('foo').playlist_get('1234')
    assert youtube.items.page_tokens == [None, 'page-2']
    assert [item.title for item in result.items] == ['first', 'second']
    assert result.collection_name == 'Example Playlist'
