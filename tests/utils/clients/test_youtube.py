from datetime import datetime, timezone

from discord_bot.utils.clients.youtube import YoutubeClient

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


class MockYoutube():
    def __init__(self, with_page_token=False):
        self.with_page_token = with_page_token

    def playlistItems(self): #pylint: disable=invalid-name
        return MockYoutubePlaylistItems(with_next_page=self.with_page_token)

def google_api_build(_typer, _version, developerKey=None): #pylint:disable=invalid-name,unused-argument
    return MockYoutube()

def google_api_build_with_page(_typer, _version, developerKey=None): #pylint:disable=invalid-name,unused-argument
    return MockYoutube(with_page_token=True)

def test_youtube_playlist_get(mocker):
    mocker.patch('discord_bot.utils.clients.youtube.build', side_effect=google_api_build)
    x = YoutubeClient('foo')
    res = x.playlist_get('1234')
    assert len(res) == 2
    assert res[0] == '1234ABC'

def test_youtube_playlist_get_with_page_token(mocker):
    mocker.patch('discord_bot.utils.clients.youtube.build', side_effect=google_api_build_with_page)
    x = YoutubeClient('foo')
    res = x.playlist_get('1234')
    assert len(res) == 2
    assert res[0] == '1234ABC'
