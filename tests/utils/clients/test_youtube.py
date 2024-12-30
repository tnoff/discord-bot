from datetime import datetime, timezone

from discord_bot.utils.clients.youtube import YoutubeClient

class MockYoutubeRequest():
    def __init__(self):
        pass

    def execute(self):
        return {
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
            'nextPageToken': None,
        }

class MockYoutubePlaylistItems():
    def __init__(self):
        pass

    def list(self, **_):
        return MockYoutubeRequest()

    def list_next(self, *_, **__):
        return None


class MockYoutube():
    def __init__(self):
        pass

    def playlistItems(self): #pylint: disable=invalid-name
        return MockYoutubePlaylistItems()

def google_api_build(_typer, _version, developerKey=None): #pylint:disable=invalid-name,unused-argument
    return MockYoutube()

def test_youtube_playlist_get(mocker):
    mocker.patch('discord_bot.utils.clients.youtube.build', side_effect=google_api_build)
    x = YoutubeClient('foo')
    res = x.playlist_get('1234')
    assert len(res) == 2
    assert res[0] == '1234ABC'
