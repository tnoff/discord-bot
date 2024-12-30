from discord_bot.utils.clients.youtube_music import YoutubeMusicClient

def yield_youtube_mock(results):
    class MockYoutubeMusic():
        def __init__(self):
            pass
        def search(self, _search, **_kwargs):
            return results
    return MockYoutubeMusic


def test_youtube_music_client(mocker):
    mocker.patch('discord_bot.utils.clients.youtube_music.YTMusic', side_effect=yield_youtube_mock([]))
    x = YoutubeMusicClient()
    assert x.search('foo bar') is None

def test_youtube_music_client_with_data(mocker):
    results = [
        {
            'videoId': '1234'
        }
    ]
    mocker.patch('discord_bot.utils.clients.youtube_music.YTMusic', side_effect=yield_youtube_mock(results))
    x = YoutubeMusicClient()
    assert x.search('foo bar') == '1234'
