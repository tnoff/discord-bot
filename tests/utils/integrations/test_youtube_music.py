import pytest

from ytmusicapi.exceptions import YTMusicServerError

from discord_bot.utils.integrations.youtube_music import YoutubeMusicClient, YoutubeMusicRetryException

def yield_youtube_mock(results):
    class MockYoutubeMusic():
        def __init__(self):
            pass
        def search(self, _search, **_kwargs):
            return results
    return MockYoutubeMusic


def test_youtube_music_client(mocker):
    mocker.patch('discord_bot.utils.integrations.youtube_music.YTMusic', side_effect=yield_youtube_mock([]))
    x = YoutubeMusicClient()
    assert x.search('foo bar') is None

def test_youtube_music_client_with_data(mocker):
    results = [
        {
            'videoId': '1234'
        }
    ]
    mocker.patch('discord_bot.utils.integrations.youtube_music.YTMusic', side_effect=yield_youtube_mock(results))
    x = YoutubeMusicClient()
    assert x.search('foo bar') == '1234'

def _make_error_mock(exc):
    class MockYoutubeMusic():
        def __init__(self):
            pass
        def search(self, _search, **_kwargs):
            raise exc
    return MockYoutubeMusic


def test_youtube_music_server_error_429(mocker):
    '''YTMusicServerError with 429 in message raises YoutubeMusicRetryException'''
    error = YTMusicServerError('Rate limit hit: 429 Too Many Requests')
    mocker.patch('discord_bot.utils.integrations.youtube_music.YTMusic', side_effect=_make_error_mock(error))
    client = YoutubeMusicClient()
    with pytest.raises(YoutubeMusicRetryException):
        client.search('foo bar')


def test_youtube_music_server_error_other(mocker):
    '''YTMusicServerError without 429 re-raises the original error'''
    error = YTMusicServerError('Internal server error')
    mocker.patch('discord_bot.utils.integrations.youtube_music.YTMusic', side_effect=_make_error_mock(error))
    client = YoutubeMusicClient()
    with pytest.raises(YTMusicServerError):
        client.search('foo bar')
