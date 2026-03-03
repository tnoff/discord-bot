import pytest

from discord_bot.cogs.music import match_generator

from discord_bot.cogs.music_helpers.download_client import VideoTooLong, VideoBanned

from tests.helpers import fake_engine, fake_context #pylint:disable=unused-import

def test_match_generator_no_data():
    func = match_generator(None, None)
    info = {
        'duration': 100,
        'webpage_url': 'https://example.com/foo',
        'id': '1234',
        'extractor': 'foo extractor'
    }
    result = func(info, incomplete=None) #pylint:disable=assignment-from-no-return
    assert result is None

def test_match_generator_too_long():
    func = match_generator(1, None)
    info = {
        'duration': 100,
        'webpage_url': 'https://example.com/foo',
        'id': '1234',
        'extractor': 'foo extractor'
    }
    with pytest.raises(VideoTooLong) as exc:
        func(info, incomplete=None)
    assert 'Video Too Long' in str(exc.value)

def test_match_generator_banned_vidoes():
    func = match_generator(None, ['https://example.com/foo'])
    info = {
        'duration': 100,
        'webpage_url': 'https://example.com/foo',
        'id': '1234',
        'extractor': 'foo extractor'
    }
    with pytest.raises(VideoBanned) as exc:
        func(info, incomplete=None)
    assert 'Video Banned' in str(exc.value)
