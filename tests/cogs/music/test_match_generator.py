import pytest

from discord_bot.database import VideoCache
from discord_bot.cogs.music import match_generator
from discord_bot.cogs.music_helpers.download_client import VideoTooLong, VideoBanned

from tests.helpers import mock_session
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
    with pytest.raises(VideoTooLong):
        func(info, incomplete=None)

def test_match_generator_banned_vidoes():
    func = match_generator(None, ['1234'])
    info = {
        'duration': 100,
        'webpage_url': 'https://example.com/foo',
        'id': '1234',
        'extractor': 'foo extractor'
    }
    with pytest.raises(VideoBanned):
        func(info, incomplete=None)

def test_match_generator_video_exists(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    func = match_generator(None, None, video_cache_client=None, engine=fake_engine)
    info = {
        'duration': 100,
        'webpage_url': 'https://example.com/foo',
        'id': '1234',
        'extractor': 'foo extractor'
    }

    with mock_session(fake_engine) as db_session:
        video_cache = VideoCache(base_path='/foo/bar', video_url='https://example.com/foo', count=0)
        db_session.add(video_cache)
        db_session.commit()
        result = func(info, incomplete=None) #pylint:disable=assignment-from-no-return
        assert result is None