from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from discord_bot.cogs.music import match_generator

from discord_bot.cogs.music_helpers.download_client import VideoTooLong, VideoBanned
from discord_bot.cogs.music_helpers.download_client import ExistingFileException
from discord_bot.cogs.music_helpers.video_cache_client import VideoCacheClient


from tests.helpers import mock_session, fake_source_download
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


def test_match_generator_video_exists(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(Path(tmp_dir), fake_context=fake_context) as sd:
            x = VideoCacheClient(Path(tmp_dir), 10, partial(mock_session, fake_engine), None, None)
            x.iterate_file(sd)
            func = match_generator(None, None, video_cache_search=partial(x.search_existing_file))
            info = {
                'duration': 120,
                'webpage_url': sd.webpage_url, #pylint:disable=no-member
                'id': sd.id, #pylint:disable=no-member
                'extractor': sd.extractor, #pylint:disable=no-member
            }
            with pytest.raises(ExistingFileException) as exc:
                func(info, incomplete=None)
            assert 'File already downloaded' in str(exc)
            assert exc.value.video_cache
