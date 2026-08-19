'''
Tests for the DownloadResult model, in particular the ytdlp_data projection.
'''
from discord_bot.types.download import (
    YTDLP_DATA_KEYS,
    DownloadResult,
    DownloadStatus,
)

from tests.helpers import fake_source_dict, generate_fake_context


class UnserializablePostProcessor:
    '''Stands in for yt_dlp.postprocessor.ffmpeg.FFmpegFixupM3u8PP.'''


def _full_ytdlp_data():
    '''A yt-dlp info dict shaped like the real one: known keys plus internals.'''
    return {
        'id': 'abc123',
        'title': 'A Song',
        'webpage_url': 'https://youtube.com/watch?v=abc123',
        'uploader': 'Some Uploader',
        'duration': 212,
        'extractor': 'youtube',
        # Everything below is yt-dlp internal and read by nobody.
        '__postprocessors': [UnserializablePostProcessor()],
        'formats': [{'format_id': '251'} for _ in range(40)],
        'automatic_captions': {'en': [{'url': 'http://c'}]},
        'http_headers': {'User-Agent': 'x'},
    }


def _result(ytdlp_data):
    return DownloadResult(
        status=DownloadStatus(success=True),
        media_request=fake_source_dict(generate_fake_context()),
        ytdlp_data=ytdlp_data,
        file_name=None,
    )


def test_ytdlp_data_projects_to_known_keys():
    '''Only the consumed keys survive; yt-dlp internals are dropped.'''
    result = _result(_full_ytdlp_data())
    assert set(result.ytdlp_data) == set(YTDLP_DATA_KEYS)
    assert result.ytdlp_data['title'] == 'A Song'
    assert result.ytdlp_data['extractor'] == 'youtube'
    assert result.ytdlp_data['duration'] == 212


def test_ytdlp_data_drops_unserializable_postprocessor():
    '''
    Regression for the prod failure: an HLS download attaches
    FFmpegFixupM3u8PP instances under __postprocessors, and
    register_download_result then died on model_dump(mode='json') with
    PydanticSerializationError after the download had already succeeded.
    '''
    result = _result(_full_ytdlp_data())
    assert '__postprocessors' not in result.ytdlp_data
    dumped = result.model_dump(mode='json')
    assert dumped['ytdlp_data']['webpage_url'] == 'https://youtube.com/watch?v=abc123'


def test_ytdlp_data_none_passes_through():
    '''A result with no yt-dlp data stays None rather than becoming {}.'''
    assert _result(None).ytdlp_data is None


def test_ytdlp_data_omits_keys_that_are_absent():
    '''A partial dict is not padded out with None values.'''
    result = _result({'extractor': 'youtube'})
    assert result.ytdlp_data == {'extractor': 'youtube'}


def test_ytdlp_data_projection_survives_round_trip():
    '''
    The validator also runs on deserialization, so a result that crosses the
    broker and comes back is unchanged rather than projected a second time.
    '''
    original = _result(_full_ytdlp_data())
    restored = DownloadResult.model_validate(original.model_dump(mode='json'))
    assert restored.ytdlp_data == original.ytdlp_data
