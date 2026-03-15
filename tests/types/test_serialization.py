import pytest

from discord_bot.cogs.music_helpers.common import MediaRequestLifecycleStage, SearchType
from discord_bot.types.media_request import MediaRequest, RetryInformation
from discord_bot.types.search import SearchResult

from tests.helpers import fake_context, fake_source_dict  # noqa: F401  # pylint: disable=unused-import


# ---------------------------------------------------------------------------
# SearchResult
# ---------------------------------------------------------------------------

def test_search_result_roundtrip_minimal():
    sr = SearchResult(search_type=SearchType.SEARCH, raw_search_string='lofi hip hop')
    assert SearchResult.model_validate(sr.model_dump(mode='json')) == sr

def test_search_result_roundtrip_full():
    sr = SearchResult(
        search_type=SearchType.SPOTIFY,
        raw_search_string='spotify:track:abc',
        proper_name='Lofi Hip Hop',
        youtube_music_search_string='https://youtube.com/watch?v=abc',
    )
    assert SearchResult.model_validate(sr.model_dump(mode='json')) == sr

@pytest.mark.parametrize('search_type', list(SearchType))
def test_search_result_all_search_types(search_type):
    sr = SearchResult(search_type=search_type, raw_search_string='test')
    assert SearchResult.model_validate(sr.model_dump(mode='json')).search_type == search_type


# ---------------------------------------------------------------------------
# RetryInformation
# ---------------------------------------------------------------------------

def test_retry_information_roundtrip_default():
    ri = RetryInformation()
    result = RetryInformation.model_validate(ri.model_dump(mode='json'))
    assert result.retry_count == 0
    assert result.retry_reason is None
    assert result.retry_backoff_seconds == 0
    assert result.retry_reason_sent is False

def test_retry_information_roundtrip_with_data():
    ri = RetryInformation()
    ri.retry_count = 2
    ri.retry_reason = 'HTTP 429'
    ri.retry_backoff_seconds = 30
    ri.retry_reason_sent = True
    result = RetryInformation.model_validate(ri.model_dump(mode='json'))
    assert result.retry_count == 2
    assert result.retry_reason == 'HTTP 429'
    assert result.retry_backoff_seconds == 30
    assert result.retry_reason_sent is True


# ---------------------------------------------------------------------------
# MediaRequest
# ---------------------------------------------------------------------------

def test_media_request_roundtrip_basic(fake_context):  # noqa: F811  #pylint:disable=redefined-outer-name
    mr = fake_source_dict(fake_context)
    result = MediaRequest.deserialize(mr.serialize())

    assert result.uuid == mr.uuid
    assert result.guild_id == mr.guild_id
    assert result.channel_id == mr.channel_id
    assert result.requester_name == mr.requester_name
    assert result.requester_id == mr.requester_id
    assert result.download_file == mr.download_file
    assert result.added_from_history == mr.added_from_history
    assert result.lifecycle_stage == mr.lifecycle_stage
    assert result.failure_reason == mr.failure_reason
    assert result.bundle_uuid == mr.bundle_uuid
    assert result.add_to_playlist == mr.add_to_playlist
    assert result.history_playlist_item_id == mr.history_playlist_item_id

def test_media_request_roundtrip_search_result_preserved(fake_context):  # noqa: F811  #pylint:disable=redefined-outer-name
    mr = fake_source_dict(fake_context)
    mr.search_result.proper_name = 'A Proper Name'
    mr.search_result.youtube_music_search_string = 'https://youtube.com/watch?v=xyz'
    result = MediaRequest.deserialize(mr.serialize())

    assert result.search_result.search_type == mr.search_result.search_type
    assert result.search_result.raw_search_string == mr.search_result.raw_search_string
    assert result.search_result.proper_name == 'A Proper Name'
    assert result.search_result.youtube_music_search_string == 'https://youtube.com/watch?v=xyz'

def test_media_request_roundtrip_retry_information_preserved(fake_context):  # noqa: F811  #pylint:disable=redefined-outer-name
    mr = fake_source_dict(fake_context)
    mr.download_retry_information.retry_count = 1
    mr.download_retry_information.retry_reason = 'TLS error'
    mr.download_retry_information.retry_backoff_seconds = 15
    mr.youtube_music_retry_information.retry_count = 2
    mr.youtube_music_retry_information.retry_reason_sent = True
    result = MediaRequest.deserialize(mr.serialize())

    assert result.download_retry_information.retry_count == 1
    assert result.download_retry_information.retry_reason == 'TLS error'
    assert result.download_retry_information.retry_backoff_seconds == 15
    assert result.youtube_music_retry_information.retry_count == 2
    assert result.youtube_music_retry_information.retry_reason_sent is True

def test_media_request_roundtrip_lifecycle_stage_preserved(fake_context):  # noqa: F811  #pylint:disable=redefined-outer-name
    mr = fake_source_dict(fake_context)
    mr.lifecycle_stage = MediaRequestLifecycleStage.RETRY_DOWNLOAD
    result = MediaRequest.deserialize(mr.serialize())
    assert result.lifecycle_stage == MediaRequestLifecycleStage.RETRY_DOWNLOAD

def test_media_request_roundtrip_optional_fields(fake_context):  # noqa: F811  #pylint:disable=redefined-outer-name
    mr = fake_source_dict(fake_context)
    mr.bundle_uuid = 'request.bundle.some-uuid'
    mr.add_to_playlist = 42
    mr.history_playlist_item_id = 7
    mr.failure_reason = 'Video unavailable'
    result = MediaRequest.deserialize(mr.serialize())

    assert result.bundle_uuid == 'request.bundle.some-uuid'
    assert result.add_to_playlist == 42
    assert result.history_playlist_item_id == 7
    assert result.failure_reason == 'Video unavailable'

def test_media_request_deserialize_creates_fresh_state_machine(fake_context):  # noqa: F811  #pylint:disable=redefined-outer-name
    mr = fake_source_dict(fake_context)
    callback_called = []
    mr.state_machine.set_on_change(lambda req, stage: callback_called.append(stage))

    result = MediaRequest.deserialize(mr.serialize())

    # Deserialized request has a fresh state machine with no callback
    result.state_machine.mark_queued()
    assert not callback_called, 'on_change should not carry over after deserialization'
    assert result.lifecycle_stage == MediaRequestLifecycleStage.QUEUED
