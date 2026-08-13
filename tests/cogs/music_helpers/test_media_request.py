import pytest

from discord_bot.types.media_request import MediaRequestStateMachine, RetryInformation, chunk_list
from discord_bot.cogs.music_helpers.common import MediaRequestLifecycleStage

from tests.helpers import fake_source_dict
from tests.helpers import fake_context #pylint:disable=unused-import

@pytest.mark.asyncio
async def test_media_request_basics(fake_context): #pylint:disable=redefined-outer-name
    x = fake_source_dict(fake_context)
    assert str(x) == x.search_result.resolved_search_string
    x_direct = fake_source_dict(fake_context, is_direct_search=True)
    assert str(x_direct) == f'<{x_direct.search_result.raw_search_string}>'

def test_media_request_display_name_default(fake_context):  #pylint:disable=redefined-outer-name
    """display_name returns the raw search string (no override set)"""
    x = fake_source_dict(fake_context)
    assert x.display_name == x.search_result.raw_search_string

def test_media_request_display_name_override(fake_context):  #pylint:disable=redefined-outer-name
    """proper_name takes precedence over raw_search_string"""
    x = fake_source_dict(fake_context)
    x.search_result.proper_name = 'My Custom Title'
    assert x.display_name == 'My Custom Title'

@pytest.mark.asyncio
async def test_media_request_retry_count_initialization(fake_context): #pylint:disable=redefined-outer-name
    """Test that retry_count is always initialized to 0"""
    x = fake_source_dict(fake_context)
    assert x.download_retry_information.retry_count == 0

@pytest.mark.asyncio
async def test_media_request_retry_count_increments(fake_context): #pylint:disable=redefined-outer-name
    """Test that retry_count can be incremented"""
    x = fake_source_dict(fake_context)
    assert x.download_retry_information.retry_count == 0

    x.download_retry_information.retry_count += 1
    assert x.download_retry_information.retry_count == 1

    x.download_retry_information.retry_count += 1
    assert x.download_retry_information.retry_count == 2

def test_chunk_list_edge_cases():
    """Test chunk_list function with edge cases"""
    # Test empty list
    result = chunk_list([], 5)
    assert result == []

    # Test size 0 (should be clamped to 1)
    result = chunk_list([1, 2, 3], 0)
    assert result == [[1], [2], [3]]

    # Test negative size (should be clamped to 1)
    result = chunk_list([1, 2, 3], -5)
    assert result == [[1], [2], [3]]

    # Test size larger than list
    result = chunk_list([1, 2], 10)
    assert result == [[1, 2]]

    # Test exact divisible chunks
    result = chunk_list([1, 2, 3, 4], 2)
    assert result == [[1, 2], [3, 4]]

    # Test non-divisible chunks
    result = chunk_list([1, 2, 3, 4, 5], 2)
    assert result == [[1, 2], [3, 4], [5]]


# ---------------------------------------------------------------------------
# MediaRequestStateMachine tests
# ---------------------------------------------------------------------------

def test_state_machine_created_on_media_request(fake_context):  #pylint:disable=redefined-outer-name
    """MediaRequest.__post_init__ attaches a MediaRequestStateMachine instance"""
    req = fake_source_dict(fake_context)
    assert isinstance(req.state_machine, MediaRequestStateMachine)

def test_state_machine_mark_searching(fake_context):  #pylint:disable=redefined-outer-name
    """mark_searching transitions lifecycle_stage to SEARCHING"""
    req = fake_source_dict(fake_context)
    req.lifecycle_stage = MediaRequestLifecycleStage.RETRY_SEARCH
    req.state_machine.mark_searching()
    assert req.lifecycle_stage == MediaRequestLifecycleStage.SEARCHING

def test_state_machine_mark_queued(fake_context):  #pylint:disable=redefined-outer-name
    """mark_queued transitions lifecycle_stage to QUEUED"""
    req = fake_source_dict(fake_context)
    req.state_machine.mark_queued()
    assert req.lifecycle_stage == MediaRequestLifecycleStage.QUEUED

def test_state_machine_mark_in_progress(fake_context):  #pylint:disable=redefined-outer-name
    """mark_in_progress transitions lifecycle_stage to IN_PROGRESS"""
    req = fake_source_dict(fake_context)
    req.state_machine.mark_in_progress()
    assert req.lifecycle_stage == MediaRequestLifecycleStage.IN_PROGRESS

def test_state_machine_mark_backoff(fake_context):  #pylint:disable=redefined-outer-name
    """mark_backoff transitions lifecycle_stage to BACKOFF"""
    req = fake_source_dict(fake_context)
    req.state_machine.mark_backoff()
    assert req.lifecycle_stage == MediaRequestLifecycleStage.BACKOFF

def test_state_machine_mark_completed(fake_context):  #pylint:disable=redefined-outer-name
    """mark_completed transitions lifecycle_stage to COMPLETED"""
    req = fake_source_dict(fake_context)
    req.state_machine.mark_completed()
    assert req.lifecycle_stage == MediaRequestLifecycleStage.COMPLETED

def test_state_machine_mark_discarded(fake_context):  #pylint:disable=redefined-outer-name
    """mark_discarded transitions lifecycle_stage to DISCARDED"""
    req = fake_source_dict(fake_context)
    req.state_machine.mark_discarded()
    assert req.lifecycle_stage == MediaRequestLifecycleStage.DISCARDED

def test_state_machine_mark_failed_sets_stage(fake_context):  #pylint:disable=redefined-outer-name
    """mark_failed transitions lifecycle_stage to FAILED"""
    req = fake_source_dict(fake_context)
    req.state_machine.mark_failed()
    assert req.lifecycle_stage == MediaRequestLifecycleStage.FAILED

def test_state_machine_mark_failed_with_reason(fake_context):  #pylint:disable=redefined-outer-name
    """mark_failed sets failure_reason when provided"""
    req = fake_source_dict(fake_context)
    req.state_machine.mark_failed('something went wrong')
    assert req.failure_reason == 'something went wrong'
    assert req.lifecycle_stage == MediaRequestLifecycleStage.FAILED

def test_state_machine_mark_failed_without_reason_preserves_existing(fake_context):  #pylint:disable=redefined-outer-name
    """mark_failed without reason leaves an existing failure_reason untouched"""
    req = fake_source_dict(fake_context)
    req.failure_reason = 'original reason'
    req.state_machine.mark_failed()
    assert req.failure_reason == 'original reason'

def test_state_machine_mark_retry_download(fake_context):  #pylint:disable=redefined-outer-name
    """mark_retry_download sets stage and retry info atomically"""
    req = fake_source_dict(fake_context)
    req.state_machine.mark_retry_download('ytdlp error', 30)
    assert req.lifecycle_stage == MediaRequestLifecycleStage.RETRY_DOWNLOAD
    assert req.download_retry_information.retry_reason == 'ytdlp error'
    assert req.download_retry_information.retry_backoff_seconds == 30

def test_state_machine_mark_retry_download_no_backoff(fake_context):  #pylint:disable=redefined-outer-name
    """mark_retry_download coerces a None backoff to 0, keeping retry_backoff_seconds
    a valid int — a None would break BundleState re-validation in the broker (pool
    mode reports no backoff while an exit is free)."""
    req = fake_source_dict(fake_context)
    req.state_machine.mark_retry_download('error', None)
    assert req.lifecycle_stage == MediaRequestLifecycleStage.RETRY_DOWNLOAD
    assert req.download_retry_information.retry_backoff_seconds == 0

def test_state_machine_mark_retry_search_no_backoff(fake_context):  #pylint:disable=redefined-outer-name
    """mark_retry_search likewise coerces a None backoff to 0."""
    req = fake_source_dict(fake_context)
    req.state_machine.mark_retry_search('error', None)
    assert req.lifecycle_stage == MediaRequestLifecycleStage.RETRY_SEARCH
    assert req.youtube_music_retry_information.retry_backoff_seconds == 0

def test_retry_information_coerces_none_backoff_on_validate():
    """A persisted retry_backoff_seconds=None is coerced to 0 on model_validate, so a
    bundle written with a None (before the setters normalised it) self-heals on load
    instead of wedging the broker's BundleState validation."""
    assert RetryInformation.model_validate({'retry_backoff_seconds': None}).retry_backoff_seconds == 0
    # A real int is untouched.
    assert RetryInformation.model_validate({'retry_backoff_seconds': 30}).retry_backoff_seconds == 30

def test_state_machine_mark_retry_search(fake_context):  #pylint:disable=redefined-outer-name
    """mark_retry_search sets stage and retry info atomically"""
    req = fake_source_dict(fake_context)
    req.state_machine.mark_retry_search('429 rate limit', 60)
    assert req.lifecycle_stage == MediaRequestLifecycleStage.RETRY_SEARCH
    assert req.youtube_music_retry_information.retry_reason == '429 rate limit'
    assert req.youtube_music_retry_information.retry_backoff_seconds == 60

def test_state_machine_mark_retry_stores_count_and_resets_sent(fake_context):  #pylint:disable=redefined-outer-name
    """mark_retry_* stores the authoritative attempt count and re-arms the
    summary by clearing retry_reason_sent, for both download and search."""
    req = fake_source_dict(fake_context)
    req.download_retry_information.retry_reason_sent = True
    req.youtube_music_retry_information.retry_reason_sent = True

    req.state_machine.mark_retry_download('boom', 30, retry_count=2, max_retries=5)
    assert req.download_retry_information.retry_count == 2
    assert req.download_retry_information.retry_max == 5
    assert req.download_retry_information.retry_reason_sent is False

    req.state_machine.mark_retry_search('throttled', 60, retry_count=1, max_retries=4)
    assert req.youtube_music_retry_information.retry_count == 1
    assert req.youtube_music_retry_information.retry_max == 4
    assert req.youtube_music_retry_information.retry_reason_sent is False

def test_state_machine_mark_retry_download_omitted_count_preserved(fake_context):  #pylint:disable=redefined-outer-name
    """Omitting retry_count/max_retries leaves the existing values untouched
    (both default None), so an update from a caller that reports neither can't
    reset a budget an earlier worker already established."""
    req = fake_source_dict(fake_context)
    req.download_retry_information.retry_count = 5
    req.download_retry_information.retry_max = 8
    req.state_machine.mark_retry_download('error')
    assert req.download_retry_information.retry_count == 5
    assert req.download_retry_information.retry_max == 8

def test_state_machine_mark_retry_does_not_cross_contaminate(fake_context):  #pylint:disable=redefined-outer-name
    """mark_retry_download and mark_retry_search write to separate RetryInformation objects"""
    req = fake_source_dict(fake_context)
    req.state_machine.mark_retry_download('download error', 10)
    req.state_machine.mark_retry_search('search error', 20)
    assert req.download_retry_information.retry_reason == 'download error'
    assert req.youtube_music_retry_information.retry_reason == 'search error'

def test_state_machine_no_callback_no_error(fake_context):  #pylint:disable=redefined-outer-name
    """Transitions complete without error"""
    req = fake_source_dict(fake_context)
    req.state_machine.mark_completed()
    assert req.lifecycle_stage == MediaRequestLifecycleStage.COMPLETED
