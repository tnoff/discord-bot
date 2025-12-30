import pytest

from discord_bot.exceptions import ExitEarlyException
from discord_bot.cogs.music import Music

from discord_bot.cogs.music_helpers.media_download import MediaDownload

from tests.cogs.test_music import BASE_MUSIC_CONFIG
from tests.helpers import fake_engine, fake_context #pylint:disable=unused-import


@pytest.mark.asyncio
async def test_youtube_backoff_time_doesnt_exist_yet(fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    assert await cog.youtube_backoff_time(10, 10)

@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_youtube_backoff_time(freezer, fake_context):  #pylint:disable=redefined-outer-name
    sd = MediaDownload(None, {
        'extractor': 'youtube'
    }, None)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    freezer.move_to('2025-01-01 12:00:00 UTC')
    cog.update_download_timestamp(sd)
    freezer.move_to('2025-01-01 16:00:00 UTC')
    await cog.youtube_backoff_time(cog.config.download.youtube_wait_period_minimum, cog.config.download.youtube_wait_period_max_variance)

@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_youtube_backoff_time_with_bot_shutdown(freezer, fake_context):  #pylint:disable=redefined-outer-name
    sd = MediaDownload(None, {
        'extractor': 'youtube'
    }, None)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    freezer.move_to('2025-01-01 12:00:00 UTC')
    cog.update_download_timestamp(sd)
    cog.bot_shutdown_event.set()
    freezer.move_to('2025-01-01 16:00:00 UTC')
    with pytest.raises(ExitEarlyException) as exc:
        await cog.youtube_backoff_time(cog.config.download.youtube_wait_period_minimum, cog.config.download.youtube_wait_period_max_variance)
    assert 'Exiting bot wait loop' in str(exc.value)

@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_youtube_last_update_time_with_more_backoff(freezer, fake_context):  #pylint:disable=redefined-outer-name
    sd = MediaDownload(None, {
        'extractor': 'youtube'
    }, None)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    freezer.move_to('2025-01-01 12:00:00 UTC')
    cog.update_download_timestamp(sd, add_additional_backoff=60)
    assert cog.last_download_timestamp == 1735732860
    cog.update_download_timestamp(sd)
    assert cog.last_download_timestamp == 1735732800

@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_update_download_timestamp_method(freezer, fake_context):  #pylint:disable=redefined-outer-name
    """Test update_download_timestamp method"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    sd = MediaDownload(None, {
        'extractor': 'youtube'
    }, None)

    # Test basic timestamp update
    freezer.move_to('2025-01-01 12:00:00 UTC')
    cog.update_download_timestamp(sd)

    # Verify timestamp was set
    assert cog.last_download_timestamp is not None
    timestamp = cog.last_download_timestamp
    assert timestamp > 0

    # Test with additional backoff
    original_timestamp = timestamp
    cog.update_download_timestamp(sd, add_additional_backoff=60)

    new_timestamp = cog.last_download_timestamp
    assert new_timestamp == original_timestamp + 60
