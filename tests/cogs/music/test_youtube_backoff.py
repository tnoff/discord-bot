from tempfile import TemporaryDirectory
import pytest

from discord_bot.exceptions import ExitEarlyException
from discord_bot.cogs.music import Music

from discord_bot.cogs.music_helpers.media_download import MediaDownload

from tests.cogs.test_music import BASE_MUSIC_CONFIG
from tests.helpers import fake_engine, fake_context #pylint:disable=unused-import
from tests.helpers import fake_media_download


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
    cog.update_download_lockfile(sd)
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
    cog.update_download_lockfile(sd)
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
    cog.update_download_lockfile(sd, add_additional_backoff=60)
    assert cog.last_download_lockfile.read_text(encoding='utf-8') == '1735732860'
    cog.update_download_lockfile(sd)
    assert cog.last_download_lockfile.read_text(encoding='utf-8') == '1735732800'

@pytest.mark.asyncio
async def test_update_download_lockfile_method(fake_context):  #pylint:disable=redefined-outer-name
    """Test update_download_lockfile method"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            # Test basic lockfile update
            cog.update_download_lockfile(sd)

            # Verify lockfile was created and contains timestamp
            assert cog.last_download_lockfile.exists()
            timestamp_str = cog.last_download_lockfile.read_text(encoding='utf-8')
            timestamp = int(timestamp_str)
            assert timestamp > 0

            # Test with additional backoff
            original_timestamp = timestamp
            cog.update_download_lockfile(sd, add_additional_backoff=60)

            new_timestamp_str = cog.last_download_lockfile.read_text(encoding='utf-8')
            new_timestamp = int(new_timestamp_str)
            assert new_timestamp == original_timestamp + 60
