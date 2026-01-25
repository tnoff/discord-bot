from tempfile import TemporaryDirectory

import pytest

from discord_bot.exceptions import ExitEarlyException
from discord_bot.cogs.music import Music

from discord_bot.cogs.music_helpers.media_download import MediaDownload
from discord_bot.cogs.music_helpers.download_client import RetryableException, BotDownloadFlagged, DownloadStatus
from discord_bot.cogs.music_helpers.music_player import MusicPlayer

from tests.cogs.test_music import BASE_MUSIC_CONFIG, yield_fake_download_client
from tests.helpers import fake_engine, fake_context, fake_source_dict, fake_media_download #pylint:disable=unused-import


@pytest.mark.asyncio
async def test_youtube_backoff_time_doesnt_exist_yet(fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    assert await cog.youtube_backoff_time()

@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_youtube_backoff_time(freezer, fake_context, mocker):  #pylint:disable=redefined-outer-name
    # Mock random.randint to return consistent value
    mocker.patch('discord_bot.cogs.music.random.randint', return_value=5000)
    sd = MediaDownload(None, {
        'extractor': 'youtube'
    }, None)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    freezer.move_to('2025-01-01 12:00:00 UTC')
    cog.update_download_timestamp(sd)
    freezer.move_to('2025-01-01 16:00:00 UTC')
    await cog.youtube_backoff_time()

@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_youtube_backoff_time_with_bot_shutdown(freezer, fake_context, mocker):  #pylint:disable=redefined-outer-name
    # Mock random.randint to return consistent value
    mocker.patch('discord_bot.cogs.music.random.randint', return_value=5000)
    sd = MediaDownload(None, {
        'extractor': 'youtube'
    }, None)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    freezer.move_to('2025-01-01 12:00:00 UTC')
    cog.update_download_timestamp(sd)
    cog.bot_shutdown_event.set()
    freezer.move_to('2025-01-01 16:00:00 UTC')
    with pytest.raises(ExitEarlyException) as exc:
        await cog.youtube_backoff_time()
    assert 'Exiting bot wait loop' in str(exc.value)

@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_youtube_last_update_time_with_more_backoff(freezer, fake_context, mocker):  #pylint:disable=redefined-outer-name
    # Mock random.randint to return consistent value (5000 / 1000 = 5 seconds variance)
    mocker.patch('discord_bot.cogs.music.random.randint', return_value=5000)
    sd = MediaDownload(None, {
        'extractor': 'youtube'
    }, None)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    freezer.move_to('2025-01-01 12:00:00 UTC')
    # backoff_multiplier=2: now (1735732800) + 30*2 + 5 = 1735732865
    cog.update_download_timestamp(sd, backoff_multiplier=2)
    assert cog.youtube_download_wait_timestamp == 1735732865
    # backoff_multiplier=1 (default): now (1735732800) + 30*1 + 5 = 1735732835
    cog.update_download_timestamp(sd)
    assert cog.youtube_download_wait_timestamp == 1735732835

@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_update_download_timestamp_method(freezer, fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test update_download_timestamp method"""
    # Mock random.randint to return consistent value (5000 / 1000 = 5 seconds variance)
    mocker.patch('discord_bot.cogs.music.random.randint', return_value=5000)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    sd = MediaDownload(None, {
        'extractor': 'youtube'
    }, None)

    # Test basic timestamp update
    freezer.move_to('2025-01-01 12:00:00 UTC')
    cog.update_download_timestamp(sd)

    # Verify timestamp was set
    # Expected: now (1735732800) + 30*1 + 5 = 1735732835
    assert cog.youtube_download_wait_timestamp is not None
    timestamp = cog.youtube_download_wait_timestamp
    assert timestamp == 1735732835

    # Test with backoff_multiplier=2
    # Expected: now (1735732800) + 30*2 + 5 = 1735732865
    cog.update_download_timestamp(sd, backoff_multiplier=2)

    new_timestamp = cog.youtube_download_wait_timestamp
    assert new_timestamp == 1735732865


def yield_download_client_retryable_exception():
    """Fake download client that raises RetryableException"""
    class FakeDownloadClient():
        def __init__(self, *_args, **_kwargs):
            pass

        async def create_source(self, media_request, *_args, **_kwargs):
            raise RetryableException('Test retryable error', media_request=media_request)

    return FakeDownloadClient


def yield_download_client_bot_flagged():
    """Fake download client that raises BotDownloadFlagged"""
    class FakeDownloadClient():
        def __init__(self, *_args, **_kwargs):
            pass

        async def create_source(self, media_request, *_args, **_kwargs):
            raise BotDownloadFlagged('Bot download flagged', media_request=media_request)

    return FakeDownloadClient


@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_retryable_exception_adds_failure_to_queue(freezer, fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that RetryableException adds a failure to download_failure_queue"""
    mocker.patch('discord_bot.cogs.music.random.randint', return_value=5000)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_download_client_retryable_exception())

    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    freezer.move_to('2025-01-01 12:00:00 UTC')

    # Create a media request
    s = fake_source_dict(fake_context)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.download_queue.put_nowait(fake_context['guild'].id, s)

    # Verify failure queue is empty before
    assert cog.download_failure_queue.size == 0

    await cog.download_files()

    # Verify failure was added to queue
    assert cog.download_failure_queue.size == 1


@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_retryable_exception_applies_exponential_backoff(freezer, fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that RetryableException applies exponential backoff based on failure queue size"""
    mocker.patch('discord_bot.cogs.music.random.randint', return_value=5000)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_download_client_retryable_exception())

    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    freezer.move_to('2025-01-01 12:00:00 UTC')

    # Pre-populate failure queue with 2 failures to test exponential backoff
    cog.download_failure_queue.add_item(DownloadStatus(success=False, exception_type='RetryableException'))
    cog.download_failure_queue.add_item(DownloadStatus(success=False, exception_type='RetryableException'))
    assert cog.download_failure_queue.size == 2

    s = fake_source_dict(fake_context)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.download_queue.put_nowait(fake_context['guild'].id, s)

    await cog.download_files()

    # After failure, queue size is now 3
    assert cog.download_failure_queue.size == 3

    # Backoff multiplier should be 2^3 = 8 (since size is 3 after adding new failure)
    # Expected timestamp: now (1735732800) + 30*8 + 5 = 1735733045
    assert cog.youtube_download_wait_timestamp == 1735733045


@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_bot_download_flagged_applies_backoff(freezer, fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that BotDownloadFlagged (a RetryableException) applies proper backoff"""
    mocker.patch('discord_bot.cogs.music.random.randint', return_value=5000)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_download_client_bot_flagged())

    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    freezer.move_to('2025-01-01 12:00:00 UTC')

    s = fake_source_dict(fake_context)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.download_queue.put_nowait(fake_context['guild'].id, s)

    # Verify timestamp not set before
    assert cog.youtube_download_wait_timestamp is None

    await cog.download_files()

    # After BotDownloadFlagged, failure queue size is 1, so multiplier is 2^1 = 2
    # Expected timestamp: now (1735732800) + 30*2 + 5 = 1735732865
    assert cog.youtube_download_wait_timestamp == 1735732865
    assert cog.download_failure_queue.size == 1


@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_successful_download_clears_failure_from_queue(freezer, fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that successful download removes one item from failure queue"""
    mocker.patch('discord_bot.cogs.music.random.randint', return_value=5000)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
            freezer.move_to('2025-01-01 12:00:00 UTC')

            # Pre-populate failure queue
            cog.download_failure_queue.add_item(DownloadStatus(success=False, exception_type='RetryableException'))
            cog.download_failure_queue.add_item(DownloadStatus(success=False, exception_type='RetryableException'))
            assert cog.download_failure_queue.size == 2

            await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
            cog.download_queue.put_nowait(fake_context['guild'].id, sd.media_request)

            await cog.download_files()

            # Successful download should remove one failure from queue
            assert cog.download_failure_queue.size == 1
