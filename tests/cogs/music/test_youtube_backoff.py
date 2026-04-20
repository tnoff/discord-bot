from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock

import pytest

from discord_bot.exceptions import ExitEarlyException
from discord_bot.cogs.music import Music
from discord_bot.cogs.music_helpers.download_client import DownloadClient
from discord_bot.types.download import DownloadErrorType, DownloadResult, DownloadStatus
from discord_bot.utils.failure_queue import FailureStatus
from discord_bot.cogs.music_helpers.music_player import MusicPlayer

from tests.cogs.test_music import BASE_MUSIC_CONFIG, yield_fake_download_client
from tests.helpers import fake_engine, fake_context, fake_source_dict, fake_media_download #pylint:disable=unused-import


@pytest.mark.asyncio
async def test_backoff_wait_no_timestamp(fake_context):  #pylint:disable=redefined-outer-name
    """backoff_wait returns immediately when no timestamp is set."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    await cog.download_client.backoff_wait(cog.bot_shutdown_event)


@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_backoff_wait_elapsed(freezer, fake_context, mocker):  #pylint:disable=redefined-outer-name
    """backoff_wait returns normally when backoff period has already elapsed."""
    mocker.patch('discord_bot.cogs.music_helpers.download_client.random.randint', return_value=5000)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    freezer.move_to('2025-01-01 12:00:00 UTC')
    cog.download_client.set_wait_timestamp()
    freezer.move_to('2025-01-01 16:00:00 UTC')
    await cog.download_client.backoff_wait(cog.bot_shutdown_event)


@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_backoff_wait_raises_on_shutdown(freezer, fake_context, mocker):  #pylint:disable=redefined-outer-name
    """backoff_wait raises ExitEarlyException when bot_shutdown is set."""
    mocker.patch('discord_bot.cogs.music_helpers.download_client.random.randint', return_value=5000)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    freezer.move_to('2025-01-01 12:00:00 UTC')
    cog.download_client.set_wait_timestamp()
    cog.bot_shutdown_event.set()
    freezer.move_to('2025-01-01 16:00:00 UTC')
    with pytest.raises(ExitEarlyException) as exc:
        await cog.download_client.backoff_wait(cog.bot_shutdown_event)
    assert 'Exiting bot wait loop' in str(exc.value)


@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_set_wait_timestamp_backoff_multiplier(freezer, fake_context, mocker):  #pylint:disable=redefined-outer-name
    """set_wait_timestamp with backoff_multiplier=2 sets correct timestamp."""
    mocker.patch('discord_bot.cogs.music_helpers.download_client.random.randint', return_value=5000)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    freezer.move_to('2025-01-01 12:00:00 UTC')
    # backoff_multiplier=2: now (1735732800) + 30*2 + 5 = 1735732865
    cog.download_client.set_wait_timestamp(backoff_multiplier=2)
    assert cog.download_client.wait_timestamp == 1735732865
    # backoff_multiplier=1 (default): now (1735732800) + 30*1 + 5 = 1735732835
    cog.download_client.set_wait_timestamp()
    assert cog.download_client.wait_timestamp == 1735732835

@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_set_wait_timestamp_basic(freezer, fake_context, mocker):  #pylint:disable=redefined-outer-name
    """set_wait_timestamp sets correct timestamp with default multiplier."""
    mocker.patch('discord_bot.cogs.music_helpers.download_client.random.randint', return_value=5000)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    freezer.move_to('2025-01-01 12:00:00 UTC')
    cog.download_client.set_wait_timestamp()
    # Expected: now (1735732800) + 30*1 + 5 = 1735732835
    assert cog.download_client.wait_timestamp is not None
    assert cog.download_client.wait_timestamp == 1735732835
    # With backoff_multiplier=2: now (1735732800) + 30*2 + 5 = 1735732865
    cog.download_client.set_wait_timestamp(backoff_multiplier=2)
    assert cog.download_client.wait_timestamp == 1735732865

def yield_download_client_retryable_exception():
    """Fake download client that returns a retryable failure DownloadResult"""
    class FakeDownloadClient(DownloadClient):
        def __init__(self, *_args, **kwargs):
            super().__init__(Path("/tmp"), broker=kwargs.get('broker'), failure_queue=kwargs.get('failure_queue'),
                wait_period_minimum=kwargs.get('wait_period_minimum', 30),
                wait_period_max_variance=kwargs.get('wait_period_max_variance', 10),
            )

        async def create_source(self, media_request, *_args, **_kwargs):
            result = DownloadResult(status=DownloadStatus(success=False, error_type=DownloadErrorType.RETRYABLE, error_detail='Test retryable error'), media_request=media_request, ytdlp_data=None, file_name=None)
            self.update_tracking(result)
            return result

    return FakeDownloadClient


def yield_download_client_bot_flagged():
    """Fake download client that returns a BotDownloadFlagged failure DownloadResult"""
    class FakeDownloadClient(DownloadClient):
        def __init__(self, *_args, **kwargs):
            super().__init__(
                Path('/tmp'),
                broker=kwargs.get('broker'),
                failure_queue=kwargs.get('failure_queue'),
                wait_period_minimum=kwargs.get('wait_period_minimum', 30),
                wait_period_max_variance=kwargs.get('wait_period_max_variance', 10),
            )

        async def create_source(self, media_request, *_args, **_kwargs):
            result = DownloadResult(status=DownloadStatus(success=False, error_type=DownloadErrorType.BOT_FLAGGED, error_detail='Bot download flagged'), media_request=media_request, ytdlp_data=None, file_name=None)
            self.update_tracking(result)
            return result

    return FakeDownloadClient


@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_retryable_exception_adds_failure_to_queue(freezer, fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that RetryableException adds a failure to the download failure queue"""
    mocker.patch('discord_bot.cogs.music_helpers.download_client.random.randint', return_value=5000)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch('discord_bot.cogs.music_helpers.download_client.sleep', return_value=None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_download_client_retryable_exception())

    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    freezer.move_to('2025-01-01 12:00:00 UTC')

    s = fake_source_dict(fake_context)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.download_client.submit(fake_context['guild'].id, s)

    assert cog.download_client.failure_queue.size == 0

    await cog.download_client.run(cog.bot_shutdown_event)
    await cog.process_download_results()

    assert cog.download_client.failure_queue.size == 1


@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_retryable_exception_applies_exponential_backoff(freezer, fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that RetryableException applies exponential backoff based on failure queue size"""
    mocker.patch('discord_bot.cogs.music_helpers.download_client.random.randint', return_value=5000)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch('discord_bot.cogs.music_helpers.download_client.sleep', return_value=None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_download_client_retryable_exception())

    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    freezer.move_to('2025-01-01 12:00:00 UTC')

    # Pre-populate failure queue with 2 failures to test exponential backoff
    cog.download_client.failure_queue.add_item(FailureStatus(success=False, exception_type='RetryableException'))
    cog.download_client.failure_queue.add_item(FailureStatus(success=False, exception_type='RetryableException'))
    assert cog.download_client.failure_queue.size == 2

    s = fake_source_dict(fake_context)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.download_client.submit(fake_context['guild'].id, s)

    await cog.download_client.run(cog.bot_shutdown_event)
    await cog.process_download_results()

    # After failure, queue size is now 3
    assert cog.download_client.failure_queue.size == 3

    # Backoff multiplier should be 2^3 = 8 (since size is 3 after adding new failure)
    # Expected timestamp: now (1735732800) + 30*8 + 5 = 1735733045
    assert cog.download_client.wait_timestamp == 1735733045

@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_bot_download_flagged_applies_backoff(freezer, fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that BotDownloadFlagged (a RetryableException) applies proper backoff"""
    mocker.patch('discord_bot.cogs.music_helpers.download_client.random.randint', return_value=5000)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch('discord_bot.cogs.music_helpers.download_client.sleep', return_value=None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_download_client_bot_flagged())

    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    freezer.move_to('2025-01-01 12:00:00 UTC')

    s = fake_source_dict(fake_context)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.download_client.submit(fake_context['guild'].id, s)

    assert cog.download_client.wait_timestamp is None
    await cog.download_client.run(cog.bot_shutdown_event)
    await cog.process_download_results()

    # After BotDownloadFlagged, failure queue size is 1, so multiplier is 2^1 = 2
    # Expected timestamp: now (1735732800) + 30*2 + 5 = 1735732865
    assert cog.download_client.wait_timestamp == 1735732865
    assert cog.download_client.failure_queue.size == 1


@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_successful_download_clears_failure_from_queue(freezer, fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that successful download removes one item from failure queue"""
    mocker.patch('discord_bot.cogs.music_helpers.download_client.random.randint', return_value=5000)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch('discord_bot.cogs.music_helpers.download_client.sleep', return_value=None)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
            cog.dispatcher = MagicMock()
            freezer.move_to('2025-01-01 12:00:00 UTC')

            # Pre-populate failure queue
            cog.download_client.failure_queue.add_item(FailureStatus(success=False, exception_type='RetryableException'))
            cog.download_client.failure_queue.add_item(FailureStatus(success=False, exception_type='RetryableException'))
            assert cog.download_client.failure_queue.size == 2

            await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
            cog.download_client.submit(fake_context['guild'].id, sd.media_request)

            await cog.download_client.run(cog.bot_shutdown_event)
            await cog.process_download_results()

            # Successful download should remove one failure from queue
            assert cog.download_client.failure_queue.size == 1
