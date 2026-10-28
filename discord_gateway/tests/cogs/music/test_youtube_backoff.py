import pytest

from discord_core.exceptions import ExitEarlyException

from discord_gateway.cogs.music import Music
from discord_gateway.tests.cogs.test_music import BASE_MUSIC_CONFIG

from tests.helpers import fake_engine, fake_context #pylint:disable=unused-import
from tests.helpers import attach_in_process_download


@pytest.mark.asyncio
async def test_backoff_wait_no_timestamp(fake_context):  #pylint:disable=redefined-outer-name
    """backoff_wait returns immediately when no timestamp is set."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_download(cog)
    await cog.download_client.local_worker.backoff_wait(cog.bot_shutdown_event)


@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_backoff_wait_elapsed(freezer, fake_context, mocker):  #pylint:disable=redefined-outer-name
    """backoff_wait returns normally when backoff period has already elapsed."""
    mocker.patch('discord_downloader.interfaces.download_protocols.random.randint', return_value=5000)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_download(cog)
    freezer.move_to('2025-01-01 12:00:00 UTC')
    cog.download_client.local_worker.set_wait_timestamp()
    freezer.move_to('2025-01-01 16:00:00 UTC')
    await cog.download_client.local_worker.backoff_wait(cog.bot_shutdown_event)


@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_backoff_wait_raises_on_shutdown(freezer, fake_context, mocker):  #pylint:disable=redefined-outer-name
    """backoff_wait raises ExitEarlyException when bot_shutdown is set."""
    mocker.patch('discord_downloader.interfaces.download_protocols.random.randint', return_value=5000)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_download(cog)
    freezer.move_to('2025-01-01 12:00:00 UTC')
    cog.download_client.local_worker.set_wait_timestamp()
    cog.bot_shutdown_event.set()
    freezer.move_to('2025-01-01 16:00:00 UTC')
    with pytest.raises(ExitEarlyException) as exc:
        await cog.download_client.local_worker.backoff_wait(cog.bot_shutdown_event)
    assert 'Exiting bot wait loop' in str(exc.value)


@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_set_wait_timestamp_backoff_multiplier(freezer, fake_context, mocker):  #pylint:disable=redefined-outer-name
    """set_wait_timestamp with backoff_multiplier=2 sets correct timestamp."""
    mocker.patch('discord_downloader.interfaces.download_protocols.random.randint', return_value=5000)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_download(cog)
    freezer.move_to('2025-01-01 12:00:00 UTC')
    # backoff_multiplier=2: now (1735732800) + 30*2 + 5 = 1735732865
    cog.download_client.local_worker.set_wait_timestamp(backoff_multiplier=2)
    assert cog.download_client.local_worker.wait_timestamp == 1735732865
    # backoff_multiplier=1 (default): now (1735732800) + 30*1 + 5 = 1735732835
    cog.download_client.local_worker.set_wait_timestamp()
    assert cog.download_client.local_worker.wait_timestamp == 1735732835

@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_set_wait_timestamp_basic(freezer, fake_context, mocker):  #pylint:disable=redefined-outer-name
    """set_wait_timestamp sets correct timestamp with default multiplier."""
    mocker.patch('discord_downloader.interfaces.download_protocols.random.randint', return_value=5000)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_download(cog)

    freezer.move_to('2025-01-01 12:00:00 UTC')
    cog.download_client.local_worker.set_wait_timestamp()
    # Expected: now (1735732800) + 30*1 + 5 = 1735732835
    assert cog.download_client.local_worker.wait_timestamp is not None
    assert cog.download_client.local_worker.wait_timestamp == 1735732835
    # With backoff_multiplier=2: now (1735732800) + 30*2 + 5 = 1735732865
    cog.download_client.local_worker.set_wait_timestamp(backoff_multiplier=2)
    assert cog.download_client.local_worker.wait_timestamp == 1735732865
