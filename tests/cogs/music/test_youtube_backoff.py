from datetime import datetime

import pytest
from freezegun import freeze_time

from discord_bot.cogs.music import Music

from tests.helpers import fake_context #pylint:disable=unused-import

BASE_MUSIC_CONFIG = {
    'general': {
        'include': {
            'music': True
        }
    },
}

@pytest.mark.asyncio()
async def test_youtube_backoff_time_doesnt_exist_yet(fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    result = await cog.youtube_backoff_time_seconds()
    assert result == 0

@pytest.mark.asyncio()
async def test_youtube_backoff_time(freezer, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    test_time = datetime(2022, 1, 1, 10, 0, 0)
    freezer.move_to(test_time)
    # Set up a backoff scenario 
    cog.youtube_last_update_time[fake_context['guild'].id] = datetime(2022, 1, 1, 10, 0, 0)
    cog.youtube_backoff_multiplicand[fake_context['guild'].id] = 60
    freezer.move_to(datetime(2022, 1, 1, 10, 0, 30))
    result = await cog.youtube_backoff_time_seconds()
    # Should return remaining backoff time (60 - 30 = 30 seconds)
    assert result == 30

@pytest.mark.asyncio()
async def test_youtube_backoff_time_with_bot_shutdown(freezer, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    test_time = datetime(2022, 1, 1, 10, 0, 0)
    freezer.move_to(test_time)
    # Set up a backoff scenario 
    cog.youtube_last_update_time[fake_context['guild'].id] = datetime(2022, 1, 1, 10, 0, 0)
    cog.youtube_backoff_multiplicand[fake_context['guild'].id] = 60
    # Set bot shutdown flag
    cog.bot_shutting_down = True
    freezer.move_to(datetime(2022, 1, 1, 10, 0, 30))
    result = await cog.youtube_backoff_time_seconds()
    # Should return 0 when bot is shutting down
    assert result == 0

@pytest.mark.asyncio()
async def test_youtube_last_update_time_with_more_backoff(freezer, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    test_time = datetime(2022, 1, 1, 10, 0, 0)
    freezer.move_to(test_time)
    # Set up initial backoff
    cog.youtube_last_update_time[fake_context['guild'].id] = datetime(2022, 1, 1, 10, 0, 0)
    cog.youtube_backoff_multiplicand[fake_context['guild'].id] = 30
    # Move time forward past backoff period 
    freezer.move_to(datetime(2022, 1, 1, 10, 0, 45))
    # This should reset the backoff since enough time has passed
    result = await cog.youtube_backoff_time_seconds()
    assert result == 0
    # Check that backoff multiplicand was reset
    assert fake_context['guild'].id not in cog.youtube_backoff_multiplicand