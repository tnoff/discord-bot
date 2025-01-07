import logging
from tempfile import NamedTemporaryFile

import pytest
from sqlalchemy import create_engine

from discord_bot.database import BASE
from discord_bot.exceptions import ExitEarlyException
from discord_bot.cogs.music import Music

from discord_bot.cogs.music_helpers.source_download import SourceDownload

from tests.helpers import fake_bot_yielder


@pytest.mark.asyncio
async def test_youtube_backoff_time_doesnt_exist_yet():
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'music': True
                }
            },
        }
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, logging, config, engine)
        assert await cog.youtube_backoff_time(10, 10)

@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_youtube_backoff_time(freezer):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'music': True
                }
            },
        }
        sd = SourceDownload(None, {
            'extractor': 'youtube'
        }, None)
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, logging, config, engine)
        freezer.move_to('2025-01-01 12:00:00 UTC')
        cog.update_download_lockfile(sd)
        freezer.move_to('2025-01-01 16:00:00 UTC')
        await cog.youtube_backoff_time(cog.youtube_wait_period_min, cog.youtube_wait_period_max_variance)

@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_youtube_backoff_time_with_bot_shutdown(freezer):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'music': True
                }
            },
        }
        sd = SourceDownload(None, {
            'extractor': 'youtube'
        }, None)
        fake_bot = fake_bot_yielder()()
        cog = Music(fake_bot, logging, config, engine)
        freezer.move_to('2025-01-01 12:00:00 UTC')
        cog.update_download_lockfile(sd)
        cog.bot_shutdown = True
        freezer.move_to('2025-01-01 16:00:00 UTC')
        with pytest.raises(ExitEarlyException) as exc:
            await cog.youtube_backoff_time(cog.youtube_wait_period_min, cog.youtube_wait_period_max_variance)
        assert 'Exiting bot wait loop' in str(exc.value)
