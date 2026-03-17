# pylint: disable=redefined-outer-name,protected-access
"""
Tests for ha_download_result_loop — the HA-mode replacement for download_files.

These tests cover:
  - Empty result queue → early return
  - Bot shutdown → ExitEarlyException
  - Successful result → MediaDownload registered, added to player
  - Worker-reported error → __return_bad_video called
  - Missing pending entry (already discarded) → no-op
  - Missing player → mark_discarded
  - Player in shutdown → mark_discarded
  - File missing from disk → __return_bad_video called
"""
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from unittest.mock import MagicMock

import pytest

from discord_bot.cogs.music import Music
from discord_bot.cogs.music_helpers.common import MediaRequestLifecycleStage
from discord_bot.cogs.music_helpers.music_player import MusicPlayer
from discord_bot.exceptions import ExitEarlyException
from discord_bot.utils.broker_http_server import BrokerResult

from tests.cogs.test_music import BASE_MUSIC_CONFIG
from tests.helpers import fake_source_dict, fake_context  #pylint:disable=unused-import


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

HA_MUSIC_CONFIG = BASE_MUSIC_CONFIG | {
    'music': {
        'ha': {
            'enabled': True,
            'redis_url': 'redis://localhost:6379',
            'broker_host': '127.0.0.1',
            'broker_port': 18765,
        }
    }
}


def _make_ha_cog(fake_context):
    """Create a Music cog in HA mode with common mocks applied."""
    cog = Music(fake_context['bot'], HA_MUSIC_CONFIG, None)
    cog.dispatcher = MagicMock()
    return cog


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def ha_cog(fake_context, mocker):
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    return _make_ha_cog(fake_context)


# ---------------------------------------------------------------------------
# Empty queue
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ha_loop_empty_queue_returns_early(ha_cog):
    """Nothing in the result queue → loop returns without error."""
    assert ha_cog._broker_result_queue.empty()
    # Should not raise
    await ha_cog.ha_download_result_loop()


# ---------------------------------------------------------------------------
# Bot shutdown
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ha_loop_shutdown_raises_exit_early(ha_cog):
    ha_cog.bot_shutdown_event.set()
    with pytest.raises(ExitEarlyException):
        await ha_cog.ha_download_result_loop()


# ---------------------------------------------------------------------------
# Missing pending entry
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ha_loop_missing_pending_no_op(ha_cog, fake_context):
    """Result arrives but the original MediaRequest was already discarded."""
    result = BrokerResult(
        media_request_uuid='not-in-pending',
        guild_id=fake_context['guild'].id,
        file_path=Path('/tmp/nonexistent.mp3'),
    )
    await ha_cog._broker_result_queue.put(result)
    # _pending is empty so pop_pending returns None → should return silently
    await ha_cog.ha_download_result_loop()
    # Queue consumed
    assert ha_cog._broker_result_queue.empty()


# ---------------------------------------------------------------------------
# Worker-reported error
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ha_loop_error_result_calls_return_bad_video(ha_cog, fake_context):
    media_request = fake_source_dict(fake_context)
    result = BrokerResult(
        media_request_uuid=str(media_request.uuid),
        guild_id=fake_context['guild'].id,
        file_path=None,
        error_message='Download failed: age restricted',
    )
    await ha_cog._broker_result_queue.put(result)
    ha_cog.download_queue._pending[str(media_request.uuid)] = media_request

    await ha_cog.ha_download_result_loop()

    assert media_request.lifecycle_stage == MediaRequestLifecycleStage.FAILED
    assert 'age restricted' in (media_request.failure_reason or '')


# ---------------------------------------------------------------------------
# No player for guild
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ha_loop_no_player_marks_discarded(ha_cog, fake_context):
    media_request = fake_source_dict(fake_context)
    with TemporaryDirectory() as tmp:
        with NamedTemporaryFile(dir=tmp, suffix='.mp3', delete=False) as f:
            f.write(b'audio')
            file_path = Path(f.name)
        result = BrokerResult(
            media_request_uuid=str(media_request.uuid),
            guild_id=fake_context['guild'].id,
            file_path=file_path,
            ytdlp_data={'title': 'test', 'ext': 'mp3'},
        )
        await ha_cog._broker_result_queue.put(result)
        ha_cog.download_queue._pending[str(media_request.uuid)] = media_request

        # No player exists for this guild
        assert fake_context['guild'].id not in ha_cog.players

        await ha_cog.ha_download_result_loop()

    assert media_request.lifecycle_stage == MediaRequestLifecycleStage.DISCARDED


# ---------------------------------------------------------------------------
# Player in shutdown
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ha_loop_player_shutdown_marks_discarded(ha_cog, fake_context, mocker):
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await ha_cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    ha_cog.players[fake_context['guild'].id].shutdown_called = True

    media_request = fake_source_dict(fake_context)
    with TemporaryDirectory() as tmp:
        with NamedTemporaryFile(dir=tmp, suffix='.mp3', delete=False) as f:
            f.write(b'audio')
            file_path = Path(f.name)
        result = BrokerResult(
            media_request_uuid=str(media_request.uuid),
            guild_id=fake_context['guild'].id,
            file_path=file_path,
            ytdlp_data={'title': 'test', 'ext': 'mp3'},
        )
        await ha_cog._broker_result_queue.put(result)
        ha_cog.download_queue._pending[str(media_request.uuid)] = media_request

        await ha_cog.ha_download_result_loop()

    assert media_request.lifecycle_stage == MediaRequestLifecycleStage.DISCARDED


# ---------------------------------------------------------------------------
# File missing from disk
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ha_loop_missing_file_calls_return_bad_video(ha_cog, fake_context, mocker):
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await ha_cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])

    media_request = fake_source_dict(fake_context)
    result = BrokerResult(
        media_request_uuid=str(media_request.uuid),
        guild_id=fake_context['guild'].id,
        file_path=Path('/tmp/does_not_exist_12345.mp3'),  # nonexistent
        ytdlp_data={'title': 'test', 'ext': 'mp3'},
    )
    await ha_cog._broker_result_queue.put(result)
    ha_cog.download_queue._pending[str(media_request.uuid)] = media_request

    await ha_cog.ha_download_result_loop()

    assert media_request.lifecycle_stage == MediaRequestLifecycleStage.FAILED


# ---------------------------------------------------------------------------
# Successful result — full happy path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ha_loop_success_adds_to_player(ha_cog, fake_context, mocker):
    """Successful BrokerResult → MediaDownload created and added to player queue."""
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await ha_cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])

    media_request = fake_source_dict(fake_context)

    with TemporaryDirectory() as tmp:
        with NamedTemporaryFile(dir=tmp, suffix='.mp3', delete=False) as f:
            f.write(b'real audio data')
            file_path = Path(f.name)

        ytdlp_data = {
            'title': 'Test Song',
            'id': 'abc',
            'webpage_url': 'https://youtube.com/watch?v=abc',
            'uploader': 'Artist',
            'duration': 180,
            'extractor': 'youtube',
            'ext': 'mp3',
        }
        result = BrokerResult(
            media_request_uuid=str(media_request.uuid),
            guild_id=fake_context['guild'].id,
            file_path=file_path,
            ytdlp_data=ytdlp_data,
        )
        await ha_cog._broker_result_queue.put(result)
        ha_cog.download_queue._pending[str(media_request.uuid)] = media_request

        await ha_cog.ha_download_result_loop()

    assert media_request.lifecycle_stage == MediaRequestLifecycleStage.COMPLETED
    queue_items = ha_cog.players[fake_context['guild'].id].get_queue_items()
    assert len(queue_items) == 1
    assert queue_items[0].title == 'Test Song'


# ---------------------------------------------------------------------------
# pop_pending removes the entry
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ha_loop_consumes_pending_entry(ha_cog, fake_context, mocker):
    """After processing, the entry is no longer in _pending."""
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await ha_cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])

    media_request = fake_source_dict(fake_context)

    with TemporaryDirectory() as tmp:
        with NamedTemporaryFile(dir=tmp, suffix='.mp3', delete=False) as f:
            f.write(b'audio')
            file_path = Path(f.name)

        result = BrokerResult(
            media_request_uuid=str(media_request.uuid),
            guild_id=fake_context['guild'].id,
            file_path=file_path,
            ytdlp_data={'title': 'T', 'id': 'x', 'webpage_url': 'https://ex.com',
                        'uploader': 'U', 'duration': 1, 'extractor': 'yt', 'ext': 'mp3'},
        )
        ha_cog.download_queue._pending[str(media_request.uuid)] = media_request
        await ha_cog._broker_result_queue.put(result)

        await ha_cog.ha_download_result_loop()

    assert str(media_request.uuid) not in ha_cog.download_queue._pending
