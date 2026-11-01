from unittest.mock import MagicMock

import pytest

from discord_gateway.cogs.music import Music
from discord_gateway.cogs.music_helpers.music_player import MusicPlayer

from discord_gateway.tests.cogs.test_music import BASE_MUSIC_CONFIG
from tests.helpers import fake_engine, fake_context  # pylint: disable=unused-import
from tests.helpers import attach_in_process_broker


@pytest.mark.asyncio
async def test_get_player(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """Test basic player creation"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    assert fake_context['guild'].id in cog.players


@pytest.mark.asyncio
async def test_get_player_join_channel(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """Test player creation with join channel"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'], join_channel=fake_context['channel'])
    assert fake_context['guild'].id in cog.players


@pytest.mark.asyncio
async def test_get_player_no_create(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """Test get_player with create_player=False returns None when player doesn't exist"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    mocker.patch.object(MusicPlayer, 'start_tasks')
    assert await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'], create_player=False) is None


@pytest.mark.asyncio
async def test_cog_load_creates_background_tasks(fake_context):  # pylint: disable=redefined-outer-name
    """cog_load sets up the dispatcher and creates background loop tasks."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    # Provide a real (or mock) event loop so create_task doesn't fail
    loop_mock = MagicMock()
    loop_mock.create_task = MagicMock(return_value=MagicMock())
    fake_context['bot'].loop = loop_mock
    # get_cog returns a fake dispatcher
    fake_context['bot'].get_cog = MagicMock(return_value=MagicMock())

    await cog.cog_load()

    assert loop_mock.create_task.call_count >= 3  # cleanup, download, youtube_search


@pytest.mark.asyncio
async def test_cog_load_starts_the_resume_task(fake_context):  # pylint: disable=redefined-outer-name
    """cog_load starts the one-shot that picks guilds back up after a restart."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    loop_mock = MagicMock()
    loop_mock.create_task = MagicMock(return_value=MagicMock())
    fake_context['bot'].loop = loop_mock
    fake_context['bot'].get_cog = MagicMock(return_value=MagicMock())

    await cog.cog_load()

    # peer wait, cleanup, resume, gateway intents: the result loops run in the commands pod, and no loop
    # records plays any more
    assert loop_mock.create_task.call_count == 4
    assert cog._init_task is loop_mock.create_task.return_value  # pylint: disable=protected-access
