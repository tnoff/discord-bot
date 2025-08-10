import pytest

from discord_bot.cogs.music import Music

from discord_bot.cogs.music_helpers.music_player import MusicPlayer

from tests.cogs.test_music import BASE_MUSIC_CONFIG
from tests.helpers import fake_engine, fake_context #pylint:disable=unused-import

@pytest.mark.asyncio
async def test_cleanup_players_just_bot(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'], create_player=True, join_channel=fake_context['channel'])
    fake_context['channel'].members = [fake_context['bot'].user]
    await cog.cleanup_players()
    assert fake_context['guild'].id not in cog.players
