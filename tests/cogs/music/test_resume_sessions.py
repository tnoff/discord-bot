'''Tests for resume-after-restart: saving a player session on shutdown and
replaying it on the next startup.'''
from asyncio import QueueFull
from unittest.mock import MagicMock

import pytest

from discord_bot.cogs.music import Music
from discord_bot.cogs.music_helpers.music_player import MusicPlayer
from discord_bot.types.cleanup_reason import CleanupReason
from discord_bot.types.player_session import PlayerSession

from tests.cogs.test_music import BASE_MUSIC_CONFIG
from tests.helpers import (attach_in_process_search, FakeChannel, FakeGuild, FakeVoiceClient,  #pylint:disable=unused-import
                           fake_engine, fake_context, fake_source_dict, fake_media_download)


class _FakeMember:
    '''Voice-channel occupant; bot=True stands in for the bot's own presence.'''
    def __init__(self, is_bot: bool = False):
        self.bot = is_bot


def _voice_channel(members) -> FakeChannel:
    channel = FakeChannel()
    channel.members = members
    return channel


async def _player_in_voice(cog, fake_context, mocker, voice_channel):  #pylint:disable=redefined-outer-name
    '''Build a player for the fixture guild that is sitting in voice_channel.'''
    mocker.patch.object(MusicPlayer, 'start_tasks')
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    voice_client = FakeVoiceClient(guild=fake_context['guild'])
    voice_client.channel = voice_channel
    fake_context['guild'].voice_client = voice_client
    return player


# ---------------------------------------------------------------------------
# Saving a session on shutdown
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_shutdown_saves_session_with_queue(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''BOT_SHUTDOWN records the voice channel, text channel and queued requests.'''
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    voice_channel = _voice_channel([_FakeMember()])
    player = await _player_in_voice(cog, fake_context, mocker, voice_channel)

    with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
        player.add_to_play_queue(media_download)

        await cog.cleanup(fake_context['guild'], reason=CleanupReason.BOT_SHUTDOWN)

    sessions = await cog.broker_client.list_player_sessions()
    assert len(sessions) == 1
    assert sessions[0].guild_id == fake_context['guild'].id
    assert sessions[0].voice_channel_id == voice_channel.id
    assert sessions[0].text_channel_id == fake_context['channel'].id
    assert [str(r.uuid) for r in sessions[0].queue] == [str(media_download.media_request.uuid)]


@pytest.mark.asyncio
async def test_shutdown_records_was_playing_false_when_idle(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''A player parked in voice with nothing playing is saved as not-playing, so
    the resume guard skips it rather than rejoining an idle channel.'''
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    await _player_in_voice(cog, fake_context, mocker, _voice_channel([_FakeMember()]))

    await cog.cleanup(fake_context['guild'], reason=CleanupReason.BOT_SHUTDOWN)

    sessions = await cog.broker_client.list_player_sessions()
    assert sessions[0].was_playing is False


@pytest.mark.asyncio
async def test_shutdown_records_was_playing_true_mid_track(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''A player mid-track is saved as playing.'''
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    player = await _player_in_voice(cog, fake_context, mocker, _voice_channel([_FakeMember()]))

    with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
        player.current_media_download = media_download

        await cog.cleanup(fake_context['guild'], reason=CleanupReason.BOT_SHUTDOWN)

    sessions = await cog.broker_client.list_player_sessions()
    assert sessions[0].was_playing is True
    # The in-progress track leads the queue — a resume restarts it from the top
    assert str(sessions[0].queue[0].uuid) == str(media_download.media_request.uuid)


@pytest.mark.asyncio
async def test_shutdown_without_voice_client_saves_nothing(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''No voice channel means nothing to rejoin, so no session is written.'''
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    fake_context['guild'].voice_client = None

    await cog.cleanup(fake_context['guild'], reason=CleanupReason.BOT_SHUTDOWN)

    assert await cog.broker_client.list_player_sessions() == []


@pytest.mark.asyncio
async def test_non_shutdown_cleanup_saves_nothing(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''Only BOT_SHUTDOWN writes a session — the other reasons mean the guild is
    genuinely done, not coming back.'''
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_search(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    await _player_in_voice(cog, fake_context, mocker, _voice_channel([_FakeMember()]))

    await cog.cleanup(fake_context['guild'], reason=CleanupReason.VOICE_INACTIVE)

    assert await cog.broker_client.list_player_sessions() == []


# ---------------------------------------------------------------------------
# Resuming a session on startup
# ---------------------------------------------------------------------------

def _resumable_cog(fake_context, mocker, voice_members):  #pylint:disable=redefined-outer-name
    '''A cog whose bot can resolve the fixture guild + a populated voice channel.'''
    voice_channel = _voice_channel(voice_members)
    guild = fake_context['guild']
    guild.channels = [voice_channel, fake_context['channel']]
    fake_context['bot'].guilds = [guild]
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    return cog, voice_channel


def _session(fake_context, voice_channel, requests, was_playing=True) -> PlayerSession:  #pylint:disable=redefined-outer-name
    return PlayerSession(
        guild_id=fake_context['guild'].id,
        voice_channel_id=voice_channel.id,
        text_channel_id=fake_context['channel'].id,
        queue=requests,
        was_playing=was_playing,
    )


@pytest.mark.asyncio
async def test_resume_rejoins_and_requeues(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''A live session rebuilds the player and re-submits its requests.'''
    cog, voice_channel = _resumable_cog(fake_context, mocker, [_FakeMember()])
    request = fake_source_dict(fake_context)
    await cog.broker_client.save_player_session(_session(fake_context, voice_channel, [request]))

    await cog.resume_player_sessions()

    assert fake_context['guild'].id in cog.players
    # The request was replayed onto the download queue
    assert await cog.download_client.queue_size(fake_context['guild'].id) == 1
    # Consumed exactly once — a failed resume must not retry against staler state
    assert await cog.broker_client.list_player_sessions() == []


@pytest.mark.asyncio
async def test_resume_mints_fresh_requests(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''Replay uses a new MediaRequest rather than the stored one, whose lifecycle
    stage is terminal and whose uuid may still have a broker entry.'''
    cog, voice_channel = _resumable_cog(fake_context, mocker, [_FakeMember()])
    request = fake_source_dict(fake_context)
    await cog.broker_client.save_player_session(_session(fake_context, voice_channel, [request]))

    await cog.resume_player_sessions()

    queued = await cog.download_client.local_worker.get_input_nowait()
    assert str(queued.uuid) != str(request.uuid)
    assert queued.search_result.raw_search_string == request.search_result.raw_search_string
    assert queued.bundle_uuid is None


@pytest.mark.asyncio
async def test_resume_skipped_when_not_playing(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''A session saved while idle is dropped, not resumed.'''
    cog, voice_channel = _resumable_cog(fake_context, mocker, [_FakeMember()])
    await cog.broker_client.save_player_session(
        _session(fake_context, voice_channel, [fake_source_dict(fake_context)], was_playing=False))

    await cog.resume_player_sessions()

    assert fake_context['guild'].id not in cog.players
    assert await cog.broker_client.list_player_sessions() == []


@pytest.mark.asyncio
async def test_resume_skipped_when_channel_has_no_humans(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''Everyone leaving while the bot was down is the clearest signal nobody is
    waiting on the queue — don't play to an empty room.'''
    cog, voice_channel = _resumable_cog(fake_context, mocker, [_FakeMember(is_bot=True)])
    await cog.broker_client.save_player_session(
        _session(fake_context, voice_channel, [fake_source_dict(fake_context)]))

    await cog.resume_player_sessions()

    assert fake_context['guild'].id not in cog.players
    assert await cog.broker_client.list_player_sessions() == []


@pytest.mark.asyncio
async def test_resume_skipped_when_guild_gone(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''The bot may have been removed from the guild while it was down.'''
    cog, voice_channel = _resumable_cog(fake_context, mocker, [_FakeMember()])
    fake_context['bot'].guilds = []
    await cog.broker_client.save_player_session(
        _session(fake_context, voice_channel, [fake_source_dict(fake_context)]))

    await cog.resume_player_sessions()

    assert fake_context['guild'].id not in cog.players
    assert await cog.broker_client.list_player_sessions() == []


@pytest.mark.asyncio
async def test_resume_skipped_when_channel_deleted(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''The voice channel may have been deleted while the bot was down.'''
    cog, voice_channel = _resumable_cog(fake_context, mocker, [_FakeMember()])
    fake_context['guild'].channels = [fake_context['channel']]
    await cog.broker_client.save_player_session(
        _session(fake_context, voice_channel, [fake_source_dict(fake_context)]))

    await cog.resume_player_sessions()

    assert fake_context['guild'].id not in cog.players


@pytest.mark.asyncio
async def test_resume_skipped_when_no_playable_requests(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''An empty queue leaves nothing to play, so the player is not rebuilt.'''
    cog, voice_channel = _resumable_cog(fake_context, mocker, [_FakeMember()])
    await cog.broker_client.save_player_session(_session(fake_context, voice_channel, []))

    await cog.resume_player_sessions()

    assert fake_context['guild'].id not in cog.players


@pytest.mark.asyncio
async def test_resume_survives_one_bad_session(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''One guild's failure must not stop the others from resuming.'''
    cog, voice_channel = _resumable_cog(fake_context, mocker, [_FakeMember()])
    other_guild = FakeGuild()
    await cog.broker_client.save_player_session(PlayerSession(
        guild_id=other_guild.id, voice_channel_id=1, text_channel_id=2,
        queue=[fake_source_dict(fake_context)], was_playing=True,
    ))
    await cog.broker_client.save_player_session(
        _session(fake_context, voice_channel, [fake_source_dict(fake_context)]))
    # The unknown guild raises inside the per-session handler
    mocker.patch.object(cog.bot, 'get_guild', side_effect=[RuntimeError('boom'),
                                                           fake_context['guild']])

    await cog.resume_player_sessions()

    assert fake_context['guild'].id in cog.players


@pytest.mark.asyncio
async def test_resume_with_no_sessions_is_a_noop(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''The ordinary cold start: nothing stored, nothing to do.'''
    cog, _ = _resumable_cog(fake_context, mocker, [_FakeMember()])
    await cog.resume_player_sessions()
    assert not cog.players


@pytest.mark.asyncio
async def test_resume_stops_when_player_cannot_be_built(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''get_player returns None when the voice join fails; nothing is re-queued.'''
    cog, voice_channel = _resumable_cog(fake_context, mocker, [_FakeMember()])
    await cog.broker_client.save_player_session(
        _session(fake_context, voice_channel, [fake_source_dict(fake_context)]))
    mocker.patch.object(cog, 'get_player', return_value=None)

    await cog.resume_player_sessions()

    assert await cog.download_client.queue_size(fake_context['guild'].id) == 0


@pytest.mark.asyncio
async def test_resume_cache_hit_skips_the_download_queue(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''A request whose media is still cached goes straight to the player — the
    whole point of replaying through the ordinary enqueue path.'''
    cog, voice_channel = _resumable_cog(fake_context, mocker, [_FakeMember()])
    await cog.broker_client.save_player_session(
        _session(fake_context, voice_channel, [fake_source_dict(fake_context)]))
    mocker.patch.object(cog, '_enqueue_media_download_from_cache', return_value=True)

    await cog.resume_player_sessions()

    assert await cog.download_client.queue_size(fake_context['guild'].id) == 0


@pytest.mark.asyncio
async def test_resume_handles_queue_refusal(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''A full or blocked download queue discards that request instead of
    aborting the whole resume.'''
    cog, voice_channel = _resumable_cog(fake_context, mocker, [_FakeMember()])
    await cog.broker_client.save_player_session(
        _session(fake_context, voice_channel, [fake_source_dict(fake_context)]))
    mocker.patch.object(cog.download_client, 'submit', side_effect=QueueFull())

    await cog.resume_player_sessions()

    # The player still came up; only the un-queueable request was dropped
    assert fake_context['guild'].id in cog.players
    assert await cog.download_client.queue_size(fake_context['guild'].id) == 0
