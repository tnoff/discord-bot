'''Tests for resume-after-restart: saving where a guild's player is on shutdown and
picking its queue back up on the next startup.  The queue itself lives in the broker and
survives the restart; the session only says which channels to rejoin.'''
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock

import pytest

from discord_core.types.player_session import PlayerSession

from discord_broker.workers.guild_queue_registry import GPLAYING_KEY_PREFIX

from discord_gateway.cogs import music as music_module
from discord_gateway.cogs.music import Music
from discord_gateway.cogs.music_helpers.music_player import MusicPlayer
from discord_gateway.types.cleanup_reason import CleanupReason

from discord_gateway.tests.cogs.test_music import BASE_MUSIC_CONFIG
from tests.helpers import (attach_in_process_broker, attach_in_process_download,  #pylint:disable=unused-import
                           attach_in_process_search, FakeChannel,
                           FakeGuild, FakeVoiceClient, fake_engine, fake_context,
                           fake_media_download)


class _StopsOnDisconnect(FakeVoiceClient):
    '''
    A voice client that behaves like the real one on disconnect: it stops what is playing,
    which fires the callback play() was given, the same one a track ending fires.
    '''
    def __init__(self, guild=None):
        super().__init__(guild=guild)
        self.after = None

    def play(self, *_args, after=None, **_kwargs):
        self.after = after
        return True

    async def disconnect(self):
        if self.after:
            self.after()
        return True


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


async def _queue_track(cog, player, media_download):  #pylint:disable=redefined-outer-name
    '''Put a downloaded track in the guild's queue in the broker, as !play does.'''
    await cog.broker_client.register_download(media_download)
    result = await cog.broker_client.enqueue_track(player.guild.id, str(media_download.media_request.uuid), 10)
    assert result == 'ok'


async def _queued_uuids(cog, guild_id):
    return [str(item.media_request.uuid) for item in (await cog.broker_client.get_guild_queue(guild_id)).items]


# ---------------------------------------------------------------------------
# Saving a session on shutdown
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_shutdown_saves_the_channels_and_leaves_the_queue_in_the_broker(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''BOT_SHUTDOWN records where the player is; the queue stays put for the next gateway.'''
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    voice_channel = _voice_channel([_FakeMember()])
    player = await _player_in_voice(cog, fake_context, mocker, voice_channel)

    with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
        await _queue_track(cog, player, media_download)

        await cog.cleanup(fake_context['guild'], reason=CleanupReason.BOT_SHUTDOWN)

        sessions = await cog.broker_client.list_player_sessions()
        assert len(sessions) == 1
        assert sessions[0].guild_id == fake_context['guild'].id
        assert sessions[0].voice_channel_id == voice_channel.id
        assert sessions[0].text_channel_id == fake_context['channel'].id
        assert sessions[0].queue == []
        assert await _queued_uuids(cog, fake_context['guild'].id) == [str(media_download.media_request.uuid)]


@pytest.mark.asyncio
async def test_shutdown_records_was_playing_false_when_idle(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''A player parked in voice with nothing playing is saved as not-playing, so
    the resume guard skips it rather than rejoining an idle channel.'''
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    await _player_in_voice(cog, fake_context, mocker, _voice_channel([_FakeMember()]))

    await cog.cleanup(fake_context['guild'], reason=CleanupReason.BOT_SHUTDOWN)

    sessions = await cog.broker_client.list_player_sessions()
    assert sessions[0].was_playing is False


@pytest.mark.asyncio
async def test_shutdown_records_was_playing_true_mid_track(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''A player mid-track is saved as playing.'''
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    player = await _player_in_voice(cog, fake_context, mocker, _voice_channel([_FakeMember()]))

    with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
        player.current_media_download = media_download

        await cog.cleanup(fake_context['guild'], reason=CleanupReason.BOT_SHUTDOWN)

    sessions = await cog.broker_client.list_player_sessions()
    assert sessions[0].was_playing is True


@pytest.mark.asyncio
async def test_joining_voice_saves_a_session_for_a_crash_to_find(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''A crash never reaches BOT_SHUTDOWN, so the session is written as the player joins voice'''
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    voice_channel = _voice_channel([_FakeMember()])
    player = await _player_in_voice(cog, fake_context, mocker, voice_channel)

    await cog._save_player_session(fake_context['guild'], player)  #pylint:disable=protected-access

    [session] = await cog.broker_client.list_player_sessions()
    assert session.voice_channel_id == voice_channel.id
    assert session.text_channel_id == fake_context['channel'].id


@pytest.mark.asyncio
async def test_shutdown_without_voice_client_saves_nothing(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''No voice channel means nothing to rejoin, so no session is written.'''
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
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
    attach_in_process_broker(cog)
    attach_in_process_download(cog)
    attach_in_process_search(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    await _player_in_voice(cog, fake_context, mocker, _voice_channel([_FakeMember()]))

    await cog.cleanup(fake_context['guild'], reason=CleanupReason.VOICE_INACTIVE)

    assert await cog.broker_client.list_player_sessions() == []


@pytest.mark.asyncio
async def test_non_shutdown_cleanup_closes_the_guilds_queue(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''A guild that is genuinely done takes its queue with it, so it cannot come back on the next !play'''
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    attach_in_process_download(cog)
    attach_in_process_search(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    player = await _player_in_voice(cog, fake_context, mocker, _voice_channel([_FakeMember()]))

    with fake_media_download(player.file_dir, fake_context=fake_context) as media_download:
        await _queue_track(cog, player, media_download)

        await cog.cleanup(fake_context['guild'], reason=CleanupReason.VOICE_INACTIVE)

        queue = await cog.broker_client.get_guild_queue(fake_context['guild'].id)
        assert queue.items == []
        assert queue.closed is True


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
    attach_in_process_broker(cog)
    attach_in_process_search(cog)
    attach_in_process_download(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    return cog, voice_channel


def _session(fake_context, voice_channel, was_playing=True) -> PlayerSession:  #pylint:disable=redefined-outer-name
    return PlayerSession(
        guild_id=fake_context['guild'].id,
        voice_channel_id=voice_channel.id,
        text_channel_id=fake_context['channel'].id,
        was_playing=was_playing,
    )


@asynccontextmanager
async def _queued_track(cog, fake_context, guild_id=None):  #pylint:disable=redefined-outer-name
    '''A downloaded track sitting in the broker's queue, as a previous gateway left it.'''
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(Path(tmp_dir), fake_context=fake_context) as media_download:
            await cog.broker_client.register_download(media_download)
            result = await cog.broker_client.enqueue_track(
                guild_id or fake_context['guild'].id, str(media_download.media_request.uuid), 10)
            assert result == 'ok'
            yield media_download


async def _lapse_heartbeat(cog, guild_id):
    '''Let the playing track's heartbeat run out, as it does a few seconds after a gateway dies.'''
    registry = cog.broker_client.guild_queue._queues  #pylint:disable=protected-access
    await registry._client.delete(f'{GPLAYING_KEY_PREFIX}{guild_id}')  #pylint:disable=protected-access


@pytest.mark.asyncio
async def test_resume_rejoins_and_keeps_the_queue(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''A live session rebuilds the player and picks the broker's queue back up, untouched.'''
    cog, voice_channel = _resumable_cog(fake_context, mocker, [_FakeMember()])
    async with _queued_track(cog, fake_context) as media_download:
        await cog.broker_client.save_player_session(_session(fake_context, voice_channel))

        await cog.resume_player_sessions()

        assert fake_context['guild'].id in cog.players
        # Not replayed or re-requested: it is the same queue entry, and nothing went to the downloader
        assert await _queued_uuids(cog, fake_context['guild'].id) == [str(media_download.media_request.uuid)]
        assert await cog.download_client.queue_size(fake_context['guild'].id) == 0
        # Consumed exactly once -- a failed resume must not retry against staler state
        assert await cog.broker_client.list_player_sessions() == []


@pytest.mark.asyncio
async def test_resume_points_the_queue_at_the_sessions_text_channel(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''The play-order message follows the channel the player was last using'''
    cog, voice_channel = _resumable_cog(fake_context, mocker, [_FakeMember()])
    async with _queued_track(cog, fake_context):
        await cog.broker_client.save_player_session(_session(fake_context, voice_channel))

        await cog.resume_player_sessions()

        queue = await cog.broker_client.get_guild_queue(fake_context['guild'].id)
        assert queue.text_channel_id == fake_context['channel'].id
        assert queue.closed is False


@pytest.mark.asyncio
async def test_resume_tells_the_channel_how_many_are_still_queued(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog, voice_channel = _resumable_cog(fake_context, mocker, [_FakeMember()])
    async with _queued_track(cog, fake_context):
        await cog.broker_client.save_player_session(_session(fake_context, voice_channel))

        await cog.resume_player_sessions()

        message = cog.dispatcher.send_message.call_args.args[2]
        assert 'Resumed after a restart' in message
        assert '1 item(s)' in message


@pytest.mark.asyncio
async def test_resume_gets_back_the_track_the_old_gateway_was_playing(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''A gateway that died mid-track leaves it claimed; once its heartbeat lapses the resume
    takes it back at the head of the queue, and counts it among what is waiting.'''
    cog, voice_channel = _resumable_cog(fake_context, mocker, [_FakeMember()])
    guild_id = fake_context['guild'].id
    async with _queued_track(cog, fake_context) as interrupted:
        async with _queued_track(cog, fake_context) as waiting:
            assert await cog.broker_client.claim_next_track(guild_id, 'gw-old') is not None
            await _lapse_heartbeat(cog, guild_id)
            await cog.broker_client.save_player_session(_session(fake_context, voice_channel))

            await cog.resume_player_sessions()

            assert await _queued_uuids(cog, guild_id) == [
                str(interrupted.media_request.uuid), str(waiting.media_request.uuid)]
            assert '2 item(s)' in cog.dispatcher.send_message.call_args.args[2]


@pytest.mark.asyncio
async def test_resume_waits_for_the_old_gateway_to_stop_playing(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''A replacement that starts inside the heartbeat window would be refused the interrupted
    track, so it waits for the old record to lapse.'''
    cog, voice_channel = _resumable_cog(fake_context, mocker, [_FakeMember()])
    guild_id = fake_context['guild'].id
    async with _queued_track(cog, fake_context) as interrupted:
        assert await cog.broker_client.claim_next_track(guild_id, 'gw-old') is not None
        await cog.broker_client.save_player_session(_session(fake_context, voice_channel))

        async def _old_gateway_stops(_seconds):
            await _lapse_heartbeat(cog, guild_id)

        waited = mocker.patch('discord_gateway.cogs.music.sleep', side_effect=_old_gateway_stops)

        await cog.resume_player_sessions()

        waited.assert_awaited_once_with(1)
        # Having waited, it got the interrupted track back instead of finding it still taken
        assert await _queued_uuids(cog, guild_id) == [str(interrupted.media_request.uuid)]
        assert guild_id in cog.players


@pytest.mark.asyncio
async def test_resume_stops_waiting_for_a_gateway_that_never_stops(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''The wait is bounded: a gateway that keeps heartbeating is not waited on forever'''
    cog, voice_channel = _resumable_cog(fake_context, mocker, [_FakeMember()])
    guild_id = fake_context['guild'].id
    async with _queued_track(cog, fake_context):
        async with _queued_track(cog, fake_context):
            assert await cog.broker_client.claim_next_track(guild_id, 'gw-old') is not None
            await cog.broker_client.save_player_session(_session(fake_context, voice_channel))
            mocker.patch.object(music_module, 'FOREIGN_PLAYER_WAIT_SECONDS', 0.05)
            cog.logger = MagicMock()

            await cog.resume_player_sessions()

            assert any('still appears to be playing' in call.args[0] for call in cog.logger.warning.call_args_list)
            assert guild_id in cog.players


@pytest.mark.asyncio
async def test_resume_does_not_wait_for_its_own_gateway(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''A track this gateway id is playing is not a foreign player'''
    cog, voice_channel = _resumable_cog(fake_context, mocker, [_FakeMember()])
    guild_id = fake_context['guild'].id
    async with _queued_track(cog, fake_context):
        async with _queued_track(cog, fake_context):
            assert await cog.broker_client.claim_next_track(guild_id, cog.gateway_id) is not None
            await cog.broker_client.save_player_session(_session(fake_context, voice_channel))
            sleeper = mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)

            await cog.resume_player_sessions()

            sleeper.assert_not_awaited()
            assert guild_id in cog.players


@pytest.mark.asyncio
async def test_a_restart_leaves_the_interrupted_track_for_the_next_gateway(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''
    Restarting mid-track disconnects voice, which stops playback the way a track ending does.
    The track was not played out: it must not land in the guild's history, and the next
    gateway has to get it back at the head of the queue.
    '''
    cog, voice_channel = _resumable_cog(fake_context, mocker, [_FakeMember()])
    guild_id = fake_context['guild'].id
    player = await cog.get_player(guild_id, ctx=fake_context['context'])
    voice_client = _StopsOnDisconnect(guild=fake_context['guild'])
    voice_client.channel = voice_channel
    fake_context['guild'].voice_client = voice_client
    async with _queued_track(cog, fake_context) as interrupted:
        async with _queued_track(cog, fake_context) as waiting:
            task = asyncio.create_task(player.player_loop())
            for _ in range(200):
                if player.current_media_download is not None:
                    break
                await asyncio.sleep(0.005)
            assert player.current_media_download is not None

            await cog.cleanup(fake_context['guild'], reason=CleanupReason.BOT_SHUTDOWN)
            await asyncio.wait_for(task, timeout=1)

            assert await cog.broker_client.get_guild_history(guild_id) == []

            # The next gateway comes up once the old one's heartbeat has lapsed
            await _lapse_heartbeat(cog, guild_id)
            await cog.resume_player_sessions()

            assert await _queued_uuids(cog, guild_id) == [
                str(interrupted.media_request.uuid), str(waiting.media_request.uuid)]


@pytest.mark.asyncio
async def test_resume_skipped_when_nothing_is_queued(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''A session saved while idle has nothing to play, so it is dropped rather than rejoining an empty queue.'''
    cog, voice_channel = _resumable_cog(fake_context, mocker, [_FakeMember()])
    await cog.broker_client.save_player_session(_session(fake_context, voice_channel, was_playing=False))

    await cog.resume_player_sessions()

    assert fake_context['guild'].id not in cog.players
    assert await cog.broker_client.list_player_sessions() == []
    assert (await cog.broker_client.get_guild_queue(fake_context['guild'].id)).closed is True


@pytest.mark.asyncio
async def test_resume_skipped_when_channel_has_no_humans(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''Everyone leaving while the bot was down is the clearest signal nobody is
    waiting on the queue — don't play to an empty room.'''
    cog, voice_channel = _resumable_cog(fake_context, mocker, [_FakeMember(is_bot=True)])
    async with _queued_track(cog, fake_context):
        await cog.broker_client.save_player_session(_session(fake_context, voice_channel))

        await cog.resume_player_sessions()

        assert fake_context['guild'].id not in cog.players
        assert await cog.broker_client.list_player_sessions() == []
        # Nobody is coming back for it, so the queue goes too rather than coming back on the next !play
        assert (await cog.broker_client.get_guild_queue(fake_context['guild'].id)).closed is True


@pytest.mark.asyncio
async def test_resume_skipped_when_guild_gone(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''The bot may have been removed from the guild while it was down.'''
    cog, voice_channel = _resumable_cog(fake_context, mocker, [_FakeMember()])
    fake_context['bot'].guilds = []
    async with _queued_track(cog, fake_context):
        await cog.broker_client.save_player_session(_session(fake_context, voice_channel))

        await cog.resume_player_sessions()

        assert fake_context['guild'].id not in cog.players
        assert await cog.broker_client.list_player_sessions() == []
        # Nobody is coming back for it, so the queue goes too rather than coming back on the next !play
        assert (await cog.broker_client.get_guild_queue(fake_context['guild'].id)).closed is True


@pytest.mark.asyncio
async def test_resume_skipped_when_channel_deleted(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''The voice channel may have been deleted while the bot was down.'''
    cog, voice_channel = _resumable_cog(fake_context, mocker, [_FakeMember()])
    fake_context['guild'].channels = [fake_context['channel']]
    async with _queued_track(cog, fake_context):
        await cog.broker_client.save_player_session(_session(fake_context, voice_channel))

        await cog.resume_player_sessions()

        assert fake_context['guild'].id not in cog.players
        assert (await cog.broker_client.get_guild_queue(fake_context['guild'].id)).closed is True


@pytest.mark.asyncio
async def test_resume_survives_one_bad_session(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''One guild's failure must not stop the others from resuming.'''
    cog, voice_channel = _resumable_cog(fake_context, mocker, [_FakeMember()])
    other_guild = FakeGuild()
    await cog.broker_client.save_player_session(PlayerSession(
        guild_id=other_guild.id, voice_channel_id=1, text_channel_id=2, was_playing=True,
    ))
    async with _queued_track(cog, fake_context):
        await cog.broker_client.save_player_session(_session(fake_context, voice_channel))
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
    '''get_player returns None when the voice join fails; the queue is not left behind to surprise anyone.'''
    cog, voice_channel = _resumable_cog(fake_context, mocker, [_FakeMember()])
    async with _queued_track(cog, fake_context):
        await cog.broker_client.save_player_session(_session(fake_context, voice_channel))
        mocker.patch.object(cog, 'get_player', return_value=None)

        await cog.resume_player_sessions()

        assert fake_context['guild'].id not in cog.players
        queue = await cog.broker_client.get_guild_queue(fake_context['guild'].id)
        assert queue.closed is True
        assert queue.items == []
