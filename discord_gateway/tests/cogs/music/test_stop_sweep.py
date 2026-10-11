'''
A restart can stop the player at any instant, so this stops it at every point on the broker
path of a track boundary and checks what the next gateway finds.

The player talks to the broker in a handful of calls, and the broker does each in a few steps.
Every one of those steps, and the gap on either side of each call, is a point the stop can land
on: before the step runs (the broker never saw it), or after (the broker did it and the player
never heard back). For each point the test cancels the player the way a shutdown does, brings up
the next gateway, and plays out what it would play.

Nothing may be lost, a finished track may not come back, and what remains has to be in the order
it was queued.
'''
import asyncio
from contextlib import AsyncExitStack
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from discord_core.utils.common import return_loop_runner

from discord_gateway.cogs.music_helpers import music_player
from discord_gateway.types.cleanup_reason import CleanupReason

from discord_gateway.tests.cogs.music.test_resume_sessions import (
    _FakeMember, _StopsOnDisconnect, _lapse_heartbeat, _queued_track, _resumable_cog)
from tests.helpers import (attach_in_process_broker, attach_in_process_download,  #pylint:disable=unused-import
                           attach_in_process_search, FakeChannel, FakeGuild, FakeVoiceClient, fake_engine,
                           fake_context, fake_media_download)

# Where a stop can land, per call the player makes. 'client' is the player's call, 'registry'
# and 'engine' are the broker's own steps inside it.
CLIENT_CALLS = ('claim_next_track', 'finish_track', 'playing_heartbeat')
REGISTRY_STEPS = ('queue_claim_next', 'drop_claim', 'confirm_claim', 'finish_track', 'playing_heartbeat')
ENGINE_STEPS = ('checkout', 'release')

# Each scenario is a stretch of the player's life; the stop lands only inside it, and this is how
# many points it has. The dry-run test checks the count, so a step added to the broker's claim or
# finish is a failure here until the sweep is told about it.
POINTS = {
    'first_claim': 8,      # the first track being claimed
    'playing': 4,          # one heartbeat while it plays
    'track_boundary': 14,  # it ends: finish, then claim the next
    'skip_boundary': 14,   # it is skipped: the same, released without a history entry
}
SWEEP = [pytest.param(scenario, point, id=f'{scenario}-{point}')
         for scenario, points in POINTS.items() for point in range(points)]


class _Sweep:
    '''Counts the steps inside the armed stretch and fires `trigger` at the chosen one.'''

    def __init__(self, target, trigger, last=None):
        self.target = target
        self.trigger = trigger
        # The point that ends the stretch: nothing after it is counted
        self.last = last
        self.armed = False
        self.log: list[str] = []
        self.fired = False

    async def point(self, label):
        '''Called at every point; stops the player here if this is the chosen one.'''
        if not self.armed:
            return
        index = len(self.log)
        self.log.append(label)
        if label == self.last:
            self.armed = False
        if index == self.target:
            self.fired = True
            await self.trigger()


def _wrap(obj, name, kind, sweep):
    original = getattr(obj, name)

    async def wrapper(*args, **kwargs):
        await sweep.point(f'{kind}.{name} (before)')
        result = await original(*args, **kwargs)
        await sweep.point(f'{kind}.{name} (after)')
        return result

    setattr(obj, name, wrapper)


def _watch_broker(client, sweep):
    '''Report every call the player makes, and every step the broker takes for it, to the sweep.'''
    for name in CLIENT_CALLS:
        _wrap(client, name, 'client', sweep)
    for name in REGISTRY_STEPS:
        _wrap(client.guild_queue._queues, name, 'registry', sweep)  #pylint:disable=protected-access
    for name in ENGINE_STEPS:
        _wrap(client.guild_queue._broker, name, 'engine', sweep)  #pylint:disable=protected-access


async def _until(predicate, tries=400):
    for _ in range(tries):
        if predicate():
            return True
        await asyncio.sleep(0.005)
    return False


async def _drain(client, guild_id):
    '''Everything the next gateway's player would go on to play, in order.'''
    order = []
    while True:
        claimed = await client.claim_next_track(guild_id, 'gw-next')
        if claimed is None:
            return order
        uuid = str(claimed.download.media_request.uuid)
        order.append(uuid)
        await client.finish_track(guild_id, uuid, True, 10)


async def _run(mocker, fake_context, scenario, target):  #pylint:disable=redefined-outer-name
    '''
    Play three queued tracks up to `scenario`, stop the player at point `target` of it (None just
    records the points), and bring up the next gateway.

    Returns (points the scenario has, whether the stop fired, queued uuids, history uuids,
    uuids the next gateway plays in order).
    '''
    cog, voice_channel = _resumable_cog(fake_context, mocker, [_FakeMember()])
    guild = fake_context['guild']
    guild_id = guild.id
    mocker.patch.object(music_player, 'HEARTBEAT_INTERVAL_SECONDS', 0.01 if scenario == 'playing' else 3600.0)
    player = await cog.get_player(guild_id, ctx=fake_context['context'])
    voice_client = _StopsOnDisconnect(guild=guild)
    voice_client.channel = voice_channel
    voice_client.player = player
    guild.voice_client = voice_client
    plays = []
    original_play = voice_client.play

    def counted_play(*args, **kwargs):
        plays.append(player.current_media_download)
        return original_play(*args, **kwargs)

    voice_client.play = counted_play
    bot = SimpleNamespace(wait_until_ready=AsyncMock(), is_closed=lambda: False)

    cleanup_task = []

    async def trigger():
        cleanup_task.append(asyncio.create_task(cog.cleanup(guild, reason=CleanupReason.BOT_SHUTDOWN)))
        # Stay where the stop landed until the cleanup cancels this task
        await asyncio.Event().wait()

    sweep = _Sweep(target, trigger, last='client.playing_heartbeat (after)' if scenario == 'playing' else None)
    client = cog.broker_client
    _watch_broker(client, sweep)

    async with AsyncExitStack() as stack:
        queued = []
        for _ in range(3):
            queued.append(await stack.enter_async_context(_queued_track(cog, fake_context)))
        uuids = [str(item.media_request.uuid) for item in queued]

        if scenario == 'first_claim':
            sweep.armed = True
        player._player_task = asyncio.create_task(  #pylint:disable=protected-access
            return_loop_runner(player.player_loop, bot, player.logger, None)())
        if scenario == 'first_claim':
            await _until(lambda: sweep.fired or len(plays) == 1)
        else:
            assert await _until(lambda: len(plays) == 1)
            sweep.armed = True
            if scenario == 'playing':
                await _until(lambda: sweep.fired or not sweep.armed)
            else:
                player.video_skipped = scenario == 'skip_boundary'
                voice_client.after()
                await _until(lambda: sweep.fired or len(plays) == 2)
        # Let whatever trails the last step settle
        await asyncio.sleep(0.05)
        points = list(sweep.log)

        if cleanup_task:
            await cleanup_task[0]
        else:
            await cog.cleanup(guild, reason=CleanupReason.BOT_SHUTDOWN)

        await _lapse_heartbeat(cog, guild_id)
        await cog.resume_player_sessions()
        history = [item['uuid'] for item in await client.get_guild_history(guild_id)]
        played = await _drain(client, guild_id)
    return points, sweep.fired, uuids, history, played


def _problems(scenario, uuids, history, played):
    '''What the next gateway got wrong, as a list of sentences. Empty means nothing.'''
    problems = []
    first = uuids[0]
    # A skipped track is meant to be gone, from history and from the queue alike
    for uuid in uuids[1:] if scenario == 'skip_boundary' else uuids:
        if uuid not in history + played:
            problems.append(f'{uuid[:6]} was lost')
    if set(history) & set(played):
        problems.append('a finished track would play again')
    if len(played) != len(set(played)):
        problems.append('a track would play twice')
    if played != sorted(played, key=uuids.index):
        problems.append('the order changed')
    if scenario in ('first_claim', 'playing'):
        # The track was in the middle of playing: it never finished, and has to come back first
        if history:
            problems.append('a track that never finished is in the history')
        if played != uuids:
            problems.append('the interrupted track did not come back first')
    elif scenario == 'skip_boundary':
        if history:
            problems.append('a skipped track is in the history')
    elif history not in ([], [first]):
        problems.append('a track that never finished is in the history')
    return problems


def _replays(scenario, uuids, history, played):
    '''Whether the first track, which was over (or skipped) when the player stopped, plays again.'''
    return scenario in ('track_boundary', 'skip_boundary') and not history and played[:1] == uuids[:1]


@pytest.mark.asyncio
@pytest.mark.parametrize('scenario', POINTS)
async def test_the_sweep_covers_every_step_of_the_scenario(mocker, fake_context, scenario):  #pylint:disable=redefined-outer-name
    '''The run with no stop: the points are the ones the sweep below stops at, and it ends well.'''
    points, fired, uuids, history, played = await _run(mocker, fake_context, scenario, None)

    assert not fired
    assert len(points) == POINTS[scenario], points
    assert not _problems(scenario, uuids, history, played)


@pytest.mark.asyncio
@pytest.mark.parametrize('scenario,point', SWEEP)
async def test_stopping_the_player_at_any_point_loses_nothing(mocker, fake_context, scenario, point):  #pylint:disable=redefined-outer-name
    '''
    Stop at one point of a track's life and bring up the next gateway: whatever was playing comes
    back first, whatever had finished stays finished, nothing is lost or reordered.

    The one thing allowed to repeat is the track that was over when the stop landed but whose
    ending the broker never heard (the stop fell before the finish reached it): to the broker
    it is still playing, so the next gateway plays it again.
    '''
    points, fired, uuids, history, played = await _run(mocker, fake_context, scenario, point)

    assert fired, f'{scenario} has no point {point}: {points}'
    problems = _problems(scenario, uuids, history, played)
    assert not problems, (points[point], problems)
    if _replays(scenario, uuids, history, played):
        # Only before the broker has recorded the finish
        assert 'finish_track (before)' in points[point]
