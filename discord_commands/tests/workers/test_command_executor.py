import asyncio
from logging import getLogger

import pytest

from discord_core.types.command_args import NoArgs
from discord_core.types.command_envelope import CommandEnvelope

from discord_commands.workers.command_executor import CommandExecutor


def envelope(guild_id=1):
    return CommandEnvelope(guild_id=guild_id, channel_id=2, author_id=3, author_display_name='tyler')


async def settle(executor):
    '''Let every guild's worker run to the end.'''
    while executor._workers:  # pylint: disable=protected-access
        await asyncio.gather(*executor._workers.values())  # pylint: disable=protected-access


class Recorder:
    '''A handler that records what it ran, and can be held open.'''

    def __init__(self):
        self.ran = []
        self.release = {}

    def handler(self, name):
        async def run(env, args):
            self.ran.append((name, env.guild_id, args))
            gate = self.release.get(name)
            if gate is not None:
                await gate.wait()
        return run


@pytest.mark.asyncio
async def test_serves_only_the_commands_it_has_handlers_for():
    executor = CommandExecutor({'skip': Recorder().handler('skip')})
    assert executor.serves('skip')
    assert not executor.serves('play')


@pytest.mark.asyncio
async def test_a_submitted_command_runs_with_its_envelope_and_args():
    recorder = Recorder()
    executor = CommandExecutor({'skip': recorder.handler('skip')})
    args = NoArgs()
    assert executor.submit('skip', envelope(), args, 'r-1') is False
    await settle(executor)
    assert recorder.ran == [('skip', 1, args)]


@pytest.mark.asyncio
async def test_a_guilds_commands_run_in_the_order_they_were_accepted():
    recorder = Recorder()
    recorder.release['first'] = asyncio.Event()
    executor = CommandExecutor({'first': recorder.handler('first'), 'second': recorder.handler('second')})
    executor.submit('first', envelope(), NoArgs(), 'r-1')
    executor.submit('second', envelope(), NoArgs(), 'r-2')
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    # The second waits behind the first, which is still open.
    assert [name for name, _, _ in recorder.ran] == ['first']
    recorder.release['first'].set()
    await settle(executor)
    assert [name for name, _, _ in recorder.ran] == ['first', 'second']


@pytest.mark.asyncio
async def test_a_slow_command_in_one_guild_does_not_hold_up_another():
    recorder = Recorder()
    recorder.release['slow'] = asyncio.Event()
    executor = CommandExecutor({'slow': recorder.handler('slow'), 'fast': recorder.handler('fast')})
    executor.submit('slow', envelope(1), NoArgs(), 'r-1')
    executor.submit('fast', envelope(2), NoArgs(), 'r-2')
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert [(name, guild) for name, guild, _ in recorder.ran] == [('slow', 1), ('fast', 2)]
    recorder.release['slow'].set()
    await settle(executor)


@pytest.mark.asyncio
async def test_a_replayed_request_id_is_a_duplicate_and_runs_once():
    recorder = Recorder()
    executor = CommandExecutor({'skip': recorder.handler('skip')})
    assert executor.submit('skip', envelope(), NoArgs(), 'r-1') is False
    assert executor.submit('skip', envelope(), NoArgs(), 'r-1') is True
    await settle(executor)
    assert len(recorder.ran) == 1


@pytest.mark.asyncio
async def test_a_request_id_is_forgotten_after_the_ttl():
    now = [100.0]
    recorder = Recorder()
    executor = CommandExecutor({'skip': recorder.handler('skip')}, dedup_ttl=10.0, time_func=lambda: now[0])
    assert executor.submit('skip', envelope(), NoArgs(), 'r-1') is False
    now[0] = 105.0
    assert executor.submit('skip', envelope(), NoArgs(), 'r-1') is True
    now[0] = 111.0
    assert executor.submit('skip', envelope(), NoArgs(), 'r-1') is False
    await settle(executor)
    assert len(recorder.ran) == 2


@pytest.mark.asyncio
async def test_the_oldest_request_id_is_forgotten_past_the_cap():
    executor = CommandExecutor({'skip': Recorder().handler('skip')}, dedup_max_ids=2)
    for index in range(3):
        executor.submit('skip', envelope(), NoArgs(), f'r-{index}')
    # r-0 was pushed out by the cap, r-2 is still remembered.
    assert executor.submit('skip', envelope(), NoArgs(), 'r-2') is True
    assert executor.submit('skip', envelope(), NoArgs(), 'r-0') is False
    await settle(executor)


@pytest.mark.asyncio
async def test_a_command_that_raises_is_logged_and_the_next_one_still_runs(caplog):
    recorder = Recorder()

    async def boom(_env, _args):
        raise RuntimeError('boom')

    executor = CommandExecutor({'boom': boom, 'skip': recorder.handler('skip')}, logger=getLogger('test'))
    executor.submit('boom', envelope(), NoArgs(), 'r-1')
    executor.submit('skip', envelope(), NoArgs(), 'r-2')
    with caplog.at_level('ERROR'):
        await settle(executor)
    assert 'Command "boom" failed in guild 1' in caplog.text
    assert [name for name, _, _ in recorder.ran] == ['skip']


@pytest.mark.asyncio
async def test_close_cancels_unfinished_work_and_drops_what_was_queued():
    recorder = Recorder()
    recorder.release['hang'] = asyncio.Event()
    executor = CommandExecutor({'hang': recorder.handler('hang'), 'skip': recorder.handler('skip')})
    executor.submit('hang', envelope(), NoArgs(), 'r-1')
    executor.submit('skip', envelope(), NoArgs(), 'r-2')
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    await executor.close()
    assert [name for name, _, _ in recorder.ran] == ['hang']
    assert not executor._workers and not executor._pending  # pylint: disable=protected-access
