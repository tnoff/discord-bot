import asyncio
from logging import getLogger
from unittest.mock import AsyncMock

import pytest

from discord_core.exceptions import ExitEarlyException
from discord_core.utils.loop_health import LOOP_HEALTH
from discord_core.utils.otel import loop_heartbeat_observations

from discord_commands.workers.result_loops import (LOOP_PROCESS_DOWNLOAD_RESULTS, LOOP_PROCESS_SEARCH_RESULTS,
                                                   ResultLoops, _StopEventBot)  #pylint:disable=protected-access


def make_loops(**kwargs):
    broker = AsyncMock()
    results = AsyncMock()
    loops = ResultLoops(broker, results, getLogger('test'), poll_interval=0, **kwargs)
    loops.stop_event = asyncio.Event()
    return loops, broker, results


@pytest.mark.asyncio()
async def test_stop_event_bot_is_ready_and_closed_with_the_event():
    event = asyncio.Event()
    bot = _StopEventBot(event)
    await bot.wait_until_ready()
    assert not bot.is_closed()
    event.set()
    assert bot.is_closed()


@pytest.mark.asyncio()
async def test_search_result_goes_to_the_results_loop():
    loops, broker, results = make_loops()
    broker.next_search_result.return_value = 'resolution'
    await loops.process_search_results()
    results.process_search_result.assert_awaited_once_with('resolution')


@pytest.mark.asyncio()
async def test_download_result_goes_to_the_results_loop():
    loops, broker, results = make_loops()
    broker.next_result.return_value = 'result'
    await loops.process_download_results()
    results.process_download_result.assert_awaited_once_with('result')


@pytest.mark.asyncio()
async def test_idle_poll_sleeps_and_handles_nothing(mocker):
    sleep = mocker.patch('discord_commands.workers.result_loops.sleep', new_callable=AsyncMock)
    loops, broker, results = make_loops()
    loops.poll_interval = 1.5
    broker.next_search_result.return_value = None
    broker.next_result.return_value = None
    await loops.process_search_results()
    await loops.process_download_results()
    assert [call.args for call in sleep.await_args_list] == [(1.5,), (1.5,)]
    results.process_search_result.assert_not_awaited()
    results.process_download_result.assert_not_awaited()


@pytest.mark.asyncio()
@pytest.mark.parametrize('method,pop', [('process_search_results', 'next_search_result'),
                                        ('process_download_results', 'next_result')])
async def test_loops_exit_before_popping_once_stopped(method, pop):
    loops, broker, _ = make_loops()
    loops.stop_event.set()
    with pytest.raises(ExitEarlyException):
        await getattr(loops, method)()
    getattr(broker, pop).assert_not_awaited()


@pytest.mark.asyncio()
async def test_start_runs_both_loops_until_stopped():
    loops, broker, results = make_loops()
    broker.next_search_result.side_effect = ['resolution', None]
    broker.next_result.side_effect = ['result', None]
    stop_event = asyncio.Event()
    loops.start(stop_event)
    assert len(loops.tasks) == 2
    for _ in range(50):
        if results.process_search_result.await_count and results.process_download_result.await_count:
            break
        await asyncio.sleep(0)
    stop_event.set()
    await loops.stop()
    results.process_search_result.assert_awaited_once_with('resolution')
    results.process_download_result.assert_awaited_once_with('result')
    assert not loops.tasks
    assert LOOP_HEALTH.get(LOOP_PROCESS_DOWNLOAD_RESULTS).status == 'stopped'
    assert LOOP_HEALTH.get(LOOP_PROCESS_SEARCH_RESULTS).status == 'stopped'


@pytest.mark.asyncio()
async def test_start_publishes_a_heartbeat_per_loop():
    loops, broker, _ = make_loops()
    broker.next_search_result.return_value = None
    broker.next_result.return_value = None
    stop_event = asyncio.Event()
    loops.start(stop_event)
    for _ in range(50):
        await asyncio.sleep(0)
    for name in (LOOP_PROCESS_DOWNLOAD_RESULTS, LOOP_PROCESS_SEARCH_RESULTS):
        observations = loop_heartbeat_observations(name)
        assert observations[0].attributes == {'background_job': name}
        assert observations[0].value == 1
    stop_event.set()
    await loops.stop()


@pytest.mark.asyncio()
async def test_stop_cancels_a_loop_that_outlasts_the_grace():
    loops, broker, results = make_loops(grace=0.01)
    broker.next_search_result.return_value = 'resolution'
    broker.next_result.return_value = None
    release = asyncio.Event()

    async def hang(_resolution):
        await release.wait()
    results.process_search_result.side_effect = hang
    stop_event = asyncio.Event()
    loops.start(stop_event)
    for _ in range(50):
        if results.process_search_result.await_count:
            break
        await asyncio.sleep(0)
    stop_event.set()
    await loops.stop()
    assert not loops.tasks


@pytest.mark.asyncio()
async def test_stop_before_start_does_nothing():
    loops, _, _ = make_loops()
    await loops.stop()
    assert not loops.tasks
