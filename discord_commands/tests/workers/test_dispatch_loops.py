import asyncio
from logging import getLogger
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from discord_core.types.dispatch_request import (
    DeleteRequest, FetchChannelHistoryRequest, FetchGuildEmojisRequest,
)
from discord_core.types.dispatch_result import ChannelHistoryResult, GuildEmojisResult
from discord_core.utils.loop_health import LOOP_HEALTH
from discord_core.utils.otel import loop_heartbeat_observations

from discord_commands.loop_logic.delete_messages import DeleteMessagesLoop
from discord_commands.loop_logic.markov import MarkovLoop
from discord_commands.workers.dispatch_loops import (
    COG_DELETE_MESSAGES, COG_MARKOV, LOOP_DELETE_MESSAGE_CHECK, LOOP_DELETE_MESSAGE_RESULT, LOOP_MARKOV_CHECK,
    LOOP_MARKOV_RESULT, DispatchLoopPair, build_dispatch_loops, delete_requester, emojis_requester,
    history_requester,
)

LOGGER = getLogger('test')
INCLUDE_BOTH = {'general': {'include': {'markov': True, 'delete_messages': True}},
                'delete_messages': {'discord_channels': [{'server_id': 1, 'channel_id': 2, 'delete_after': 3}],
                                    'loop_sleep_interval': 11},
                'markov': {'loop_sleep_interval': 7, 'message_check_limit': 5, 'history_retention_days': 9}}


def make_dispatcher():
    dispatcher = MagicMock()
    dispatcher.register_cog_queue = MagicMock(side_effect=lambda name: asyncio.Queue())
    dispatcher.submit_request = AsyncMock()
    return dispatcher


def make_pair(process=None, request=None, dispatcher=None):
    async def idle():
        await asyncio.sleep(0)
    return DispatchLoopPair(dispatcher or make_dispatcher(), LOGGER, 'cog', 'cog_check', 'cog_result', 1.0,
                            request or idle, process or AsyncMock(), 'Cog')


async def wait_for(condition):
    for _ in range(200):
        if condition():
            return
        await asyncio.sleep(0.005)
    raise AssertionError('condition never held')


@pytest.mark.asyncio()
async def test_start_registers_the_cog_queue_and_both_loops():
    dispatcher = make_dispatcher()
    pair = make_pair(dispatcher=dispatcher)
    pair.start(asyncio.Event())
    await asyncio.sleep(0)
    dispatcher.register_cog_queue.assert_called_once_with('cog')
    assert len(pair.tasks) == 2
    assert LOOP_HEALTH.get('cog_check') is not None
    assert LOOP_HEALTH.get('cog_result') is not None
    for job in ('cog_check', 'cog_result'):
        assert loop_heartbeat_observations(job)[0].attributes == {'background_job': job}
    await pair.stop()


@pytest.mark.asyncio()
async def test_results_reach_the_processor_and_count_as_success():
    process = AsyncMock()
    pair = make_pair(process=process)
    pair.start(asyncio.Event())
    pair.queue.put_nowait('one')
    pair.queue.put_nowait('two')
    await wait_for(lambda: process.await_count == 2)
    assert [call.args for call in process.await_args_list] == [('one',), ('two',)]
    assert LOOP_HEALTH.get('cog_result').consecutive_errors == 0
    await pair.stop()


@pytest.mark.asyncio()
async def test_a_bad_result_is_logged_and_the_next_is_still_taken(caplog):
    handled = []

    async def process(result):
        if result == 'bad':
            raise RuntimeError('boom')
        handled.append(result)
    pair = make_pair(process=process)
    pair.start(asyncio.Event())
    pair.queue.put_nowait('bad')
    pair.queue.put_nowait('good')
    await wait_for(lambda: handled == ['good'])
    assert 'Cog :: error processing dispatch result' in caplog.text
    await pair.stop()


@pytest.mark.asyncio()
async def test_stop_cancels_the_loops_and_marks_them_stopped():
    pair = make_pair()
    pair.start(asyncio.Event())
    await asyncio.sleep(0)
    tasks = list(pair.tasks)
    await pair.stop()
    assert all(task.done() for task in tasks)
    assert not pair.tasks
    assert LOOP_HEALTH.get('cog_check').status == 'stopped'
    assert LOOP_HEALTH.get('cog_result').status == 'stopped'


@pytest.mark.asyncio()
async def test_stop_before_start_does_nothing():
    pair = make_pair()
    await pair.stop()
    assert not pair.tasks


@pytest.mark.asyncio()
async def test_the_request_loop_runs_until_the_pod_stops():
    calls = []

    async def request():
        calls.append(1)
        await asyncio.sleep(0)
    pair = make_pair(request=request)
    stop_event = asyncio.Event()
    pair.start(stop_event)
    await wait_for(lambda: len(calls) >= 2)
    stop_event.set()
    await pair.stop()


def test_queue_depth_reports_the_result_queue_and_zero_before_start():
    pair = make_pair()
    before = pair._queue_depth(None)  # pylint: disable=protected-access
    assert before[0].value == 0
    pair.queue = asyncio.Queue()
    pair.queue.put_nowait('a')
    pair.queue.put_nowait('b')
    observation = pair._queue_depth(None)[0]  # pylint: disable=protected-access
    assert observation.value == 2
    assert observation.attributes == {'background_job': 'cog_result', 'result_type': 'dispatch'}


@pytest.mark.asyncio()
async def test_history_requester_submits_a_history_request_for_the_cog():
    dispatcher = make_dispatcher()
    await history_requester(dispatcher, 'markov')(1, 2, limit=5, after_message_id=9)
    request = dispatcher.submit_request.await_args.args[0]
    assert request == FetchChannelHistoryRequest(guild_id=1, channel_id=2, limit=5, cog_name='markov',
                                                 after_message_id=9, span_context=request.span_context)


@pytest.mark.asyncio()
async def test_history_requester_defaults_match_the_cogs():
    dispatcher = make_dispatcher()
    await history_requester(dispatcher, 'deletemessages')(1, 2)
    request = dispatcher.submit_request.await_args.args[0]
    assert (request.limit, request.after, request.oldest_first) == (100, None, True)


@pytest.mark.asyncio()
async def test_emojis_requester_submits_an_emoji_request_for_the_cog():
    dispatcher = make_dispatcher()
    await emojis_requester(dispatcher, 'markov')(1, max_retries=5)
    request = dispatcher.submit_request.await_args.args[0]
    assert request == FetchGuildEmojisRequest(guild_id=1, cog_name='markov', max_retries=5,
                                              span_context=request.span_context)


@pytest.mark.asyncio()
async def test_delete_requester_submits_a_delete_request():
    dispatcher = make_dispatcher()
    await delete_requester(dispatcher)(1, 2, 3)
    request = dispatcher.submit_request.await_args.args[0]
    assert request == DeleteRequest(guild_id=1, channel_id=2, message_id=3, span_context=request.span_context)


def test_nothing_runs_unless_included():
    assert not build_dispatch_loops({}, make_dispatcher(), MagicMock(), LOGGER)
    assert not build_dispatch_loops({'general': {'include': {'markov': False}}}, make_dispatcher(),
                                    MagicMock(), LOGGER)


def test_only_the_included_loops_are_built():
    markov = build_dispatch_loops({'general': {'include': {'markov': True}}}, make_dispatcher(), MagicMock(), LOGGER)
    assert [(pair.cog_name, pair.check_job, pair.result_job) for pair in markov] == [
        (COG_MARKOV, LOOP_MARKOV_CHECK, LOOP_MARKOV_RESULT)]
    assert markov[0].sleep_interval == 300.0
    deletes = build_dispatch_loops(INCLUDE_BOTH | {'general': {'include': {'delete_messages': True}}},
                                   make_dispatcher(), MagicMock(), LOGGER)
    assert [pair.cog_name for pair in deletes] == [COG_DELETE_MESSAGES]


def test_both_loops_take_their_config():
    loops = build_dispatch_loops(INCLUDE_BOTH, make_dispatcher(), MagicMock(), LOGGER)
    assert len(loops) == 2
    markov, deletes = loops[0], loops[1]
    assert (markov.cog_name, markov.sleep_interval) == (COG_MARKOV, 7)
    assert (deletes.cog_name, deletes.check_job, deletes.result_job, deletes.sleep_interval) == (
        COG_DELETE_MESSAGES, LOOP_DELETE_MESSAGE_CHECK, LOOP_DELETE_MESSAGE_RESULT, 11)
    loop = markov.request.__self__
    assert (loop.loop_sleep_interval, loop.message_check_limit, loop.history_retention_days) == (7, 5, 9)
    delete_loop = deletes.request.__self__
    assert delete_loop.discord_channels == [{'server_id': 1, 'channel_id': 2, 'delete_after': 3}]


def test_delete_messages_needs_its_channels():
    settings = {'general': {'include': {'delete_messages': True}}}
    with pytest.raises(ValidationError):
        build_dispatch_loops(settings, make_dispatcher(), MagicMock(), LOGGER)


@pytest.mark.asyncio()
async def test_markov_results_are_routed_by_type(mocker):
    emojis = mocker.patch.object(MarkovLoop, 'process_emojis_result', new_callable=AsyncMock)
    history = mocker.patch.object(MarkovLoop, 'process_history_result', new_callable=AsyncMock)
    markov = build_dispatch_loops(INCLUDE_BOTH, make_dispatcher(), MagicMock(), LOGGER)[0]
    emoji_result = GuildEmojisResult(guild_id=1, emojis=[])
    history_result = ChannelHistoryResult(guild_id=1, channel_id=2, messages=[])
    for result in (emoji_result, history_result, 'something else'):
        await markov.process(result)
    emojis.assert_awaited_once_with(emoji_result)
    history.assert_awaited_once_with(history_result)


@pytest.mark.asyncio()
async def test_delete_results_are_routed_by_type(mocker):
    process = mocker.patch.object(DeleteMessagesLoop, 'process_result', new_callable=AsyncMock)
    deletes = build_dispatch_loops(INCLUDE_BOTH, make_dispatcher(), MagicMock(), LOGGER)[1]
    history_result = ChannelHistoryResult(guild_id=1, channel_id=2, messages=[])
    for result in (history_result, GuildEmojisResult(guild_id=1, emojis=[])):
        await deletes.process(result)
    process.assert_awaited_once_with(history_result)
