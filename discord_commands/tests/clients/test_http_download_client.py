'''HttpDownloadClient, tested against the downloader's queue-worker contract and nothing else.

The shared cases are `testing/queue_client_suite.py`; what the downloader pod does with a body it accepts is
discord_downloader/tests/servers/test_download_server.py, and that the two agree is
tests/integration/commands_to_downloader/test_download_round_trip.py.
'''
# NOTE: test names here deliberately avoid being exactly 40 characters long.
# trufflehog's Lob detector matches `test_` + 35 chars, so a 40-char test
# function name is reported as a VERIFIED secret and fails pr-check:secrets.
import asyncio

import pytest

from discord_commands.clients.http_download_client import HttpDownloadClient
from discord_commands.clients.http_queue_worker_client import _exception_detail

from testing.contracts.queue_worker import DOWNLOADS
from testing.queue_client_suite import QueueClientSuite, QueuePollerSuite


class TestHttpDownloadClient(QueueClientSuite, QueuePollerSuite):
    '''The shared queue-client cases, against the downloader's contract.'''
    client_cls = HttpDownloadClient
    contracts = DOWNLOADS


@pytest.mark.asyncio
async def test_poll_status_loop_runs_one_iteration_then_exits(mocker):
    '''The loop runs at least one iteration body before shutdown fires.'''
    client = HttpDownloadClient('http://localhost:9999')
    mocker.patch.object(client, 'POLL_INTERVAL_SECONDS', 0.01)
    iterations = {'n': 0}
    real_iter = client._poll_status_loop_once  # pylint: disable=protected-access

    async def counting_iter():
        iterations['n'] += 1
        await real_iter()
        client._poller_stop.set()  # pylint: disable=protected-access

    mocker.patch.object(client, '_http', return_value=None)
    mocker.patch.object(client, '_poll_status_loop_once', side_effect=counting_iter)
    await client._poll_status_loop()  # pylint: disable=protected-access
    assert iterations['n'] >= 1


def test_message_less_exception_logs_its_type():
    '''str() on a timeout is empty; the type name is the only detail it has.'''
    assert _exception_detail(asyncio.TimeoutError()) == 'TimeoutError'
    assert _exception_detail(ConnectionResetError()) == 'ConnectionResetError'


def test_a_described_exception_is_passed_through():
    '''The informative case stays byte-identical, with no type-name prefix.'''
    assert _exception_detail(RuntimeError('Cannot connect to host x:8083')) == 'Cannot connect to host x:8083'
