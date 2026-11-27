'''The broker mock the commands pod's enqueue paths are tested against.'''
from unittest.mock import AsyncMock, MagicMock


def enqueue_broker_mock(enqueue_result='ok'):
    '''A broker whose registration, enqueue and discard calls are awaitable mocks.'''
    broker = MagicMock()
    broker.register_download = AsyncMock()
    broker.enqueue_track = AsyncMock(return_value=enqueue_result)
    broker.discard = AsyncMock()
    return broker
