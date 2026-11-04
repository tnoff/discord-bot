'''Setups the broker server and client tests share: a broker with a request, and its download.'''
import asyncio

from tests.fakes.asyncio_broker import AsyncioBroker
from tests.fakes.asyncio_queues import make_broker_http_server
from tests.helpers import fake_source_dict, generate_fake_context


def broker_and_request():
    '''A fresh broker and a request that has not been registered with it.'''
    return AsyncioBroker(), fake_source_dict(generate_fake_context())


async def broker_with_result_queue():
    '''A broker with a registered request, and the HTTP server over it whose results land on a queue.

    Returns (broker, request, result_queue, server).
    '''
    broker, media_request = broker_and_request()
    await broker.register_request(media_request)
    result_queue: asyncio.Queue = asyncio.Queue()
    server = make_broker_http_server(broker, result_queue=result_queue)
    return broker, media_request, result_queue, server
