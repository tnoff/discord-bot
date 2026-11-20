'''
HttpBrokerClient.checkout against a stub broker.

The route has two answer shapes (a hit and a miss) plus the miss shape older brokers sent, which
carried a null ``guild_file_path``. Each must map to the right CheckoutResult, or to None.
'''
import pytest

from discord_core.routes import broker as broker_routes
from discord_core.types.checkout_result import CheckoutResult

from discord_core.tests.clients.stub_broker import StubBrokerBase, connected


class StubBroker(StubBrokerBase):
    '''A broker that serves only the checkout route.'''
    routes = (broker_routes.CHECKOUT,)


@pytest.mark.asyncio
async def test_checkout_hit_returns_key_and_the_clients_own_bucket():
    stub = StubBroker()
    stub.answer(broker_routes.CHECKOUT, {'s3_key': 'cache/abc.pcm'})
    async with connected(stub, bucket_name='my-bucket') as client:
        result = await client.checkout('uuid-1', 123)
    assert result == CheckoutResult(s3_key='cache/abc.pcm', bucket_name='my-bucket')
    call = stub.last(broker_routes.CHECKOUT)
    assert call['match'] == {'uuid': 'uuid-1'}
    assert call['body'] == {'guild_id': 123}


@pytest.mark.asyncio
async def test_checkout_miss_returns_none():
    stub = StubBroker()
    stub.answer(broker_routes.CHECKOUT, {})
    async with connected(stub) as client:
        assert await client.checkout('uuid-1', 123) is None


@pytest.mark.asyncio
async def test_checkout_legacy_miss_with_null_guild_file_path_returns_none():
    '''Brokers from before local staging was removed answered a miss with a null guild_file_path.'''
    stub = StubBroker()
    stub.answer(broker_routes.CHECKOUT, {'guild_file_path': None})
    async with connected(stub) as client:
        assert await client.checkout('uuid-1', 123) is None
