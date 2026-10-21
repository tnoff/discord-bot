import pytest

from discord_commands.servers.commands_health_server import CommandsHealthServer


@pytest.mark.asyncio
async def test_the_pod_is_healthy_while_it_is_up():
    server = CommandsHealthServer(port=18086, bind_address='127.0.0.1')
    assert await server._check() == (True, {})  # pylint: disable=protected-access
    assert await server._readiness_check() == (True, {})  # pylint: disable=protected-access


def test_the_pod_is_named_for_readiness_metrics():
    assert CommandsHealthServer(port=18086, bind_address='127.0.0.1').pod == 'commands'
