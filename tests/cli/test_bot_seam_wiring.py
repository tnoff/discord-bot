'''
Which clients the bot process starts a route check for, and when.

The wiring is the part that rots. A new seam gets a registry, a SEAM constant
and a ROUTES_CALLED tuple, and then nothing ever calls start_seam_check on it --
which from outside is indistinguishable from a peer that is healthy. Four of the
five seams sat in exactly that state between #944 and this change.

See docs/projects/http-seam-contract.md, acceptance criterion five.
'''
import pytest

from discord_bot.cli.bot import register_seam_checks
from discord_bot.clients.http_broker_client import HttpBrokerClient
from discord_bot.utils.common import SeamContractConfig


class _RecordingBot:
    '''Records what got bound to on_ready, and through which API.

    Models the one behaviour that matters here: ``event`` REPLACES whatever is
    bound to an event name, ``add_listener`` appends. tests/helpers.FakeBot
    collapses both into one list, which is right for exercising startup but
    cannot show the displacement this file is about.
    '''

    def __init__(self):
        self.events = []
        self.listeners = []

    def event(self, func):
        self.events = [func]
        return func

    def add_listener(self, func, name=None):
        self.listeners.append((name, func))


def test_seam_checks_bind_without_displacing_on_ready():
    '''The decorator would have unregistered the guild-rejectlist pass.

    register_on_ready binds its handler with @bot.event, which on the real Bot
    replaces whatever that name points at. A second @bot.event here would take
    the rejectlist enforcement offline silently -- no error, no log, and the bot
    simply stops leaving guilds it is not allowed to be in. add_listener appends
    instead, so both run.
    '''
    bot = _RecordingBot()

    @bot.event
    async def on_ready():  # stands in for register_on_ready's handler
        return None

    register_seam_checks(bot)

    assert bot.events == [on_ready], 'register_seam_checks displaced the existing on_ready'
    assert [name for name, _ in bot.listeners] == ['on_ready']


@pytest.mark.asyncio(loop_scope='session')
async def test_the_listener_defers_until_the_gateway_is_up():
    '''Registration must not start the check; on_ready must.

    The clients are built in a synchronous run(), so starting a probe task at
    registration raises "no running event loop" on the pod's startup path. That
    the check is still None after register_seam_checks is the whole point of
    routing this through a listener, so it is asserted rather than assumed.
    '''
    bot = _RecordingBot()
    client = HttpBrokerClient('http://broker:8081',
                              seam_contract=SeamContractConfig(interval_seconds=60))

    register_seam_checks(bot)
    assert client.seam_check is None, 'check started before the gateway was up'

    assert len(bot.listeners) == 1
    _name, listener = bot.listeners[0]
    await listener()
    try:
        assert client.seam_check is not None
        assert client.seam_check.seam == 'broker'
    finally:
        await client.close()


@pytest.mark.asyncio(loop_scope='session')
async def test_a_gateway_reconnect_does_not_double_probe():
    '''on_ready fires again on every reconnect, and must not stack probe tasks.

    Left unguarded this is a slow leak rather than an obvious break: each
    reconnect would add a second polling loop against the same peer, and the
    pod would look fine while its request rate to the peer climbed.
    '''
    bot = _RecordingBot()
    client = HttpBrokerClient('http://broker:8081',
                              seam_contract=SeamContractConfig(interval_seconds=60))
    register_seam_checks(bot)
    assert len(bot.listeners) == 1
    _name, listener = bot.listeners[0]

    await listener()
    first_task = client.seam_check.probe_task
    await listener()
    try:
        assert client.seam_check.probe_task is first_task
    finally:
        await client.close()
