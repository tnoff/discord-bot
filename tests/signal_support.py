'''Drive a pod's ``main_loop`` through a shutdown: catch its signal handlers, then fire one.'''
import asyncio
import signal

from discord_core.cli._lib import common as cli_common


def capture_signals(mocker):
    '''Intercept signal registration so the test can fire SIGTERM/SIGINT itself.'''
    captured = {}

    def _fake_signal(signum, handler):
        captured[signum] = handler

    mocker.patch.object(cli_common.signal, 'signal', new=_fake_signal)
    return captured


async def fire_sigterm(captured, *, signum=signal.SIGTERM, before=None):
    '''Give main_loop a few turns to register handlers and start tasks, then send it a signal.

    ``before`` runs just ahead of the signal, while the loops are still up.
    '''
    for _ in range(4):
        await asyncio.sleep(0)
    if before is not None:
        before()
    captured[signum](signum, None)
