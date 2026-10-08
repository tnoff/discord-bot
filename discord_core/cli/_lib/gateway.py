'''
Bot construction — the one entrypoint helper that needs discord.py at runtime.

Everything that touches a ``Bot`` lives here, so cli/_lib/common.py (imported by
every entrypoint) never has to name discord.py: ``build_bot`` calls
``Intents.default()``, ``when_mentioned_or`` and the ``Bot`` constructor, and the
lifecycle helpers below take the Bot it builds.

Only cli.bot and cli.dispatcher build a Bot, and both ship discord.py anyway:
the gateway connects, and the dispatcher sends and edits real messages.
'''
import asyncio
import contextlib
import logging
import signal
from asyncio import get_running_loop
from contextlib import asynccontextmanager
from typing import Callable, Iterator

from discord import Intents
from discord.ext.commands import Bot, when_mentioned_or

from discord_core.clients.dispatch_client_base import DispatchClientBase
from discord_core.exceptions import CogMissingRequiredArg
from discord_core.utils.common import GeneralConfig


def build_bot(general_config: GeneralConfig) -> Bot:
    '''Construct and return the Bot instance.'''
    logger = logging.getLogger('main')
    logger.debug('Main :: Generating Intents')
    intents = Intents.default()
    for intent in list(general_config.intents):
        logger.debug(f'Main :: Adding extra intents: {intent}')
        setattr(intents, intent, True)

    return Bot(
        command_prefix=when_mentioned_or('!'),
        description='Discord bot',
        intents=intents,
    )


class ShutdownState:
    '''Mutable flag shared between the signal handler and the main loop.'''
    def __init__(self):
        self.triggered: bool = False

    def __bool__(self) -> bool:
        return self.triggered


@contextlib.contextmanager
def handle_shutdown_signals(bot: Bot) -> Iterator[ShutdownState]:
    '''
    Register SIGTERM/SIGINT handlers for the duration of the with-block.

    Yields a ShutdownState whose .triggered is set to True when a signal arrives.
    Callers may also set .triggered = True directly (e.g. on KeyboardInterrupt)
    so shutdown detection is unified.
    '''
    state = ShutdownState()
    loop = get_running_loop()
    logger = logging.getLogger('main')

    def signal_handler(signum, _frame):
        if state.triggered:
            return
        state.triggered = True
        logger.info(f'Main :: Received {signal.Signals(signum).name}, triggering graceful shutdown...')
        if not bot.is_closed():
            loop.create_task(bot.close())

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    yield state


async def unload_cogs(cog_list: list) -> None:
    '''Call cog_unload() on every cog that exposes it, logging any errors.'''
    logger = logging.getLogger('main')
    for cog in cog_list:
        if hasattr(cog, 'cog_unload'):
            try:
                logger.debug(f'Main :: Calling cog_unload on {cog.__class__.__name__}')
                await cog.cog_unload()
            except Exception as e:
                logger.exception(f'Main :: Error during cog_unload for {cog.__class__.__name__}: {str(e)}')


@asynccontextmanager
async def bot_lifecycle(bot: Bot, cog_list: list, health_server=None,
                        on_shutdown: Callable | None = None):
    '''
    Async context manager encapsulating the shared bot try/except/finally pattern.

    Registers shutdown signal handlers, loads cogs, starts the optional health
    server, then yields control so the caller can run bot.start() or bot.login().
    On exit (normal or signal), unloads cogs, closes the bot, and calls on_shutdown
    if provided.

    Usage::

        async with bot_lifecycle(bot, cog_list, health_server=hs,
                                  on_shutdown=dispatcher.stop):
            logger.info('Starting…')
            await bot.start(token)
    '''
    logger = logging.getLogger('main')
    with handle_shutdown_signals(bot) as shutdown:
        async with bot:
            for cog in cog_list:
                await bot.add_cog(cog)
            if health_server:
                asyncio.create_task(health_server.serve())
            try:
                yield shutdown
            except KeyboardInterrupt:
                logger.info('Main :: Received keyboard interrupt, shutting down gracefully...')
                shutdown.triggered = True
            except Exception as exc:
                logger.debug('Main :: Shutting down main loop: %s', str(exc))
            finally:
                if shutdown:
                    await unload_cogs(cog_list)
                    if not bot.is_closed():
                        await bot.close()
                    if on_shutdown is not None:
                        await on_shutdown()
                    logger.info('Main :: Graceful shutdown complete')




def register_on_ready(bot: Bot, general_config: GeneralConfig, logger) -> None:
    '''Register an on_ready event that logs guild membership and enforces the rejectlist.'''
    rejectlist_guilds = list(general_config.rejectlist_guilds)
    logger.info(f'Main :: Gathered guild reject list {rejectlist_guilds}')

    @bot.event
    async def on_ready():
        logger.info(f'Main :: Starting bot, logged in as {bot.user} (ID: {bot.user.id})')
        guilds = [guild async for guild in bot.fetch_guilds(limit=150)]
        for guild in guilds:
            if guild.id in rejectlist_guilds:
                logger.info(f'Main :: Bot currently in guild {guild.id} thats within reject list, leaving server')
                await guild.leave()
                continue
            logger.info(f'Main :: Bot associated with guild {guild.id} with name "{guild.name}"')



def load_cogs(bot: Bot, cog_classes: list, settings: dict, stores,
              dispatcher: DispatchClientBase, redis_manager=None) -> list:
    '''Attempt to instantiate each cog class; skip those missing required args.

    stores is the DatabaseStores bundle, in the slot db_engine occupied before
    MR 4b. Cogs that need no persistence still take the argument and ignore it,
    which is what lets this loop call every constructor the same way.
    '''
    logger = logging.getLogger('main')
    cogs = []
    for cog_cls in cog_classes:
        try:
            cogs.append(cog_cls(bot, settings, dispatcher, stores,
                                redis_manager=redis_manager))
        except CogMissingRequiredArg as e:
            logger.debug(f'Main :: Cannot add cog {str(cog_cls)}, {str(e)}')
    return cogs
