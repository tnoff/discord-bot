'''
Bot construction — the one entrypoint helper that needs discord.py at runtime.

Everything else in cli/_lib/common.py referenced ``Bot`` only in an annotation,
so those moved under TYPE_CHECKING. ``build_bot`` actually calls
``Intents.default()``, ``when_mentioned_or`` and the ``Bot`` constructor, so it
has to import discord for real — and cli/_lib/common is imported by all five
entrypoints, which is how discord.py reached the broker, downloader and search
images.

Only cli.bot and cli.dispatcher build a Bot, and both ship discord.py anyway:
the gateway connects, and the dispatcher sends and edits real messages.
'''
import logging

from discord import Intents
from discord.ext.commands import Bot, when_mentioned_or

from discord_bot.utils.common import GeneralConfig


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
