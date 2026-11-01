'''
Registry of all optional cogs available to bot processes.

Kept in a separate module so that dispatcher.py (which has no SQLAlchemy dep)
can import cli/_lib/common.py without triggering heavy cog imports.
'''
import logging

from discord.ext.commands import Bot

from discord_core.clients.dispatch_client_base import DispatchClientBase
from discord_core.exceptions import CogMissingRequiredArg

from discord_gateway.cogs.general import General
from discord_gateway.cogs.markov import Markov
from discord_gateway.cogs.music import Music
from discord_gateway.cogs.role import RoleAssignment
from discord_gateway.cogs.urban import UrbanDictionary

POSSIBLE_COGS = [
    Markov,
    Music,
    RoleAssignment,
    UrbanDictionary,
    General,
]


def load_cogs(bot: Bot, cog_classes: list, settings: dict,
              dispatcher: DispatchClientBase, redis_manager=None) -> list:
    '''Attempt to instantiate each cog class; skip those missing required args.'''
    logger = logging.getLogger('main')
    cogs = []
    for cog_cls in cog_classes:
        try:
            cogs.append(cog_cls(bot, settings, dispatcher, redis_manager=redis_manager))
        except CogMissingRequiredArg as e:
            logger.debug(f'Main :: Cannot add cog {str(cog_cls)}, {str(e)}')
    return cogs
