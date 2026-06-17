'''
Registry of all optional cogs available to bot processes.

Kept in a separate module so that dispatcher.py (which has no SQLAlchemy dep)
can import cli/common.py without triggering heavy cog imports.
'''
from discord_bot.cogs.delete_messages import DeleteMessages
from discord_bot.cogs.general import General
from discord_bot.cogs.markov import Markov
from discord_bot.cogs.music import Music
from discord_bot.cogs.role import RoleAssignment
from discord_bot.cogs.urban import UrbanDictionary

POSSIBLE_COGS = [
    DeleteMessages,
    Markov,
    Music,
    RoleAssignment,
    UrbanDictionary,
    General,
]
