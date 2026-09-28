'''
Registry of all optional cogs available to bot processes.

Kept in a separate module so that dispatcher.py (which has no SQLAlchemy dep)
can import cli/_lib/common.py without triggering heavy cog imports.
'''
from discord_gateway.cogs.delete_messages import DeleteMessages
from discord_gateway.cogs.general import General
from discord_gateway.cogs.markov import Markov
from discord_gateway.cogs.music import Music
from discord_gateway.cogs.role import RoleAssignment
from discord_gateway.cogs.urban import UrbanDictionary

POSSIBLE_COGS = [
    DeleteMessages,
    Markov,
    Music,
    RoleAssignment,
    UrbanDictionary,
    General,
]
