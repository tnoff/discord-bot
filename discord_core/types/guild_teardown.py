'''
Body of the answer to POST /guilds/{guild_id}/teardown (routes/guild_teardown.py).
'''
from typing import Literal

from pydantic import BaseModel


class GuildTeardownAccepted(BaseModel):
    '''202: the pod has taken the teardown job, not finished it.'''
    status: Literal['ok'] = 'ok'
