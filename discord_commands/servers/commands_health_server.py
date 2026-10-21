'''
HTTP health server for the commands pod.

The pod holds no connection that can be down: its work is its own queue and HTTP calls it makes per
command, so liveness is "the process is serving", plus the background-loop health HealthServerBase
folds in for every pod. Readiness is the same until the pod has a peer it cannot work without.
'''
from typing import ClassVar

from discord_core.servers.health_server_base import HealthServerBase


class CommandsHealthServer(HealthServerBase):
    '''Responds 200 {"status": "ok"} while the process is up (and no registered loop is stalled).'''

    POD_NAME: ClassVar[str] = 'commands'

    async def _check(self):
        return True, {}
