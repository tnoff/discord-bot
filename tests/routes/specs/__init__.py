'''
Per-route request and response descriptions for the OpenAPI documents.

One module per served surface, each exporting a route table (route -> ``Operation``) and an
``environment()`` (an async context manager yielding an ``Env`` over a live server).
``SURFACES`` maps a surface name, as ``tests/routes/_seam_servers.SEAMS`` names it, to those two.
'''
from tests.routes.specs import broker, commands, database, dispatch, media_search, queue_worker
from tests.routes.specs._model import Surface

SURFACES = {
    'broker': Surface(broker.OPERATIONS, broker.environment),
    'commands': Surface(commands.OPERATIONS, commands.environment),
    'database': Surface(database.OPERATIONS, database.environment),
    'dispatch': Surface(dispatch.OPERATIONS, dispatch.environment),
    'media_search': Surface(media_search.OPERATIONS, media_search.environment),
    'queue_worker/downloads': Surface(queue_worker.DOWNLOADS_OPERATIONS, queue_worker.downloads_environment),
    'queue_worker/ytmusic': Surface(queue_worker.YTMUSIC_OPERATIONS, queue_worker.ytmusic_environment),
}
