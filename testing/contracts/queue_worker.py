'''
The queue-worker seam's contract: submitting to, clearing, blocking and reading the status of a worker pod's queue.

One route shape served twice, by the downloader at /downloads and the search pod at /search/ytmusic, so one table is
built per prefix. A queue refusal is not a fault: a blocked guild is a 409 and a full queue a 429, both with a
`SubmitRejectedResponse` naming the exception, and the client turns them back into `PutsBlocked` / `QueueFull`.

The request bodies and every answer are `discord_core` types, so this table only adds the summaries and which
statuses a route may answer. Both ends read it: the pods' servers are proved against it (tests/routes/specs/
queue_worker.py runs every reply) and the commands pod's clients are tested against a stub that serves it
(testing/queue_client_suite.py), so neither needs the other pod.
'''
from discord_core.routes import queue_worker as queue_worker_routes
from discord_core.types import queue_worker_responses as out
from discord_core.types.queue_worker_requests import BlockRequestBody, ClearRequestBody, SubmitRequestBody

from testing.contracts.model import Contract, reply


def contracts_for(routes: queue_worker_routes.QueueWorkerRoutes, noun: str) -> dict:
    '''The four routes of one prefix, described for the `noun` the pod works through.'''
    return {
        routes.submit: Contract(
            f'Queue a request for {noun}.', request=SubmitRequestBody,
            replies=(reply(202, out.SubmitAcceptedResponse),
                     reply(409, out.SubmitRejectedResponse, doc='The guild is blocked.'),
                     reply(429, out.SubmitRejectedResponse, doc='The queue is full.'),
                     reply(422, doc='guild_id or media_request is missing or invalid.'))),
        routes.clear: Contract(
            "Drop a guild's pending requests, returning what was dropped.", request=ClearRequestBody,
            replies=(reply(200, out.ClearGuildResponse), reply(422, doc='guild_id is missing.'))),
        routes.block: Contract(
            'Refuse new requests for a guild.', request=BlockRequestBody,
            replies=(reply(200, out.BlockResponse), reply(422, doc='guild_id is missing.'))),
        routes.status: Contract(
            "A snapshot of the pod's queues, failures and backoff, for the gateway's poller.",
            replies=(reply(200, out.StatusSnapshotResponse),)),
    }


DOWNLOADS = contracts_for(queue_worker_routes.DOWNLOADS, 'download')
YTMUSIC = contracts_for(queue_worker_routes.YTMUSIC, 'a YouTube Music search')
