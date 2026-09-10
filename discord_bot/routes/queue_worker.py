'''
The queue-worker seam: one route shape, served at two prefixes by two pods.

Imported by `servers/queue_worker_server.py` and
`clients/http_queue_worker_client.py` plus their subclasses — fanout 3 (bot,
downloader, search).

**This is one registry and two seams, which is why it does not look like the
others.** `QueueWorkerHttpServer` defines four routes off a `ROUTE_PREFIX`
ClassVar and is subclassed twice: `DownloadHttpServer` at `/downloads` in the
downloader pod, and `YoutubeMusicSearchHttpServer` at `/search/ytmusic` in the
search pod. The four routes are written once, in the base class, so one registry
module covers both — which is also what keeps this at fanout 3 rather than
splitting into two modules of 2 and 2.

The route inventory in the project spec counted the class once and so reported 4
routes here; the served total is 8, because two pods each register the set.

**The prefixes were the duplication.** Before this, `ROUTE_PREFIX` was declared
as a bare literal on the server subclass AND on the matching client subclass --
'/downloads' in two files, '/search/ytmusic' in two more -- strings that merely
happened to agree. A prefix typo moves every route on the seam at once, so it is
the more damaging half of the defect this project exists to remove, not the
lesser one.
'''
from dataclasses import dataclass

from discord_bot.routes.route import Route


@dataclass(frozen=True)
class QueueWorkerRoutes:
    '''The four routes a queue-worker pod serves under one prefix.

    A group rather than four module-level constants because the set is
    instantiated per prefix. `submit` is the bare prefix itself, with no suffix,
    which is why it cannot be derived by appending a name.
    '''
    submit: Route
    clear: Route
    block: Route
    status: Route

    @property
    def all(self) -> tuple[Route, ...]:
        '''Every route in this group, for the drift test and the subset check.'''
        return (self.submit, self.clear, self.block, self.status)

    @property
    def prefix(self) -> str:
        '''The prefix these routes hang off — the submit route's own template.'''
        return self.submit.template


def at_prefix(prefix: str) -> QueueWorkerRoutes:
    '''Build the queue-worker route set for one pod's prefix.'''
    return QueueWorkerRoutes(
        submit=Route('POST', prefix),
        clear=Route('POST', f'{prefix}/clear'),
        block=Route('POST', f'{prefix}/block'),
        status=Route('GET', f'{prefix}/status'),
    )


#: The downloader pod's set.
DOWNLOADS = at_prefix('/downloads')
#: The search pod's set. Shares the search pod with the media-search seam's
#: /search/spotify and /search/youtube, which live in routes/media_search.py.
YTMUSIC = at_prefix('/search/ytmusic')

#: Both groups, for a test that wants every route this seam serves anywhere.
ALL = DOWNLOADS.all + YTMUSIC.all
