'''
The Backstage catalog's API wiring, derived from the same measurements everything
else in this project reads.

Acceptance criterion eight of docs/projects/http-seam-contract.md: each pod
Component's `providesApis` / `consumesApis` comes from the registry rather than
being hand-listed. The test that this is real is the catalog rendering the seam
topology -- bot consuming broker, broker providing it -- not the fields merely
existing.

**Two measurements, joined on (peer image, seam).** The serving side comes from
tests/routes/_seam_servers, which builds each server and reads its router; the
calling side from tests/cli/_seam_topology, which imports each entrypoint and
finds the clients that declare a SEAM. Neither knows about Backstage, and
nothing here restates the topology -- the join is the only new thing, and it is
one line.

That join is exact rather than heuristic. `discord-search` serves TWO surfaces
(media_search and queue_worker/ytmusic) so the image alone is ambiguous, and
`queue_worker` is served by two images so the seam alone is ambiguous; the pair
is unique across all six. A test asserts that, because the day it stops being
true this would start attributing a dependency to the wrong pod silently.
'''
from tests.cli._seam_topology import _edges, measure_all as measure_topology, serving_images
from tests.routes._openapi import document_name
from tests.routes._seam_servers import SEAMS

CATALOG_API_NAMESPACE = 'default'


def surface_seam(surface: str) -> str:
    '''The seam a served surface belongs to: queue_worker/downloads -> queue_worker.'''
    return surface.split('/')[0]


def api_by_peer_and_seam() -> dict:
    '''{(serving image, seam): API entity name}. Unique by construction; asserted.'''
    return {(image, surface_seam(surface)): document_name(surface)
            for surface, _factory, _expected, image in SEAMS}


def provides() -> dict:
    '''{image: sorted API entity names it serves}.'''
    out = {}
    for surface, _factory, _expected, image in SEAMS:
        out.setdefault(image, set()).add(document_name(surface))
    return {image: sorted(names) for image, names in out.items()}


def consumes() -> dict:
    '''{image: sorted API entity names it calls}.

    Deduplicated on purpose. The bot reaches the database seam through three
    store clients at three prefixes, but they are three clients of ONE API, and
    listing it three times would be a catalog claim that is not true.
    '''
    lookup = api_by_peer_and_seam()
    out = {}
    for row in _edges(measure_topology(), serving_images()):
        out.setdefault(row['caller'], set()).add(lookup[(row['peer'], row['seam'])])
    return {image: sorted(names) for image, names in out.items()}


def api_ref(name: str) -> str:
    '''An entity reference as Backstage resolves it in providesApis/consumesApis.'''
    return f'api:{CATALOG_API_NAMESPACE}/{name}'
