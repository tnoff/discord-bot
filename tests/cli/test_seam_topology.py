'''
The seam dependency graph, and the assertions that keep it honest.

docs/seam-topology.md is generated from the same `SEAM` / `ROUTES_CALLED`
declarations the runtime check reads, so it cannot disagree with the registries.
What it *can* do is overstate: the measurement is import reachability, and a
module can be imported without anything constructing the class inside it. The
second test below is what rules that out, by pinning the measured set against
the clients the entrypoints actually build.

The serving side is derived too -- from the route registry each server module
imports, whose basename is the seam name the clients declare -- so the graph is
pod-to-pod with no prefix-to-pod map written down anywhere.

See docs/projects/http-seam-contract.md.
'''
import os

from tests.cli._seam_topology import (
    TOPOLOGY_DOC, measure_all, render_doc, resolve_peer, serving_images,
)

# The clients each image actually constructs, as a deliberate restatement of the
# wiring in cli/*.py and cogs/music.py. This is the same device as
# EXPECTED_WIRE_PATHS in tests/routes/test_seam_registries.py and it exists for
# the same reason: everything else in this file derives from one source, and a
# derivation cannot notice that the source became wrong.
#
# Adding a client to a pod means adding it here in the same commit.
BUILT_CLIENTS = {
    'discord-bot': {
        'HttpBrokerClient', 'HttpDispatchClient', 'HttpDownloadClient',
        'HttpGuildAnalyticsStore', 'HttpMarkovStore', 'HttpMediaSearchClient',
        'HttpPlaylistStore', 'HttpYoutubeMusicSearchClient',
    },
    'discord-broker': {'HttpDispatchClient', 'HttpVideoCacheStore'},
    'discord-downloader': {'HttpBrokerClient'},
    'discord-search': {'HttpBrokerClient'},
    # Serve-only pods. Empty sets, not absent keys: "this image calls nothing"
    # is a claim worth stating, and an absent key would read as an oversight.
    'discord-db': set(),
    'discord-dispatcher': set(),
}


def test_every_measured_client_is_one_a_pod_builds():
    '''Import reachability must equal what the entrypoints actually construct.

    The doc is only trustworthy if these two agree. A class that becomes
    reachable without being built would add an edge to the graph that does not
    exist in prod -- and a dependency graph that overstates is worse than none,
    because it is the thing someone reads before deciding a seam is load-bearing.
    '''
    measured = {image: set(clients) for image, clients in measure_all().items()}
    assert measured == BUILT_CLIENTS


def test_every_seam_declared_has_a_caller():
    '''A registry nothing calls is either dead or unwired.

    Both are worth surfacing, and they look identical from the registry side.
    The four seams that sat declared-but-unwired between #944 and #947 were
    exactly this shape.
    '''
    seams = {client['seam']
             for clients in measure_all().values()
             for client in clients.values()}
    assert seams == {'broker', 'database', 'dispatch', 'media_search', 'queue_worker'}


def test_queue_worker_is_called_at_two_prefixes():
    '''The one seam served by two pods, which the graph must not collapse.

    QueueWorkerHttpServer is subclassed twice -- /downloads on the downloader pod
    and /search/ytmusic on the search pod -- so a caller-to-seam edge is ambiguous
    without the prefix. Asserted because a future refactor that derived the
    prefix from the seam name would silently merge two real dependencies into one.
    '''
    bot_clients = measure_all()['discord-bot']
    prefixes = {name: client['prefix'] for name, client in bot_clients.items()
                if client['seam'] == 'queue_worker'}
    assert prefixes == {
        'HttpDownloadClient': '/downloads',
        'HttpYoutubeMusicSearchClient': '/search/ytmusic',
    }


def test_every_seam_resolves_to_a_serving_pod():
    '''The serving side, derived from the registry each server module imports.

    Exactly one pod per seam except queue_worker, which is the abstract base
    subclassed twice. Pinned because the derivation excludes routes/route.py and
    routes/contract.py by name -- both are fanout-6 machinery every server
    imports, and letting either through would make every pod claim every seam.
    '''
    assert serving_images() == {
        'broker': {'discord-broker': ''},
        'database': {'discord-db': ''},
        'dispatch': {'discord-dispatcher': ''},
        'media_search': {'discord-search': ''},
        'queue_worker': {
            'discord-downloader': '/downloads',
            'discord-search': '/search/ytmusic',
        },
    }


def test_every_client_resolves_to_exactly_one_peer():
    '''No edge in the graph may be left dangling.

    An unresolved peer means a client calls a seam no reachable image serves, or
    that two pods serve one seam and neither prefix matched. Both would render as
    "unresolved" in the doc rather than failing, so the assertion lives here --
    a graph with a hole in it is the kind of thing a reader skims past.
    '''
    serving = serving_images()
    unresolved = [
        (image, name, client['seam'])
        for image, clients in measure_all().items()
        for name, client in clients.items()
        if not resolve_peer(serving, client['seam'], client['prefix'])
    ]
    assert not unresolved


def test_the_two_queue_worker_peers_are_different_pods():
    '''The whole point of resolving by prefix rather than by seam.

    Collapsing these two onto one peer is the failure the pod-to-pod graph exists
    to prevent: it would show the bot depending on one queue-worker pod when it
    depends on two, and hide the downloader seam entirely behind the search one.
    '''
    serving = serving_images()
    assert resolve_peer(serving, 'queue_worker', '/downloads') == 'discord-downloader'
    assert resolve_peer(serving, 'queue_worker', '/search/ytmusic') == 'discord-search'


def test_topology_doc_is_current():
    '''docs/seam-topology.md matches a live measurement.

    Regenerate with: UPDATE_SEAM_TOPOLOGY=1 pytest tests/cli/test_seam_topology.py
    '''
    rendered = render_doc()
    if os.environ.get('UPDATE_SEAM_TOPOLOGY'):
        TOPOLOGY_DOC.write_text(rendered, encoding='utf-8')
    assert TOPOLOGY_DOC.read_text(encoding='utf-8') == rendered, (
        'docs/seam-topology.md is out of date. Regenerate with:\n'
        '    UPDATE_SEAM_TOPOLOGY=1 pytest tests/cli/test_seam_topology.py'
    )
