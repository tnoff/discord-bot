'''
Every seam's registry agrees with the server that serves it.

One parametrized module rather than five near-identical ones: five copies of this
comparison is exactly the duplication R0801 exists to catch, and it already fired
once during this work when two build_app() loops matched.

This is acceptance criterion three of docs/projects/http-seam-contract.md, applied
to all five seams. The broker seam has its own module as well
(test_broker_seam_contract.py), which keeps the incident fixture and the
client-source assertions specific to it.
'''
import pytest
from aiohttp import web

from discord_bot.routes import broker as broker_routes
from discord_bot.routes import contract
from discord_bot.routes import database as database_routes
from discord_bot.routes import dispatch as dispatch_routes
from discord_bot.routes import media_search as media_search_routes
from discord_bot.routes import queue_worker as queue_worker_routes
from discord_bot.clients.http_download_client import HttpDownloadClient
from discord_bot.clients.http_markov_store import HttpMarkovStore
from discord_bot.clients.http_playlist_store import HttpPlaylistStore
from discord_bot.clients.youtube_music_search_client import HttpYoutubeMusicSearchClient
from discord_bot.servers.broker_server import BrokerHttpServer
from discord_bot.servers.composite_server import CompositeHttpServer
from discord_bot.servers.database_server import DatabaseHttpServer
from discord_bot.servers.dispatch_server import DispatchHttpServer
from discord_bot.servers.download_server import DownloadHttpServer
from discord_bot.servers.media_search_server import MediaSearchHttpServer
from discord_bot.servers.youtube_music_search_server import YoutubeMusicSearchHttpServer
from discord_bot.workers.asyncio_broker import AsyncioBroker

CONTRACT_ENTRY = (contract.CONTRACT_ROUTE.method, contract.CONTRACT_ROUTE.template)


def _fully_configured_database():
    '''A db pod with every store, so its route set is the whole registry.

    The groups are conditional, so this is the only configuration whose served set
    equals ALL -- which is why the advertisement is a runtime read and this test
    has to say which pod shape it is asserting about.
    '''
    return DatabaseHttpServer(guild_analytics_store=object(), markov_store=object(),
                              playlist_store=object(), video_cache_store=object())


# (seam name, server factory, the routes that server should serve)
SEAMS = [
    ('broker', lambda: BrokerHttpServer(AsyncioBroker()), broker_routes.ALL),
    ('database', _fully_configured_database, database_routes.ALL),
    ('dispatch', lambda: DispatchHttpServer(object(), object()), dispatch_routes.ALL),
    ('media_search', lambda: MediaSearchHttpServer(object()), media_search_routes.ALL),
    # The queue-worker registry covers TWO pods at different prefixes. Both are
    # listed, because a test that walked the seam once would leave whichever pod
    # it skipped entirely unchecked -- the gap the project spec's route inventory
    # had when it counted this class once and reported 4 routes instead of 8.
    ('queue_worker/downloads', lambda: DownloadHttpServer(object()),
     queue_worker_routes.DOWNLOADS.all),
    ('queue_worker/ytmusic', lambda: YoutubeMusicSearchHttpServer(object()),
     queue_worker_routes.YTMUSIC.all),
]


def _served(app: web.Application) -> set:
    '''Seam routes the app serves: the router's real contents, minus the two
    things that are not seam routes -- HEAD (an add_get affordance) and the
    contract endpoint (served by every application server, owned by no seam).'''
    served = {entry for entry in contract.served_routes(app) if entry[0] != 'HEAD'}
    return served - {CONTRACT_ENTRY}


@pytest.mark.parametrize('name,factory,expected', SEAMS, ids=[s[0] for s in SEAMS])
def test_registry_and_router_agree(name, factory, expected):
    '''Both directions, because they fail differently.

    A served route missing from the registry is one no client can name. A registry
    route the server never registers is a client call that will 404 -- which is
    the mechanism behind both incidents this project came out of.
    '''
    served = _served(factory().build_app())
    declared = {(route.method, route.template) for route in expected}
    assert served - declared == set(), f'{name}: served but not in the registry'
    assert declared - served == set(), f'{name}: in the registry but not served'


@pytest.mark.parametrize('name,factory,expected', SEAMS, ids=[s[0] for s in SEAMS])
def test_every_server_advertises_its_seam(name, factory, expected):
    '''The advertisement is what the client-side subset check reads, so a seam
    whose routes are registered but not advertised would be invisible to it.'''
    app = factory().build_app()
    advertised = contract.served_routes(app)
    for route in expected:
        assert (route.method, route.template) in advertised, f'{name}: {route.template}'


def test_the_two_queue_worker_pods_do_not_overlap():
    '''One registry, two prefixes, and they must stay disjoint.

    Both groups come from the same four-route shape, so a bug in at_prefix() that
    dropped the prefix would silently give both pods identical routes -- and the
    search pod already co-hosts /search/ytmusic with the media-search seam's
    /search/spotify, so a collision there is a startup failure rather than a
    quiet shadow.
    '''
    downloads = {r.template for r in queue_worker_routes.DOWNLOADS.all}
    ytmusic = {r.template for r in queue_worker_routes.YTMUSIC.all}
    assert downloads & ytmusic == set()
    assert queue_worker_routes.ALL == queue_worker_routes.DOWNLOADS.all + \
        queue_worker_routes.YTMUSIC.all

@pytest.mark.parametrize('registry', [
    broker_routes, database_routes, dispatch_routes, media_search_routes,
    queue_worker_routes,
], ids=lambda r: r.__name__.rsplit('.', 1)[-1])
def test_a_registry_declares_no_route_twice(registry):
    '''No duplicates WITHIN a seam. Across seams is a different question -- below.'''
    keys = [(route.method, route.template) for route in registry.ALL]
    assert len(keys) == len(set(keys))


def test_seams_are_separate_url_spaces_not_one():
    '''Route uniqueness is per-PEER, not global, and asserting otherwise is wrong.

    `POST /downloads` is declared by two registries and both are correct: the
    broker serves it for the bot to post download RESULTS to, and the downloader
    pod serves it as its submit route. Different pods, different Services,
    different base_url -- so they are distinct routes that happen to share a path.
    A global-uniqueness assertion fails on exactly this, and satisfying it would
    mean renaming a live cross-pod route: the skew-sensitive change this whole
    project exists to avoid.

    Pinned as a test so the property is recorded rather than rediscovered.
    '''
    broker_paths = {(r.method, r.template) for r in broker_routes.ALL}
    downloads_paths = {(r.method, r.template) for r in queue_worker_routes.DOWNLOADS.all}
    assert broker_paths & downloads_paths == {('POST', '/downloads')}


def test_co_hosted_seams_do_not_collide():
    '''Where two seams DO share a pod, they must not overlap.

    The search pod fronts media_search (/search/spotify, /search/youtube) and the
    queue-worker ytmusic group (/search/ytmusic*) on one bind via
    CompositeHttpServer. That is the one place a duplicate path is a real
    collision, and aiohttp refuses it at startup rather than shadowing it -- so
    this guards the pod coming up at all.
    '''
    media = {(r.method, r.template) for r in media_search_routes.ALL}
    ytmusic = {(r.method, r.template) for r in queue_worker_routes.YTMUSIC.all}
    assert media & ytmusic == set()
    composite = CompositeHttpServer([MediaSearchHttpServer(object()),
                                     YoutubeMusicSearchHttpServer(object())])
    advertised = contract.served_routes(composite.build_app())
    assert media | ytmusic <= advertised


def test_a_bare_database_pod_serves_no_store_routes():
    '''The conditional groups, from the other end: a pod with no stores serves
    only the contract route, so the registry describes the tier and the
    advertisement describes the pod.'''
    assert _served(DatabaseHttpServer().build_app()) == set()


def test_database_bind_rejects_a_handler_mismatch():
    '''bind() is what makes a missing handler a startup failure rather than a 404.

    On this seam a 404 already means "that store is not configured on this pod",
    a supported state -- so an unregistered route would surface as nothing at all.
    '''
    with pytest.raises(ValueError, match='no handler'):
        database_routes.GUILD_ANALYTICS.bind({'get_analytics': object()})
    with pytest.raises(ValueError, match='undefined'):
        database_routes.GUILD_ANALYTICS.bind({
            'get_analytics': object(), 'record_play': object(), 'nope': object(),
        })


# The exact wire paths, pinned. This is deliberately a restatement of the
# registries and that is the point -- it is the one assertion a single-source
# registry cannot make about itself.
#
# Found by mutation while writing this module: renaming '/block' to '/blokc' in
# routes/queue_worker.py failed NOTHING. Both sides derive from the registry, so
# they agreed on the wrong string, and every drift test compared them against each
# other and passed. Single-sourcing removes DISAGREEMENT; it cannot notice that
# the agreed value changed.
#
# That is a cross-pod protocol change with no local symptom -- the running peers
# still serve the old path, so it surfaces as a 404 during the next roll, which is
# the exact failure this project exists to prevent. The runtime subset check would
# eventually catch it; this catches it at PR time instead.
#
# A route genuinely being renamed means updating this list in the same commit, and
# accepting that the two pods are incompatible until both have rolled.
EXPECTED_WIRE_PATHS = {
    'broker': {
        'POST /requests/{uuid}', 'PUT /requests/{uuid}/status',
        'POST /requests/{uuid}/checkout', 'POST /requests/{uuid}/release',
        'POST /requests/{uuid}/remove', 'POST /requests/{uuid}/discard',
        'POST /downloads', 'POST /downloads/register', 'GET /results/next',
        'POST /search-results', 'GET /search-results/next', 'POST /prefetch',
        'POST /cache/check', 'POST /cache/cleanup', 'GET /cache/count',
        'GET /bundles', 'POST /bundles', 'POST /bundles/{uuid}/finalize',
        'DELETE /bundles/{uuid}', 'GET /sessions', 'PUT /sessions/{guild_id}',
        'DELETE /sessions/{guild_id}',
    },
    'dispatch': {
        'POST /dispatch/send', 'POST /dispatch/delete',
        'POST /dispatch/update_mutable', 'POST /dispatch/remove_mutable',
        'POST /dispatch/update_mutable_channel', 'POST /dispatch/fetch_history',
        'POST /dispatch/fetch_emojis', 'GET /dispatch/results/{request_id}',
    },
    'media_search': {'POST /search/spotify', 'POST /search/youtube'},
    'queue_worker': {
        'POST /downloads', 'POST /downloads/clear', 'POST /downloads/block',
        'GET /downloads/status',
        'POST /search/ytmusic', 'POST /search/ytmusic/clear',
        'POST /search/ytmusic/block', 'GET /search/ytmusic/status',
    },
}


@pytest.mark.parametrize('seam,registry', [
    ('broker', broker_routes), ('dispatch', dispatch_routes),
    ('media_search', media_search_routes), ('queue_worker', queue_worker_routes),
])
def test_wire_paths_are_unchanged(seam, registry):
    '''A rename here is a cross-pod protocol change, not a refactor.'''
    actual = {f'{r.method} {r.template}' for r in registry.ALL}
    assert actual == EXPECTED_WIRE_PATHS[seam]


def test_database_wire_paths_are_unchanged():
    '''Checked by group and shape rather than by listing 33 strings: every route is
    a POST at /database/<group>/<name>, so the group names and per-group counts are
    what a rename would move.'''
    assert {g.name: len(g.all) for g in database_routes.GROUPS} == {
        'guild_analytics': 2, 'markov': 9, 'playlist': 16, 'video_cache': 6,
    }
    for group in database_routes.GROUPS:
        for name, route in group.routes.items():
            assert route.method == 'POST'
            assert route.template == f'/database/{group.name}/{name}'


@pytest.mark.parametrize('client_cls,group', [
    (HttpDownloadClient, queue_worker_routes.DOWNLOADS),
    (HttpYoutubeMusicSearchClient, queue_worker_routes.YTMUSIC),
])
def test_queue_worker_clients_claim_only_their_peer(client_cls, group):
    '''ROUTES_CALLED is one group, never the whole seam.

    The queue-worker registry covers two pods. A downloader client that declared
    all eight routes would have its subset check demand /search/ytmusic of the
    downloader pod, which correctly does not serve it -- turning a healthy peer
    into a permanent breach. Same trap on the database seam, where the groups are
    configured per pod.
    '''
    assert client_cls.ROUTES_CALLED == group.all
    assert client_cls.ROUTES is group
    other = (queue_worker_routes.YTMUSIC if group is queue_worker_routes.DOWNLOADS
             else queue_worker_routes.DOWNLOADS)
    assert set(client_cls.ROUTES_CALLED) & set(other.all) == set()


@pytest.mark.parametrize('store_cls,group', [
    (HttpMarkovStore, database_routes.MARKOV),
    (HttpPlaylistStore, database_routes.PLAYLIST),
])
def test_store_clients_claim_only_their_group(store_cls, group):
    '''A markov client must not demand its peer serve the playlist routes -- a db
    pod legitimately serves a subset, so requiring all 33 would fire the subset
    check against a correctly-configured peer.'''
    assert store_cls.ROUTES_CALLED == group.all
    assert store_cls.ROUTE_PREFIX == group.prefix
