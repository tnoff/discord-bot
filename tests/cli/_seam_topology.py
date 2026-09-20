'''
Measure which pod calls which seam, and render the graph.

The consuming half of the seam contract. Every HTTP client that speaks a seam
declares two class attributes -- ``SEAM`` and ``ROUTES_CALLED`` -- and those are
already the source of truth the runtime check reads, so the dependency graph is
derivable rather than something anyone has to keep in step by hand. See
docs/projects/http-seam-contract.md.

**Measured by import, in a clean interpreter, like _image_deps.** Reading the
code would miss clients a cog constructs rather than the entrypoint, which is
most of the bot's. Importing an entrypoint and asking which seam-speaking classes
became reachable finds them wherever they are built.

Reachability could in principle overstate -- a module can be imported without
anything constructing the class in it. It does not here, and that is asserted
rather than assumed: ``test_every_measured_client_is_one_a_pod_builds`` pins the
measured set against the clients the entrypoints actually wire, so the day an
image starts merely importing a client it does not use, the doc stops claiming
the edge silently and a test says so.

**The serving side is derived without any server declaring a seam**, which is
what makes this a pod-to-pod graph rather than a bipartite pod-to-seam one. A
server class does not carry a ``SEAM``, but its MODULE imports exactly one route
registry, and a registry module's basename IS the seam name the clients declare
-- ``discord_bot.routes.broker`` and ``SEAM = 'broker'``. Both sides therefore
derive the same token from the same file, and no prefix-to-pod map is written
down anywhere.

Two registry modules are excluded from that rule on purpose: ``route`` (the
shared dataclass) and ``contract`` (the advertisement endpoint). Both are
fanout-6 cross-cutting machinery imported by every server, so counting them
would make every pod look like it serves every seam.

Disambiguation, where a seam is served by more than one pod, is by prefix --
``queue_worker`` is served at ``/downloads`` by the downloader and at
``/search/ytmusic`` by the search pod, and the queue-worker server subclasses
carry a ``ROUTES`` group whose prefix matches the client's exactly.
'''
import json

from tests.cli._image_deps import IMAGE_NAMES, REPO_ROOT, run_probe

TOPOLOGY_DOC = REPO_ROOT / 'docs' / 'seam-topology.md'

# Probes one entrypoint for every reachable class that declares a seam. Keyed by
# class name because that is what a reader greps for; the module comes back too
# so the table can point at the file.
_PROBE = (
    'import importlib, inspect, json, sys; '
    'importlib.import_module({entrypoint!r}); '
    'found = {{}}; '
    '[found.setdefault(obj.__name__, {{'
    '"seam": obj.SEAM, "module": name, '
    '"prefix": getattr(obj, "ROUTE_PREFIX", "") or "", '
    '"routes": sorted(r.method + " " + r.template for r in obj.ROUTES_CALLED)}}) '
    'for name in sorted(sys.modules) if name.startswith("discord_bot.") '
    'for _a, obj in sorted(vars(sys.modules[name]).items()) '
    'if inspect.isclass(obj) and getattr(obj, "__module__", None) == name '
    'and getattr(obj, "SEAM", None) and getattr(obj, "ROUTES_CALLED", ())]; '
    'print(json.dumps(found))'
)


# The serving side. A server class carries no SEAM, but the module it lives in
# imports exactly one route registry, and that registry's basename is the seam.
# `route` and `contract` are excluded: every server imports them, so counting
# them would make every pod appear to serve every seam.
#
# A registry is recognised by sitting in a `routes` package, NOT by a fixed
# `discord_bot.routes.` prefix. The prefix form stopped matching the moment the
# per-image-code-split moved a seam: `routes/database.py` becomes
# `seams/database/routes/database.py`, and the database seam silently vanished
# from the measured topology while every test still passed on the others.
_SERVER_PROBE = (
    'import importlib, inspect, json, sys; '
    'importlib.import_module({entrypoint!r}); '
    'from discord_bot.servers.base import AiohttpServerBase; '
    'skip = {{"discord_bot.core.routes.route", "discord_bot.core.routes.contract"}}; '
    'found = {{}}; '
    '[found.setdefault(obj.__name__, {{'
    '"seams": sorted({{v.__name__.rsplit(".", 1)[1] '
    'for v in vars(sys.modules[name]).values() if inspect.ismodule(v) '
    'and v.__name__.startswith("discord_bot.") and v.__name__ not in skip '
    'and v.__name__.split(".")[-2:-1] == ["routes"]}}), '
    '"prefix": getattr(getattr(obj, "ROUTES", None), "prefix", "")}}) '
    'for name in sorted(sys.modules) if name.startswith("discord_bot.") '
    'for _a, obj in sorted(vars(sys.modules[name]).items()) '
    'if inspect.isclass(obj) and getattr(obj, "__module__", None) == name '
    'and issubclass(obj, AiohttpServerBase) and obj is not AiohttpServerBase]; '
    'print(json.dumps(found))'
)


def measure(entrypoint: str) -> dict:
    '''Seam-speaking clients reachable from `entrypoint`, keyed by class name.'''
    return json.loads(run_probe(_PROBE.format(entrypoint=entrypoint)))


def measure_servers(entrypoint: str) -> dict:
    '''Seam-serving server classes reachable from `entrypoint`.

    Classes with no registry import are dropped: that is how
    ``QueueWorkerHttpServer`` (an abstract base, its prefix set by subclasses)
    and ``CompositeHttpServer`` (a wrapper that owns no routes of its own) fall
    out without either being named here.
    '''
    found = json.loads(run_probe(_SERVER_PROBE.format(entrypoint=entrypoint)))
    return {name: info for name, info in found.items() if info['seams']}


def measure_all() -> dict:
    '''{image name: {client class: {seam, module, prefix, routes}}} for every image.'''
    return {image: measure(entrypoint) for entrypoint, image in IMAGE_NAMES.items()}


def serving_images() -> dict:
    '''{seam: {image: prefix}} -- which pod answers each seam.'''
    serving = {}
    for entrypoint, image in IMAGE_NAMES.items():
        for info in measure_servers(entrypoint).values():
            for seam in info['seams']:
                serving.setdefault(seam, {})[image] = info['prefix']
    return serving


def resolve_peer(serving: dict, seam: str, prefix: str) -> str:
    '''The image serving `seam` for a client calling it at `prefix`.

    One candidate is the common case and the prefix is not consulted -- the
    database stores each call their own sub-prefix while the db pod's server
    declares none. Where a seam has several servers, the prefix is what tells
    them apart, and it must match exactly rather than by containment: /downloads
    and /search/ytmusic are different pods, and a substring rule would be one
    renamed prefix away from silently pairing a client with the wrong peer.
    '''
    candidates = serving.get(seam, {})
    if len(candidates) == 1:
        return next(iter(candidates))
    for image, served_prefix in sorted(candidates.items()):
        if served_prefix == prefix:
            return image
    return ''


def _edges(topology: dict, serving: dict) -> list:
    '''One (caller, peer, seam, prefix, client, routes) row per client.'''
    rows = []
    for image in sorted(topology):
        for name, client in sorted(topology[image].items(),
                                   key=lambda kv: (kv[1]['seam'], kv[0])):
            rows.append({
                'caller': image,
                'peer': resolve_peer(serving, client['seam'], client['prefix']),
                'seam': client['seam'],
                'prefix': client['prefix'],
                'client': name,
                'routes': len(client['routes']),
            })
    return rows


def _mermaid(rows: list) -> list:
    '''A pod -> pod graph, one edge per (caller, peer, seam).

    Pod nodes on both ends rather than a seam node in the middle. A seam is a
    route shape, not a deployable: drawing it as a node made queue_worker -- the
    one seam served by two pods -- read as a service of its own, which it is not.
    Edges carry the seam name so the two queue_worker edges stay distinct.
    '''
    lines = ['```mermaid', 'graph LR']
    pods = sorted({r['caller'] for r in rows} | {r['peer'] for r in rows if r['peer']})
    for pod in pods:
        lines.append(f'  {pod.replace("-", "_")}["{pod}"]')
    merged = {}
    for row in rows:
        if not row['peer']:
            continue
        key = (row['caller'], row['peer'], row['seam'])
        merged[key] = merged.get(key, 0) + row['routes']
    for (caller, peer, seam), count in sorted(merged.items()):
        lines.append(f'  {caller.replace("-", "_")} -->|{seam} {count}| '
                     f'{peer.replace("-", "_")}')
    lines.append('```')
    return lines


def render_doc() -> str:
    '''Render the topology doc from a live measurement.'''
    topology = measure_all()
    serving = serving_images()
    rows = _edges(topology, serving)
    lines = [
        '# Which pod calls which',
        '',
        '<!-- GENERATED by tests/cli/test_seam_topology.py. Do not edit by hand.',
        '     Regenerate with: UPDATE_SEAM_TOPOLOGY=1 pytest tests/cli/test_seam_topology.py -->',
        '',
        'The pod-to-pod HTTP topology, derived from the `SEAM` / `ROUTES_CALLED`',
        'declarations the runtime route check already reads and from the registry each',
        'server module imports. Measured by importing each image entrypoint in a clean',
        'interpreter. Nothing here is hand-maintained and there is no prefix-to-pod map',
        'written down anywhere, so it cannot drift from the registries.',
        '',
        '## The graph',
        '',
        'Edges are labelled with the seam and the number of routes the caller claims',
        'on it.',
        '',
    ]
    lines += _mermaid(rows)
    lines += [
        '',
        '**A seam is a route shape, not a pod.** Four of the five are served by exactly',
        'one pod, but `queue_worker` is an abstract base subclassed twice -- at',
        '`/downloads` on the downloader and `/search/ytmusic` on the search pod -- so',
        '`discord-bot` has two separate dependencies there, not one. The search pod also',
        'answers two different seams, `media_search` and `queue_worker`, behind one',
        'composite app.',
        '',
        '## Edges',
        '',
        '| caller | peer | seam | prefix | client | routes |',
        '|---|---|---|---|---|---|',
    ]
    for row in rows:
        prefix = f'`{row["prefix"]}`' if row['prefix'] else '—'
        peer = f'`{row["peer"]}`' if row['peer'] else '**unresolved**'
        lines.append(f'| `{row["caller"]}` | {peer} | {row["seam"]} | {prefix} | '
                     f'`{row["client"]}` | {row["routes"]} |')

    lines += ['', '## Seams', '',
              '| seam | served by | called by | routes |', '|---|---|---|---|']
    seams = {}
    for image, clients in topology.items():
        for client in clients.values():
            seams.setdefault(client['seam'], {'images': set(), 'routes': set()})
            seams[client['seam']]['images'].add(image)
            seams[client['seam']]['routes'].update(client['routes'])
    for seam in sorted(seams):
        servers = ', '.join(f'`{i}`' for i in sorted(serving.get(seam, {})))
        callers = ', '.join(f'`{i}`' for i in sorted(seams[seam]['images']))
        lines.append(f'| {seam} | {servers or "—"} | {callers} | '
                     f'{len(seams[seam]["routes"])} |')

    silent = sorted(image for image, clients in topology.items() if not clients)
    lines += ['', '## Images that call no seam', '']
    if silent:
        lines.append(', '.join(f'`{i}`' for i in silent) + '.')
        lines.append('')
        lines.append('Serve-only pods. An image appearing here that should be calling '
                     'something is the failure this doc makes visible.')
    else:
        lines.append('None — every image calls at least one seam.')
    return '\n'.join(lines) + '\n'
