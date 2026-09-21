'''
Measure what each published image actually imports.

Shared by the import-boundary tests and by the generated ownership table in
docs/image-dependencies.md, so both come from one measurement rather than two
hand-maintained copies that agree only while someone keeps them in step.

Everything runs in a subprocess: the test suite has already imported the cogs,
so with the module cache pre-poisoned nothing here would be observable in-process.
'''
import ast
import functools
import json
import subprocess  # nosec B404 - fixed argv, no shell, test-only import probe
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
OWNERSHIP_DOC = REPO_ROOT / 'docs' / 'image-dependencies.md'
CLOSURE_DOC = REPO_ROOT / 'docs' / 'image-closure.json'
LAYOUT_DOC = REPO_ROOT / 'docs' / 'module-layout.md'

# The tier-defining third-party packages: heavy, tier-specific, or both. Base
# dependencies (aiohttp, pydantic, redis, the otel stack) are deliberately out of
# scope — every image has those by construction, so asserting them says nothing.
#
# A package leaves this vocabulary only when it leaves the project entirely.
# moviepy is still here despite being installed by no extra: the vocabulary is
# what the images are checked AGAINST, so keeping a dropped package listed is how
# re-adding it meets an explicit refusal rather than silence.
VOCABULARY = (
    'discord',
    'yt_dlp',
    'moviepy',
    'ytmusicapi',
    'spotipy',
    'googleapiclient',
    'sqlalchemy',
    # Tracked as of the migration runner. The db pod is the only process that
    # may reach alembic, and this is what stops the CLI arriving anywhere else
    # through an import chain -- the forbidden set is VOCABULARY minus each
    # image's declaration, so nothing has to be listed as banned per image.
    'alembic',
    'boto3',
    'bs4',
    'dappertable',
)

# Image entrypoint -> the vocabulary packages it imports. ONE declaration per
# image: the forbidden set is derived as VOCABULARY minus this, so the two can
# never drift apart. Declaring what an image DOES import (rather than only what
# it must not) also catches the opposite failure — an extra that has gone
# over-broad, where the image installs something nothing reaches any more. That
# is how yt_dlp sat in [bot] after the download dual path was collapsed.
IMAGE_IMPORTS = {
    # bs4 is the urban cog's and stays; spotipy and googleapiclient left with the
    # media_search cutover, and the forbidden set derived from VOCABULARY is now
    # what stops them coming back through an import chain.
    'discord_bot.cli.bot': frozenset({
        'discord', 'boto3', 'bs4', 'dappertable',
    }),
    # The strictest image. discord is here on its own merits, not by accident:
    # workers/message_dispatcher sends and edits real messages.
    'discord_bot.services.dispatcher.cli.dispatcher': frozenset({'discord'}),
    # The S3 checkout (boto3); dappertable renders bundles. sqlalchemy left with
    # the MR 4a cutover: the video-cache CATALOG moved to the db pod and is
    # reached over HTTP, while the OBJECTS stayed here, which is why boto3 did
    # not follow it out.
    'discord_bot.services.broker.cli.broker': frozenset({'boto3', 'dappertable'}),
    # Downloads (yt_dlp) and uploads finished media (boto3).
    'discord_bot.services.downloader.cli.downloader': frozenset({'yt_dlp', 'boto3'}),
    # Thin HTTP clients, plus the two provider SDKs it now owns outright.
    'discord_bot.services.search.cli.search': frozenset({'ytmusicapi', 'spotipy', 'googleapiclient'}),
    # Owns the schema and the engine. dappertable is not a leak and not a
    # copy-paste from the broker: PlaylistClient shortens playlist names with
    # shorten_string, so the pod serving those routes imports it. No boto3 --
    # VideoCacheClient is a pure catalog, which is what made it movable.
    #
    # alembic arrived with the migration runner (cli/_lib/migrations.py). Only
    # this image installs it and only this image ships the revisions, so this is
    # the one declaration that may name it.
    'discord_bot.services.db.cli.database': frozenset({'sqlalchemy', 'alembic', 'dappertable'}),
}

IMAGE_NAMES = {
    'discord_bot.cli.bot': 'discord-bot',
    'discord_bot.services.dispatcher.cli.dispatcher': 'discord-dispatcher',
    'discord_bot.services.broker.cli.broker': 'discord-broker',
    'discord_bot.services.downloader.cli.downloader': 'discord-downloader',
    'discord_bot.services.search.cli.search': 'discord-search',
    'discord_bot.services.db.cli.database': 'discord-db',
}

# The Dockerfile that builds each image. Declared here rather than in the CI
# matrix because the matrix is now generated from this: ci.yml used to carry its
# own copy of image -> dockerfile, which made it a second place the set of images
# was written down. The 2026-09-04 finding is what that costs -- discord-db was
# added to five of the six places and missed in release.yml, and built green
# while shipping nothing for four days.
#
# test_every_dockerfile_exists keeps these honest against the filesystem.
IMAGE_DOCKERFILES = {
    'discord_bot.cli.bot': 'docker/Dockerfile',
    'discord_bot.services.dispatcher.cli.dispatcher': 'docker/Dockerfile.dispatcher',
    'discord_bot.services.broker.cli.broker': 'docker/Dockerfile.broker',
    'discord_bot.services.downloader.cli.downloader': 'docker/Dockerfile.downloader',
    'discord_bot.services.search.cli.search': 'docker/Dockerfile.search',
    'discord_bot.services.db.cli.database': 'docker/Dockerfile.db',
}


@functools.lru_cache(maxsize=None)
def run_probe(probe: str) -> str:
    '''Run `probe` in a clean interpreter; return its last stdout line.

    Shared with _seam_topology so both generated docs measure the same way and
    the subprocess is configured in one place. Cached because each call is a
    fresh interpreter, ~0.5s a go, and both callers probe the same six
    entrypoints.
    '''
    result = subprocess.run([sys.executable, '-c', probe],  # nosec B603 - fixed argv, no shell
                            capture_output=True, text=True, check=True, cwd=REPO_ROOT)
    return result.stdout.strip().splitlines()[-1]


def _measure_cached(entrypoint: str) -> str:
    '''Raw probe output for one entrypoint's imports.'''
    return run_probe(
        'import importlib, json, sys; '
        f'importlib.import_module({entrypoint!r}); '
        f'vocab = {list(VOCABULARY)!r}; '
        'print(json.dumps({'
        '"packages": sorted(m for m in vocab if m in sys.modules), '
        '"modules": sorted(m for m in sys.modules if m.startswith("discord_bot."))'
        '}))'
    )


def measure(entrypoint: str) -> dict:
    '''Import entrypoint in a clean interpreter and report what it pulled in.'''
    return json.loads(_measure_cached(entrypoint))


def declared_extras() -> set:
    '''The extras pyproject actually defines, for the ownership table.'''
    import tomllib  # pylint: disable=import-outside-toplevel
    pyproject = tomllib.loads((REPO_ROOT / 'pyproject.toml').read_text(encoding='utf-8'))
    return set(pyproject['project']['optional-dependencies'])


def render_table() -> str:
    '''Render the ownership doc from a live measurement.'''
    measured = {ep: measure(ep) for ep in IMAGE_IMPORTS}
    module_sets = {ep: set(m['modules']) for ep, m in measured.items()}
    every = set().union(*module_sets.values())
    shared_by = {m: sum(1 for s in module_sets.values() if m in s) for m in every}

    lines = [
        '# What each image depends on',
        '',
        '<!-- GENERATED by tests/cli/test_import_boundaries.py. Do not edit by hand.',
        '     Regenerate with: UPDATE_IMAGE_DEPS=1 pytest tests/cli/test_import_boundaries.py -->',
        '',
        'Measured by importing each entrypoint in a clean interpreter, not by reading',
        'the code. The packages column is the tier-defining vocabulary only — every',
        'image also gets the base dependencies (aiohttp, pydantic, redis, the OTel',
        'stack), which is why the numbers below are smaller than an image manifest.',
        '',
        '## Tier-defining packages per image',
        '',
        '| image | extra | packages it imports |',
        '|---|---|---|',
    ]
    extras = declared_extras()
    for ep, name in IMAGE_NAMES.items():
        candidate = name.replace('discord-', '')
        # Reported, not assumed: an image with no extra of its own installs the
        # base dependencies only, and saying so is the point of this column.
        extra = f'`[{candidate}]`' if candidate in extras else 'base only'
        pkgs = ', '.join(f'`{p}`' for p in sorted(measured[ep]['packages'])) or '—'
        lines.append(f'| `{name}` | {extra} | {pkgs} |')

    lines += [
        '',
        '## How much of the tree each image loads',
        '',
        '| image | `discord_bot` modules imported | exclusive to it |',
        '|---|---|---|',
    ]
    for ep, name in IMAGE_NAMES.items():
        excl = {m for m in module_sets[ep] if shared_by[m] == 1}
        lines.append(f'| `{name}` | {len(module_sets[ep])} | {len(excl)} |')

    total = len(IMAGE_NAMES)
    spread = {n: sum(1 for m, c in shared_by.items() if c == n) for n in range(1, total + 1)}
    lines += [
        '',
        '## Why this is one package and not one per image',
        '',
        f'Modules by how many of the {total} entrypoints import them:',
        '',
        '| imported by | modules |',
        '|---|---|',
    ]
    for n in range(1, total + 1):
        lines.append(f'| {n} of {total} | {spread[n]} |')
    shared_2plus = sum(spread[n] for n in range(2, total + 1))

    # Derived, not written down. The prose used to name which images share which
    # package by hand, and a hand-written list of that shape is wrong the moment
    # an image is added -- `sqlalchemy` read "(bot + broker)" while the db pod was
    # being built to hold it. Anything this paragraph asserts now comes out of the
    # same measurement as the tables above it.
    shared_pkgs = []
    for pkg in sorted(VOCABULARY):
        owners = [name.replace('discord-', '')
                  for ep, name in IMAGE_NAMES.items() if pkg in measured[ep]['packages']]
        if len(owners) > 1:
            shared_pkgs.append(f'`{pkg}` ({" + ".join(owners)})')
    shared_desc = ', '.join(shared_pkgs[:-1]) + f' and {shared_pkgs[-1]}' if shared_pkgs else 'none'

    lines += [
        '',
        f'{shared_2plus} of {len(every)} modules ({shared_2plus * 100 // len(every)}%) are '
        f'imported by two or more entrypoints but not all {total}. Splitting the tree into one',
        'installable distribution per tier would force every one of those into a shared',
        f'`core` distribution — and dependencies follow modules, so {shared_desc}',
        f'would land back on all {total} images. That is strictly worse than the per-image',
        f'extras, which is why this stays one package with {total} per-image extras.',
        '',
    ]
    return '\n'.join(lines)


def short_name(entrypoint: str) -> str:
    '''The name CI uses for an image: `discord-db` -> `db`.'''
    return IMAGE_NAMES[entrypoint].replace('discord-', '')


def render_closure() -> str:
    '''
    Render the per-image closure CI keys its build filter on.

    JSON rather than the markdown ownership table next door because this one is
    read by a program, not a person: ci.yml maps a PR's changed files onto it to
    decide which images to build. The markdown table stays because it answers a
    different question -- "what does this image carry" -- and answering both from
    one file would make each worse.

    The module list is MEASURED, not walked. A static AST walk misses lazy and
    PEP 562 imports, and the gate has to be right about reachability or it skips
    an image that needed building. The one dynamic import in the tree
    (utils/integrations/youtube_music.py) is exactly the case a walk would miss.
    '''
    measured = {ep: measure(ep) for ep in IMAGE_IMPORTS}
    images = []
    for ep in IMAGE_IMPORTS:
        images.append({
            'image': short_name(ep),
            'entrypoint': ep,
            'dockerfile': IMAGE_DOCKERFILES[ep],
            'extra': short_name(ep),
            'modules': sorted(set(measured[ep]['modules']) | {'discord_bot'}),
        })
    images.sort(key=lambda i: i['image'])
    doc = {
        '_generated_by': 'tests/cli/test_import_boundaries.py '
                         '(UPDATE_IMAGE_DEPS=1 pytest tests/cli/test_import_boundaries.py)',
        '_do_not_edit': 'Regenerate rather than editing. ci.yml reads this to pick which images to build.',
        'images': images,
    }
    return json.dumps(doc, indent=2, sort_keys=False) + '\n'


# The prefix whose leaf name gives a seam its name. A shared group that contains
# exactly one of these is a seam with a contract already written down; the route
# module IS the contract, so the name comes out of the tree rather than out of a
# hand-written map that would go stale the first time a seam is renamed.
# A seam is named by the route module in it, found STRUCTURALLY rather than by a
# fixed prefix. The first version of this matched `discord_bot.routes.` literally,
# which stopped matching the moment a seam moved -- `routes/database.py` becomes
# `seams/database/routes/database.py`, and the rule written to guide the move did
# not survive it. Matching on the `routes` package wherever it sits works before
# and after, so the doc keeps naming a seam through its own migration.
ROUTE_PACKAGE = 'routes'


def route_leaf(module: str) -> str | None:
    """`...routes.database` -> `database`; None if the module is not a route."""
    parts = module.split('.')
    return parts[-1] if len(parts) > 1 and parts[-2] == ROUTE_PACKAGE else None

# The split keeps `discord_bot` as the single import root and puts the layout
# INSIDE it, rather than hoisting libs/ and services/ to the repo root. That is
# what makes the move invisible to everything keyed on a path: the CI filter's
# `discord_bot/` prefix, every Dockerfile COPY, and setuptools' packages.find
# all keep working through a half-moved tree, and no part of the package becomes
# an __init__-less directory that setuptools would treat as a namespace package
# -- the trap pyproject.toml already documents for alembic/.
PACKAGE = 'discord_bot'


def _owner_sets(measured):
    '''Map every measured module to the set of images that reach it.'''
    module_sets = {short_name(ep): set(m['modules']) for ep, m in measured.items()}
    owners = {}
    for image, modules in module_sets.items():
        for module in modules:
            # The root package is excluded deliberately. render_closure() unions
            # it into every image so a change to __init__.py rebuilds all six,
            # but it is the package root itself -- it has no home to be assigned
            # in a layout that keeps discord_bot/ as the root and nests the
            # classes beneath it.
            if module == 'discord_bot':
                continue
            owners.setdefault(module, set()).add(image)
    return owners


def _is_scaffolding(module: str) -> bool:
    """
    True for a package `__init__.py` that declares no code of its own.

    Such a file has no home to be assigned. `discord_bot/cogs/__init__.py` is
    reached by all six images only because every image imports SOME cog, and it
    cannot move to the core: `discord_bot/cogs/` has to keep existing wherever
    cogs live, and each new home needs its own empty init. Placing these by
    closure is a category error -- they follow their children rather than having
    a position of their own.

    Docstring-only counts as no code, which is not a technicality here:
    `routes/__init__.py` carries a long docstring whose subject is precisely
    that the file is deliberately empty, and that emptiness is load-bearing for
    the seam fanouts.
    """
    init = REPO_ROOT / (module.replace('.', '/') + '/__init__.py')
    if not init.is_file():
        return False
    body = ast.parse(init.read_text(encoding='utf-8')).body
    if not body:
        return True
    return len(body) == 1 and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant)


def classify_modules():
    '''
    Assign every measured module the folder it would live in, from the closure.

    Three rules, in order, and none of them consults a list of names:

      * reached by every image            -> `discord_bot/core/`
      * reached by exactly one            -> `discord_bot/services/<image>/`
      * the group holds one route module  -> `discord_bot/seams/<route leaf>/`

    Anything left is returned UNPLACED rather than filed somewhere plausible.
    A generated layout that silently invents a home for the modules the rule
    cannot reach would read as a finished answer, and the modules it could not
    place are precisely the ones where the decision has to be made by a person.
    '''
    measured = {ep: measure(ep) for ep in IMAGE_IMPORTS}
    owners = _owner_sets(measured)
    total = len(IMAGE_NAMES)

    groups = {}
    for module, images in owners.items():
        groups.setdefault(frozenset(images), []).append(module)

    scaffolding = sorted(m for m in owners if _is_scaffolding(m))
    scaffold_set = set(scaffolding)
    groups = {images: [m for m in modules if m not in scaffold_set]
              for images, modules in groups.items()}
    groups = {images: modules for images, modules in groups.items() if modules}

    placed, unplaced = {}, {}
    for images, modules in groups.items():
        if len(images) == total:
            home = f'{PACKAGE}/core'
        elif len(images) == 1:
            home = f'{PACKAGE}/services/{next(iter(images))}'
        else:
            routes = [m for m in modules if route_leaf(m)]
            # Exactly one: a group with two route modules names no single seam,
            # and a group with none is a sharing pattern nobody has written a
            # contract for yet. Both are honest UNPLACED answers.
            home = f'{PACKAGE}/seams/{route_leaf(routes[0])}' if len(routes) == 1 else None
        target = placed if home else unplaced
        target[images] = (home, sorted(modules))
    return placed, unplaced, scaffolding


def render_layout() -> str:
    '''
    Render the folder each module would live in under criterion 7's layout.

    Nothing moves. This is the target rendered against today's tree so it can be
    argued with before a single file is touched, and regenerated after each step
    so "where does this file go" stays a measurement.

    Note what this doc CANNOT tell you, because it will read as if it can: while
    homes are derived from the closure, a test comparing the two cannot fail. It
    only gains content at step 6, when a module's folder becomes a declaration
    instead of a restatement of what imports it.
    '''
    placed, unplaced, scaffolding = classify_modules()
    total_placed = sum(len(mods) for _, mods in placed.values())
    total_unplaced = sum(len(mods) for _, mods in unplaced.values())
    every = total_placed + total_unplaced + len(scaffolding)

    lines = [
        '# Where each module would live',
        '',
        '<!-- GENERATED by tests/cli/test_import_boundaries.py. Do not edit by hand.',
        '     Regenerate with: UPDATE_IMAGE_DEPS=1 pytest tests/cli/test_import_boundaries.py -->',
        '',
        'The target layout of criterion 7 in `per-image-code-split`, rendered against',
        'the tree as it stands. **No file has moved.** Homes come from the measured',
        'import closure, by three rules that consult no list of names:',
        '',
        f'`{PACKAGE}` stays the single import root and the layout nests inside it, so',
        'every path-keyed thing in the repo — the CI filter, the Dockerfile `COPY`s,',
        "setuptools' `packages.find` — keeps working unchanged through a half-moved",
        'tree.',
        '',
        '| a module reached by | goes to |',
        '|---|---|',
        f'| all {len(IMAGE_NAMES)} images | `{PACKAGE}/core/` |',
        f'| exactly one image | `{PACKAGE}/services/<image>/` |',
        f'| a group holding one `{ROUTE_PACKAGE}.*` module | `{PACKAGE}/seams/<that route>/` |',
        '',
        'The third rule is why the seam names below are not invented here. A seam is',
        'a contract, the route module *is* the contract, and so the folder takes its',
        'name — rename the route and this doc follows.',
        '',
        f'**{total_placed} of {every} modules place. {total_unplaced} do not**, and they',
        'are listed at the bottom rather than filed somewhere plausible. A further',
        f'**{len(scaffolding)} are package `__init__.py` files that declare no code**;',
        'they have no home of their own and are listed separately.',
        '',
        '## Summary',
        '',
        '| folder | modules | reached by |',
        '|---|---|---|',
    ]
    for images, (home, modules) in sorted(placed.items(), key=lambda kv: kv[1][0]):
        lines.append(f'| `{home}/` | {len(modules)} | {", ".join(sorted(images))} |')
    lines.append(f'| *(unplaced)* | {total_unplaced} | {len(unplaced)} groups, see below |')

    # A seam takes its name from a route, and a route may be named after the pod
    # it talks TO, so the same word can name both. Derived rather than written
    # down, so a future rename that introduces or removes one is reported either
    # way -- a hand-written note here would go quietly wrong.
    seam_prefix, service_prefix = f'{PACKAGE}/seams/', f'{PACKAGE}/services/'
    pods = {short_name(ep) for ep in IMAGE_IMPORTS}
    collisions = sorted(home[len(seam_prefix):] for home, _ in placed.values()
                        if home.startswith(seam_prefix) and home[len(seam_prefix):] in pods)
    if collisions:
        shared = ', '.join(f'`{c}`' for c in collisions)
        lines += [
            '',
            f'**{shared} names both a seam and a pod, and they are not the same thing.**',
            f'`{seam_prefix}<x>/` holds what the other images use to *talk to* that pod —',
            f'client, routes, wire types — and `{service_prefix}<x>/` holds the pod itself.',
            'The two prefixes keep the paths distinct, so nothing reading a path can',
            'confuse them; it is the prose and the review conversation that need the care.',
        ]

    lines += [
        '',
        '## Placed',
        '',
    ]
    for images, (home, modules) in sorted(placed.items(), key=lambda kv: kv[1][0]):
        lines += [f'### `{home}/` — {len(modules)}, reached by {", ".join(sorted(images))}', '']
        lines += [f'- `{m}`' for m in modules]
        lines.append('')

    lines += [
        '## Scaffolding',
        '',
        'Package `__init__.py` files declaring no code. These are reported rather',
        'than placed because they have no position of their own: they exist wherever',
        'their children live. `discord_bot/cogs/__init__.py` is reached by all six',
        'images only because every image imports *some* cog — it cannot move to the',
        'core, because `discord_bot/cogs/` has to keep existing wherever cogs live,',
        'and every new home needs its own empty init. Counting them as core modules',
        'overstated the core by a third.',
        '',
    ]
    lines += [f'- `{m}`' for m in scaffolding]
    lines += [
        '',
        '## Unplaced',
        '',
        'Shared by more than one image but fewer than all, with no single route',
        'module to name them. Each needs a decision, and the decision is not the',
        f"generator's to make. Widening one into `{PACKAGE}/core/` costs the images that",
        'do not reach it; inventing a seam folder claims a contract that has not been',
        'written. The groups are small and several are recognisably the *other half*',
        'of a seam already named above — the implementation side, where the seam',
        'folder holds the client and the wire types.',
        '',
    ]
    for images, (_, modules) in sorted(unplaced.items(),
                                       key=lambda kv: (-len(kv[0]), sorted(kv[0]))):
        lines += [f'### {", ".join(sorted(images))} — {len(modules)}', '']
        lines += [f'- `{m}`' for m in modules]
        lines.append('')
    return '\n'.join(lines)
