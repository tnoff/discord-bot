'''
Measure what each published image actually imports.

Shared by the import-boundary enforcement tests (what each image is and is not
allowed to reach) and by the generated tests/cli/image-closure.json, so both
come from one measurement rather than two hand-maintained copies that agree
only while someone keeps them in step.

Everything runs in a subprocess: the test suite has already imported the cogs,
so with the module cache pre-poisoned nothing here would be observable in-process.
'''
import ast
import functools
import json
import subprocess  # nosec B404 - fixed argv, no shell, test-only import probe
import sys
from pathlib import Path

from tests.cli._roots import (
    CORE_DIR, LEGACY_ROOT, MODULE_PREFIXES, ROOTS, SEAMS_DIR, SERVICES_DIR,
    folder_of, home_of,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
CLOSURE_DOC = REPO_ROOT / 'tests' / 'cli' / 'image-closure.json'

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
    'discord_gateway.cli.bot': frozenset({
        'discord', 'boto3', 'bs4', 'dappertable',
    }),
    # The strictest image. discord is here on its own merits, not by accident:
    # workers/message_dispatcher sends and edits real messages.
    'discord_dispatcher.cli.dispatcher': frozenset({'discord'}),
    # The S3 checkout (boto3); dappertable renders bundles. sqlalchemy left with
    # the MR 4a cutover: the video-cache CATALOG moved to the db pod and is
    # reached over HTTP, while the OBJECTS stayed here, which is why boto3 did
    # not follow it out.
    'discord_broker.cli.broker': frozenset({'boto3', 'dappertable'}),
    # Downloads (yt_dlp) and uploads finished media (boto3).
    'discord_downloader.cli.downloader': frozenset({'yt_dlp', 'boto3'}),
    # Thin HTTP clients, plus the two provider SDKs it now owns outright.
    'discord_search.cli.search': frozenset({'ytmusicapi', 'spotipy', 'googleapiclient'}),
    # Owns the schema and the engine. dappertable is not a leak and not a
    # copy-paste from the broker: PlaylistClient shortens playlist names with
    # shorten_string, so the pod serving those routes imports it. No boto3 --
    # VideoCacheClient is a pure catalog, which is what made it movable.
    #
    # alembic arrived with the migration runner (cli/_lib/migrations.py). Only
    # this image installs it and only this image ships the revisions, so this is
    # the one declaration that may name it.
    'discord_db.cli.database': frozenset({'sqlalchemy', 'alembic', 'dappertable'}),
}

IMAGE_NAMES = {
    'discord_gateway.cli.bot': 'discord-bot',
    'discord_dispatcher.cli.dispatcher': 'discord-dispatcher',
    'discord_broker.cli.broker': 'discord-broker',
    'discord_downloader.cli.downloader': 'discord-downloader',
    'discord_search.cli.search': 'discord-search',
    'discord_db.cli.database': 'discord-db',
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
    'discord_gateway.cli.bot': 'docker/Dockerfile.gateway',
    'discord_dispatcher.cli.dispatcher': 'docker/Dockerfile.dispatcher',
    'discord_broker.cli.broker': 'docker/Dockerfile.broker',
    'discord_downloader.cli.downloader': 'docker/Dockerfile.downloader',
    'discord_search.cli.search': 'docker/Dockerfile.search',
    'discord_db.cli.database': 'docker/Dockerfile.db',
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
        f'roots = {MODULE_PREFIXES!r}; '
        'print(json.dumps({'
        '"packages": sorted(m for m in vocab if m in sys.modules), '
        '"modules": sorted(m for m in sys.modules if m.startswith(roots))'
        '}))'
    )


def measure(entrypoint: str) -> dict:
    '''Import entrypoint in a clean interpreter and report what it pulled in.'''
    return json.loads(_measure_cached(entrypoint))


def declared_scripts() -> dict:
    '''Console script name -> the entrypoint it targets, read from EVERY
    package's own pyproject.toml.

    Criterion 8 step 5 gave each package its own pyproject.toml with its own
    `[project.scripts]`; there is no longer one file to read this from.
    '''
    import tomllib  # pylint: disable=import-outside-toplevel
    scripts = {}
    for root in ROOTS:
        candidate = REPO_ROOT / root / 'pyproject.toml'
        if not candidate.is_file():
            continue
        declared = tomllib.loads(candidate.read_text(encoding='utf-8'))
        scripts.update(declared.get('project', {}).get('scripts', {}))
    return scripts


def declared_dependencies(root: str) -> list:
    '''The `dependencies` list `root`'s own pyproject.toml declares, or [].'''
    import tomllib  # pylint: disable=import-outside-toplevel
    candidate = REPO_ROOT / root / 'pyproject.toml'
    if not candidate.is_file():
        return []
    declared = tomllib.loads(candidate.read_text(encoding='utf-8'))
    return declared.get('project', {}).get('dependencies', [])


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
    # No `extra` field any more: criterion 8 step 5 gave each package its own
    # `dependencies` instead of a per-image extra on one pyproject.toml, so the
    # field held nothing but a second copy of `image` (`extra: "broker"` next
    # to `image: "broker"`) and _affected_images.py's `pyproject.toml` handling
    # is what actually read it. That handling now derives the same answer from
    # `modules` instead -- see `root_of()` there -- so the duplicate is gone
    # rather than carried forward unread.
    measured = {ep: measure(ep) for ep in IMAGE_IMPORTS}
    images = []
    for ep in IMAGE_IMPORTS:
        images.append({
            'image': short_name(ep),
            'entrypoint': ep,
            'dockerfile': IMAGE_DOCKERFILES[ep],
            'modules': sorted(set(measured[ep]['modules']) | _roots_reached(measured[ep])),
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

# CRITERION 7 kept `discord_bot` as the single import root and put the layout
# INSIDE it, rather than hoisting libs/ and services/ to the repo root. That is
# what made the move invisible to everything keyed on a path: the CI filter's
# `discord_bot/` prefix, every Dockerfile COPY, and setuptools' packages.find
# all kept working through a half-moved tree, and no part of the package became
# an __init__-less directory that setuptools would treat as a namespace package
# -- the trap pyproject.toml already documents for alembic/.
#
# CRITERION 8 hoists them after all, so each folder can be published on its own,
# and buys back the namespace-package trap by giving each one a DISTINCT root
# rather than making `discord_bot` a namespace. The roots live in _roots.py,
# which `_affected_images` can also import; this name survives only as the
# legacy spelling used in diagnostics.
PACKAGE = LEGACY_ROOT


def _roots_reached(measurement: dict) -> set:
    """The bare import roots an image's measured modules sit under.

    This used to be a literal `{'discord_bot'}` unioned into every image, so a
    change to the root `__init__.py` rebuilt all six. That was right while there
    was one root. Criterion 8 gives each folder its own, and unioning them all
    would rebuild every image on a change to `discord_gateway/__init__.py` -- a
    file only the bot has. Deriving it keeps the property and drops the special
    case: every image reaches modules under `discord_bot` today, so all six
    still claim it, and a per-pod root will be claimed by its pod alone.

    The probe filters on prefixes with trailing dots, so bare roots never appear
    in the measurement and have to be added back here.
    """
    return {module.split('.')[0] for module in measurement['modules']} & ROOTS


def _owner_sets(measured):
    '''Map every measured module to the set of images that reach it.'''
    module_sets = {short_name(ep): set(m['modules']) for ep, m in measured.items()}
    owners = {}
    for image, modules in module_sets.items():
        for module in modules:
            # A bare root with no home is excluded deliberately: `discord_bot`
            # is the umbrella package itself, and under criterion 7's layout it
            # has no folder to be assigned. A bare criterion 8 root DOES have
            # one -- `discord_core` is the core -- so it is checked normally and
            # exempted, if at all, for being a codeless init like any other.
            if home_of(module) is None and module in ROOTS:
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


# The DERIVED placement path used to live above this line: compute a module's
# home from its closure. It was deleted on 2026-09-22 with the last 25 modules,
# because a layout computed FROM what imports what agrees with what imports what
# by construction, and once every module has a declared home there is nothing
# left for it to decide. What follows reads the home a module actually has --
# its path on disk -- and checks the closure against it. That check can fail,
# which is the entire point of it.
# ---------------------------------------------------------------------------

# Top-level packages under discord_bot/ that criterion 7 has not split yet.
# DECLARED, not derived, and checked for equality rather than containment: a
# derived version of this set is the same tautology step 6 exists to escape,
# and containment would let the set quietly stop shrinking.
#
# Equality buys both directions. A brand new flat package -- the most likely way
# the split gets walked back, one plausible file at a time -- is not in here and
# fails. A package that has been fully emptied by a move is still in here and
# also fails, so finishing a package forces the list to shrink rather than
# leaving a spent entry behind to be read as work remaining.
#
# The granularity is the package, not the module, and that is deliberate. A new
# module under utils/ is honest: utils/ genuinely has no home yet, and making
# every such PR edit a list here would be friction with no signal in it. What is
# NOT honest is a new top-level package, because that is the split going
# backwards.
#: EMPTY as of 2026-09-22 -- all 25 were placed and the seven flat packages
#: they lived in are retired. Kept as an equality-checked constant rather
#: than deleted: a NEW top-level package under discord_bot/ now fails the
#: suite instead of quietly reopening the flat tree.
NOT_YET_SPLIT = frozenset()


#: Every module reached by ALL SIX images, excluding codeless package inits.
#:
#: This is the guard that used to be implicit in "core means all six". Homing
#: the last 25 modules on 2026-09-22 relaxed core to mean SHARED, which is the
#: right PLACEMENT rule -- but the thing the old rule actually protected was the
#: FANOUT: a module every image carries is one whose every change rebuilds every
#: image, and criterion 7 named "a core that grows a tier-defining dependency"
#: as the failure to watch for.
#:
#: WRITTEN IN THE CRITERION 8 SPELLING, deliberately, while the tree is still
#: in criterion 7's. The measurement is canonicalised before it is compared, so
#: a module's identity here survives the move that changes its path -- which is
#: the thing this set is about. The other direction works equally well today and
#: leaves a rewrite plus a direction flip to do at the end; this way the end
#: state is already written and finishing the migration deletes code.
#:
#: Checked for EQUALITY, so it fails in both directions. A module ARRIVING is a
#: new all-six dependency and should be argued for. One LEAVING is usually good
#: news -- but drifting in unnoticed is exactly how `utils/otel.py` became a
#: six-image rebuild trigger, and it took criterion 5 to get it back out.
ALL_SIX = frozenset({
    'discord_core.cli._lib.common',
    'discord_core.cogs.music_helpers.common',
    'discord_core.cogs.schema',
    'discord_core.exceptions',
    'discord_core.routes.contract',
    'discord_core.routes.route',
    'discord_core.servers.health_server_base',
    'discord_core.types.media_request',
    'discord_core.types.search',
    'discord_core.utils.common',
    'discord_core.utils.discord_utils',
    'discord_core.utils.gc_census',
    'discord_core.utils.loop_health',
    'discord_core.utils.memory_profiler',
    'discord_core.utils.otel',
    'discord_core.utils.process_metrics',
})


def declared_home(module: str) -> tuple | None:
    """The folder a module actually lives in: `('core', None)`, `('seams', 'database')`.

    Read from the dotted path, so this is a fact about the tree rather than about
    the closure -- which is what lets the two be compared at all. Delegates to
    `_roots.home_of`, which answers for BOTH the criterion 7 spelling
    (`discord_bot.core.x`) and the criterion 8 one (`discord_core.x`), so the
    layout rules keep working through a half-moved tree.
    """
    return home_of(module)


def measured_owners() -> dict:
    """Every measured module mapped to the set of images that reach it."""
    return _owner_sets({ep: measure(ep) for ep in IMAGE_IMPORTS})


def codeless_inits(owners) -> set:
    """The package `__init__.py` files in `owners` that declare no code.

    Exempt from the declared check for the same reason step 3 refused to place
    them: an init's fanout is its children's, unioned, not its own. A seam init
    can read as all-six with no child reaching all six -- two children reached by
    complementary halves are enough -- so checking it would report a violation
    that exists in no module. Every child IS checked individually, so a real
    misplacement always surfaces on the module that declares the code.
    """
    return {m for m in owners if _is_scaffolding(m)}


def layout_violations(owners, exempt=()) -> list:
    """Modules whose declared folder contradicts the closure that reaches them.

    Pure in `owners` so it can be exercised against a constructed map as well as
    a measured one -- see test_a_misplaced_module_is_actually_caught, which is
    the guard against this returning [] because it stopped looking.
    """
    total = len(IMAGE_NAMES)
    exempt = set(exempt)
    violations = []
    for module in sorted(owners):
        if module in exempt:
            continue
        home = declared_home(module)
        if home is None:
            continue
        kind, name = home
        images = owners[module]
        if kind == CORE_DIR and len(images) < 2:
            violations.append(
                f'{module} is in {folder_of(module)}/ but only {sorted(images)} '
                f'reaches it. The core is SHARED code -- a module exactly one image '
                f'reaches is that pod\'s private code and belongs in its service '
                f'folder. Note core no longer means all six: ALL_SIX is what guards '
                f'the fanout this rule used to.'
            )
        elif kind == SERVICES_DIR and images != {name}:
            intruders = sorted(images - {name})
            violations.append(
                f'{module} is in {folder_of(module)}/ but {intruders} '
                f'reach it too. A service folder is that pod\'s private code. Either '
                f'an in-process tier came back, or something annotated against a '
                f'concrete type where the client Protocol was meant -- which is how '
                f'interfaces/broker_protocols kept reaching music_player. Move the '
                f'shared part to a seam; do not widen the folder.'
            )
        elif kind == SEAMS_DIR and (len(images) < 2 or len(images) == total):
            reached = sorted(images)
            violations.append(
                f'{module} is in {folder_of(module)}/ but is reached by '
                f'{reached}. A seam carries a contract between tiers: reached by '
                f'one, it is that service\'s private code; reached by all {total}, '
                f'nothing about it is tier-specific and it belongs in the core.'
            )
    return violations
