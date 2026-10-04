'''
The import roots this repo has today, and the ones criterion 8 moves to.

Criterion 7 kept `discord_bot` as the single import root and nested the layout
inside it -- `discord_bot/core/`, `discord_bot/seams/<seam>/`,
`discord_bot/services/<pod>/`. That is what made its step 2 free: every
path-keyed thing in the repo still matched a `discord_bot/` prefix, so a
half-moved tree needed no second recognizer.

Criterion 8 changes the root, and that free pass ends. Each folder becomes its
own top-level package so each can be a separately published distribution:

    discord_bot/core/utils/otel.py            ->  discord_core/utils/otel.py
    discord_bot/seams/dispatch/clients/x.py   ->  discord_seam_dispatch/clients/x.py
    discord_bot/services/bot/cogs/music.py    ->  discord_gateway/cogs/music.py

WHY THIS FILE EXISTS AT ALL
---------------------------
A path-keyed reader that does not recognise a root contributes NO images for
it, so a moved file builds nothing -- silently. That is the under-build
criterion 3 exists to prevent, arriving through the front door rather than the
back. Every such reader now asks here instead of spelling `discord_bot/` out,
and BOTH spellings are live for the length of the migration.

It imports nothing but `pathlib`. `_affected_images` runs in ci.yml's `changes`
job, which has no installed package and no dependencies, and must not import
`_image_deps` (which shells out to a real interpreter per entrypoint). A table
both can read has to cost nothing to import.

NORMALISE TOWARD THE TARGET, NOT AWAY FROM IT
---------------------------------------------
`canonical()` maps either spelling onto the criterion 8 one, so declared
constants like `ALL_SIX` are written in their FINAL form now and the tree is
what moves to meet them. The other direction would work equally well today and
leave a rewrite plus a direction flip to do at the end. Here the end state is
already written, and finishing the migration means deleting `LEGACY_ROOT` and
the branch that handles it.
'''
from pathlib import Path

#: The criterion 7 root, still the only one on disk. Everything below is the
#: target; nothing has moved yet.
LEGACY_ROOT = 'discord_bot'

#: The folder names inside the legacy root, which become the roots below.
CORE_DIR = 'core'
SEAMS_DIR = 'seams'
SERVICES_DIR = 'services'

CORE_ROOT = 'discord_core'

#: There is no seam-root table any more. `discord-seam-<name>` distributions
#: existed while every seam was its own top-level package; all five folded
#: into `discord_core` (discord-bot #1000 through #1004, dispatch last), and
#: criterion 8's DECIDED shape is now two tiers -- pods and core -- not
#: pods/core/seams. See per-image-code-split.md criterion 8 for the tradeoff
#: record. `SEAMS_DIR` stays declared below: `discord_bot/seams/<name>/` is
#: still a legacy-spelling SHAPE `home_of()` recognises, a distinct fact from
#: whether any seam is currently a separate package. Whether that legacy
#: recognition itself is still worth keeping -- `discord_bot/` has not
#: existed on disk since #997 -- is a separate, not-yet-made decision; see
#: per-image-code-split.md.
#:
#: Service folder -> distribution import root. `bot` becomes `discord_gateway`:
#: the pod is the Discord gateway connection, the whole cog surface and the HTTP
#: client hub that fans out to the other five. Renaming the PACKAGE is internal.
#: The IMAGE and the console script were renamed from `discord-bot` to
#: `discord-gateway` afterwards, which meant a new OCI repo path, new docker-apps
#: manifests and a cutover.
SERVICE_ROOTS = {
    'gateway': 'discord_gateway',
    'broker': 'discord_broker',
    'db': 'discord_db',
    'dispatcher': 'discord_dispatcher',
    'downloader': 'discord_downloader',
    'search': 'discord_search',
}

#: Every legal top-level import root, both spellings at once.
#:
#: Checked for EQUALITY against the packages actually on disk, the same idiom as
#: ALL_SIX and IMAGE_IMPORTS. That equality is what keeps the silent under-build
#: shut: a root that appears in the tree without being declared here is not
#: recognised by `module_for`, so files under it would contribute no images --
#: and the test fails instead.
ROOTS = frozenset({LEGACY_ROOT, CORE_ROOT, *SERVICE_ROOTS.values()})

#: Target root -> the (kind, name) home it stands for. The legacy spellings are
#: handled by reading the folder out of the dotted path instead; see `home_of`.
_TARGET_HOMES = {
    CORE_ROOT: (CORE_DIR, None),
    **{root: (SERVICES_DIR, name) for name, root in SERVICE_ROOTS.items()},
}

#: Prefixes for matching a module name in either spelling. Trailing dots, so a
#: BARE root is not matched -- callers that want the root itself say so.
MODULE_PREFIXES = tuple(sorted(root + '.' for root in ROOTS))


def canonical(module: str) -> str:
    '''Any module name, in either spelling, as its criterion 8 spelling.

    `discord_bot.core.utils.otel` -> `discord_core.utils.otel`. Already-moved
    names and names with no target (the bare legacy root, the `seams` and
    `services` aggregating packages, which do not survive the move) pass
    through unchanged.
    '''
    parts = module.split('.')
    if parts[0] != LEGACY_ROOT or len(parts) < 2:
        return module
    if parts[1] == CORE_DIR:
        return '.'.join([CORE_ROOT, *parts[2:]])
    if len(parts) >= 3 and parts[1] == SERVICES_DIR and parts[2] in SERVICE_ROOTS:
        return '.'.join([SERVICE_ROOTS[parts[2]], *parts[3:]])
    return module


def home_of(module: str) -> tuple | None:
    '''The folder a module lives in -- `('core', None)`, `('seams', 'database')`.

    Reads the dotted path in EITHER spelling, so this stays a fact about the
    tree rather than about the closure, which is what lets the two be compared.
    None for a module with no home: the bare legacy root, and anything outside
    a declared root.
    '''
    parts = module.split('.')
    target = _TARGET_HOMES.get(parts[0])
    if target is not None:
        return target
    if parts[0] != LEGACY_ROOT:
        return None
    if len(parts) >= 2 and parts[1] == CORE_DIR:
        return (CORE_DIR, None)
    if len(parts) >= 3 and parts[1] in (SEAMS_DIR, SERVICES_DIR):
        return (parts[1], parts[2])
    return None


def root_of(path: str) -> str | None:
    '''The declared import root a repo-relative path sits under, or None.'''
    root = path.split('/')[0]
    return root if root in ROOTS and root != path else None


def folder_of(module: str) -> str | None:
    '''The on-disk folder a module lives in, in whichever spelling it uses.

    `discord_bot.core.utils.otel` -> `discord_bot/core`, and
    `discord_core.utils.otel` -> `discord_core`. Diagnostics name the folder a
    reader would actually go looking in, which stops being one fixed prefix the
    moment the first package is hoisted.
    '''
    home = home_of(module)
    if home is None:
        return None
    root = module.split('.')[0]
    if root != LEGACY_ROOT:
        return root
    kind, name = home
    return f'{LEGACY_ROOT}/{kind}' if name is None else f'{LEGACY_ROOT}/{kind}/{name}'


def package_dirs(repo_root: Path) -> list:
    """Every declared import root that actually exists, as a directory.

    The suite is full of scans written as `REPO_ROOT / 'discord_bot'` -- the
    orphan guard, the client-validation scan, the metric-ownership scan. Each
    one is a path-keyed reader that nobody thinks of as one, and hoisting a
    package makes them cover strictly less without failing. Two of the three
    would have gone on passing over a smaller tree; the third only breaks
    loudly by luck, because the single file it locates moved out from under it.

    Scans go through here so a new root is covered the moment it exists, rather
    than the next time somebody remembers these exist.
    """
    return [repo_root / root for root in sorted(ROOTS)
            if (repo_root / root / '__init__.py').is_file()]


#: The name every package's own test tree is nested under, as of criterion 8
#: step 5. `discord_core/tests/`, `discord_gateway/tests/`, etc. are real,
#: `__init__.py`-bearing packages -- the same convention the old central
#: `tests/` used -- so a plain `rglob('*.py')` finds them as readily as any
#: production module. Callers here answer "what does this repo SHIP", and
#: test code is exactly what a Dockerfile's COPY line and each pyproject.toml's
#: `packages.find` exclude both already agree does not ship.
TESTS_DIR = 'tests'


def source_files(repo_root: Path) -> list:
    """Every first-party `.py` file that ships, across every root that exists.

    Excludes each root's own `tests/` subtree -- see `TESTS_DIR` -- so a test
    file does not have to be reachable from a real entrypoint the way a
    shipped module does. `test_the_declared_roots_are_exactly_the_packages_on_disk`
    and the Dockerfiles are the check that test code does NOT ship; this is
    the check that everything else does.
    """
    return sorted(path for directory in package_dirs(repo_root)
                  for path in directory.rglob('*.py')
                  if TESTS_DIR not in path.relative_to(directory).parts)


def dirs_for(repo_root: Path, kind: str, name: str | None = None) -> list:
    """Directories holding one home, in whichever spelling is on disk.

    `dirs_for(root, 'core')` is `discord_core/` once it is hoisted and
    `discord_bot/core/` before that. `dirs_for(root, 'seams')` is a LIST rather
    than one path, because criterion 8 gives each seam its own root and the
    `seams/` parent does not survive the move.

    Rules keyed on a folder go through here for the same reason scans go through
    `package_dirs`: this project has twice shipped a rule that stopped matching
    the moment the thing it guards moved, and both times it passed rather than
    failed.
    """
    targets = {CORE_DIR: [CORE_ROOT], SERVICES_DIR: sorted(SERVICE_ROOTS.values())}
    if name is not None:
        lookup = {SERVICES_DIR: SERVICE_ROOTS}.get(kind, {})
        targets = {kind: [lookup[name]]} if name in lookup else {kind: []}
    found = [repo_root / root for root in targets.get(kind, [])
             if (repo_root / root / '__init__.py').is_file()]
    legacy = repo_root / LEGACY_ROOT / kind
    if name is not None:
        legacy = legacy / name
    if legacy.is_dir():
        found.append(legacy)
    return found


def module_prefixes_for(kind: str, name: str | None = None) -> tuple:
    """Dotted prefixes that name one home, in both spellings.

    The companion to `dirs_for`: a rule that finds files by folder usually also
    has to recognise an IMPORT of that folder, and the two spellings differ.
    """
    roots = {CORE_DIR: [CORE_ROOT],
             SERVICES_DIR: sorted(SERVICE_ROOTS.values())}.get(kind, [])
    if name is not None:
        lookup = {SERVICES_DIR: SERVICE_ROOTS}.get(kind, {})
        roots = [lookup[name]] if name in lookup else []
    legacy = f'{LEGACY_ROOT}.{kind}' + (f'.{name}' if name else '')
    return tuple(sorted(roots + [legacy]))
