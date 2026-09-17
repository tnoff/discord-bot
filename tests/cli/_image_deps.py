'''
Measure what each published image actually imports.

Shared by the import-boundary tests and by the generated ownership table in
docs/image-dependencies.md, so both come from one measurement rather than two
hand-maintained copies that agree only while someone keeps them in step.

Everything runs in a subprocess: the test suite has already imported the cogs,
so with the module cache pre-poisoned nothing here would be observable in-process.
'''
import functools
import json
import subprocess  # nosec B404 - fixed argv, no shell, test-only import probe
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
OWNERSHIP_DOC = REPO_ROOT / 'docs' / 'image-dependencies.md'
CLOSURE_DOC = REPO_ROOT / 'docs' / 'image-closure.json'

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
    'discord_bot.cli.dispatcher': frozenset({'discord'}),
    # The S3 checkout (boto3); dappertable renders bundles. sqlalchemy left with
    # the MR 4a cutover: the video-cache CATALOG moved to the db pod and is
    # reached over HTTP, while the OBJECTS stayed here, which is why boto3 did
    # not follow it out.
    'discord_bot.cli.broker': frozenset({'boto3', 'dappertable'}),
    # Downloads (yt_dlp) and uploads finished media (boto3).
    'discord_bot.cli.downloader': frozenset({'yt_dlp', 'boto3'}),
    # Thin HTTP clients, plus the two provider SDKs it now owns outright.
    'discord_bot.cli.search': frozenset({'ytmusicapi', 'spotipy', 'googleapiclient'}),
    # Owns the schema and the engine. dappertable is not a leak and not a
    # copy-paste from the broker: PlaylistClient shortens playlist names with
    # shorten_string, so the pod serving those routes imports it. No boto3 --
    # VideoCacheClient is a pure catalog, which is what made it movable.
    #
    # alembic arrived with the migration runner (cli/_lib/migrations.py). Only
    # this image installs it and only this image ships the revisions, so this is
    # the one declaration that may name it.
    'discord_bot.cli.database': frozenset({'sqlalchemy', 'alembic', 'dappertable'}),
}

IMAGE_NAMES = {
    'discord_bot.cli.bot': 'discord-bot',
    'discord_bot.cli.dispatcher': 'discord-dispatcher',
    'discord_bot.cli.broker': 'discord-broker',
    'discord_bot.cli.downloader': 'discord-downloader',
    'discord_bot.cli.search': 'discord-search',
    'discord_bot.cli.database': 'discord-db',
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
    'discord_bot.cli.dispatcher': 'docker/Dockerfile.dispatcher',
    'discord_bot.cli.broker': 'docker/Dockerfile.broker',
    'discord_bot.cli.downloader': 'docker/Dockerfile.downloader',
    'discord_bot.cli.search': 'docker/Dockerfile.search',
    'discord_bot.cli.database': 'docker/Dockerfile.db',
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
