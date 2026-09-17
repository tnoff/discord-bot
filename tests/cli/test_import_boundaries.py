'''
Per-image import boundaries — what each published image is allowed to import.

Each entry in ``IMAGE_IMPORTS`` (tests/cli/_image_deps.py) is a contract: importing
that image's entrypoint must pull EXACTLY those tier-defining packages into
``sys.modules``. The check is the enforcement mechanism for the per-image
dependency split (projects/discord-bot-ha-only) — a folder layout cannot prevent
``from discord_bot.utils.integrations.youtube_music import ...``, but this can,
and this is what caught the ytmusicapi leak during the search-pod work before it
could CrashLoop a pod (see reference_slim_pod_import_chain_leak).

Two things make these tests worth their weight:

- **A violation is a pod-start crash, not a test failure.** The slim images
  install only their own extra, so an import that reaches a package the image
  does not ship is an ImportError at startup — discovered in prod, on a rollout.
- **They are the discovery tool for the extras split.** The gap between what an
  image *imports* and what its extra *installs* is what the split acts on.

The assertion is EQUALITY, not absence, and that is deliberate. A forbidden-list
catches a leak — a package arriving where it should not be. It cannot catch the
opposite, an extra that has gone over-broad because the code that needed it was
deleted. That failure is silent, it costs image size rather than uptime, and it
is exactly how yt_dlp stayed in [bot] after the download dual path was collapsed
and how moviepy stayed installed while nothing imported it at all. Declaring what
each image DOES import catches both directions from one list.

There is deliberately no separate "forbidden packages" test. Equality already
implies it: if what an image imports equals what it declares, it cannot have
imported anything in ``VOCABULARY`` that it did not declare. A second test
asserting that would be tautological, and a tautological test is worse than no
test — it reads as coverage while checking nothing.
'''
import json
import os
import re

import pytest

from tests.cli._image_deps import (
    CLOSURE_DOC, IMAGE_DOCKERFILES, IMAGE_IMPORTS, OWNERSHIP_DOC, REPO_ROOT, VOCABULARY,
    measure, render_closure, render_table,
)


@pytest.mark.parametrize('entrypoint', sorted(IMAGE_IMPORTS))
def test_image_imports_exactly_its_declared_packages(entrypoint):
    '''An image imports its declared packages — no more, and no fewer.'''
    declared = IMAGE_IMPORTS[entrypoint]
    imported = set(measure(entrypoint)['packages'])
    leaked = imported - declared
    unused = declared - imported
    assert not leaked, (
        f'{entrypoint} imported {sorted(leaked)}, which it does not declare and its '
        f'image may not install. On a slim image this is an ImportError at pod start, '
        f'not a test failure. Find the chain with: python -c "import {entrypoint}" '
        f'under a tracing __import__ hook, then split the light type out of the heavy '
        f'module — the same move as CheckoutResult, ClearGuildResult and BrokerClient.'
    )
    assert not unused, (
        f'{entrypoint} no longer imports {sorted(unused)}, which it still declares. '
        f'Nothing is broken, but its extra in pyproject is now shipping a package '
        f'nothing reaches — drop it from both, the way yt_dlp and moviepy should have '
        f'been dropped when the code that used them went away.'
    )


def test_vocabulary_covers_every_declaration():
    '''Nothing is declared that the vocabulary does not know about.'''
    declared = set().union(*IMAGE_IMPORTS.values())
    unknown = declared - set(VOCABULARY)
    assert not unknown, (
        f'{sorted(unknown)} declared but missing from VOCABULARY, so the derived '
        f'forbidden set would silently ignore them on every other image.'
    )


def test_extra_names_are_normalised_and_self_references_resolve():
    '''
    Every extra name is PEP 685 normalised, and every self-reference names a real one.

    This is a silence guard, not a style check. PEP 685 normalises extra names, so
    an extra declared as ``search_providers`` is published in the metadata as
    ``search-providers``. A self-reference written with the underscore then
    resolves to NOTHING — it installs no packages, and raises no error.

    That is not hypothetical: ``[bot]`` and ``[search]`` composed
    ``search_providers`` and ``youtube_music``, and the tox environment came up
    with no spotipy, no google-api-python-client, no beautifulsoup4 and no
    ytmusicapi while every hyphen-free group resolved correctly. The suite failed
    at collection, several layers away from the cause.
    '''
    import tomllib  # pylint: disable=import-outside-toplevel
    pyproject = tomllib.loads((REPO_ROOT / 'pyproject.toml').read_text(encoding='utf-8'))
    extras = pyproject['project']['optional-dependencies']

    unnormalised = sorted(name for name in extras
                          if name != re.sub(r'[-_.]+', '-', name).lower())
    assert not unnormalised, (
        f'extras {unnormalised} are not PEP 685 normalised. Publishing normalises '
        f'them anyway, so any self-reference using this spelling silently resolves '
        f'to nothing. Rename them with hyphens.'
    )

    dangling = []
    for name, specs in extras.items():
        for spec in specs:
            match = re.match(r'^discord_bot\[([^\]]+)\]$', spec.strip())
            if not match:
                continue
            for referenced in match.group(1).split(','):
                if referenced.strip() not in extras:
                    dangling.append(f'[{name}] -> [{referenced.strip()}]')
    assert not dangling, (
        f'self-references naming extras that do not exist: {dangling}. These install '
        f'nothing and fail silently — the package only goes missing wherever that '
        f'extra was the sole route to it.'
    )


# Internal modules the bot process must not import. Unlike the package lists
# above, none of these would ImportError on the bot image — [bot] installs
# everything they need. They are here because importing them is the signature of
# a reintroduced in-process fallback, and a fallback is silent: the bot would run
# its own private broker registry while the downloader and search pods talked to
# the real one, and the symptom is "audio never plays", not a crash.
#
# This list has shrunk from five entries to one, and the shrinking is the point.
# The three asyncio_* engines and clients/broker_client left discord_bot/ rather
# than being listed here: a module under tests/ cannot reach an image, so "the
# bot must not import them" became a property of the build instead of a rule
# this tuple has to keep restating. A rule enforced by the filesystem does not
# need remembering; a rule enforced by a tuple does.
#
# What remains is the one module that genuinely ships and must still stay out of
# the bot's import graph. servers/broker_server is deployable -- the broker pod
# serves it -- so it cannot be moved out of reach, which is exactly why it still
# needs asserting.
BOT_FORBIDDEN_MODULES = (
    'discord_bot.servers.broker_server',
)


def test_bot_imports_no_in_process_tier_modules():
    '''The bot process imports none of the in-process engine modules.'''
    imported = set(measure('discord_bot.cli.bot')['modules'])
    leaked = sorted(imported & set(BOT_FORBIDDEN_MODULES))
    assert not leaked, (
        f'the bot process imported {leaked} — these are test doubles, not deployable '
        f'code. Something re-introduced an in-process tier, or annotated against an '
        f'engine type instead of the client Protocol (which is how '
        f'interfaces/broker_protocols kept reaching music_player).'
    )


def test_every_published_image_has_a_boundary():
    '''
    Every console script that ships as an image is covered above.

    Without this, adding a sixth image silently gets no boundary at all — the
    failure mode being guarded against is an image nobody thought to guard.

    The published set is read from pyproject rather than restated here. It used
    to be a hardcoded literal, which meant it agreed with ``[project.scripts]``
    only for as long as someone kept both in step by hand — and a stale copy
    passes just as green as a correct one.
    '''
    import tomllib  # pylint: disable=import-outside-toplevel
    pyproject = tomllib.loads((REPO_ROOT / 'pyproject.toml').read_text(encoding='utf-8'))
    published = {target.split(':', 1)[0]
                 for target in pyproject['project']['scripts'].values()}
    assert set(IMAGE_IMPORTS) == published, (
        f'console scripts and image boundaries disagree: '
        f'{published ^ set(IMAGE_IMPORTS)}. Every published script ships as an image, '
        f'so every one needs a boundary above.'
    )


def test_ownership_doc_is_current():
    '''
    docs/image-dependencies.md matches a live measurement.

    The doc is the human-readable answer to "what is used strictly by what". It is
    generated rather than written so it cannot drift into being confidently wrong,
    which is the failure mode this project keeps finding in restated facts.
    '''
    rendered = render_table()
    if os.environ.get('UPDATE_IMAGE_DEPS'):
        OWNERSHIP_DOC.write_text(rendered, encoding='utf-8')
    assert OWNERSHIP_DOC.read_text(encoding='utf-8') == rendered, (
        'docs/image-dependencies.md is out of date. Regenerate with:\n'
        '    UPDATE_IMAGE_DEPS=1 pytest tests/cli/test_import_boundaries.py'
    )


# The only image whose Dockerfile may COPY the migration scripts. Declared, not
# derived, because the point is that changing it takes a deliberate edit.
MIGRATION_IMAGE_DOCKERFILES = frozenset({'Dockerfile.db'})


def test_only_the_db_image_ships_the_migrations():
    """A Dockerfile may COPY alembic/ only if its image can run it.

    This exists because the property broke once already and nothing noticed:
    #917 removed the migration COPYs from the dispatcher, then the MR 4 cutover
    took `[database]` out of `[bot]` and `[broker]` -- dropping the alembic CLI
    from both images while their COPY lines stayed. An image shipping scripts it
    cannot execute is a lie about its own capability, and it was found by
    reading Dockerfiles by hand, weeks later.

    The import boundary above cannot catch it: whether a package is IMPORTED and
    whether files are COPYd are different facts, and this one lives in the
    Dockerfile rather than in the source tree.
    """
    dockerfiles = sorted((REPO_ROOT / 'docker').glob('Dockerfile*'))
    assert dockerfiles, 'no Dockerfiles found -- the glob is wrong, not the repo'

    ships = set()
    for path in dockerfiles:
        copies = [
            line for line in path.read_text(encoding='utf-8').splitlines()
            if line.startswith('COPY') and 'alembic' in line
        ]
        if copies:
            ships.add(path.name)

    assert ships == set(MIGRATION_IMAGE_DOCKERFILES), (
        f'{sorted(ships)} ship migration files, expected '
        f'{sorted(MIGRATION_IMAGE_DOCKERFILES)}. An image that COPYs alembic/ '
        'without installing the alembic CLI cannot run what it carries; one that '
        'installs it without the files has nothing to run. Change the constant '
        'above only when an image genuinely gains or loses schema ownership.'
    )


def test_every_dockerfile_exists():
    '''
    Every declared Dockerfile is on disk, and every image has exactly one.

    ci.yml's build matrix is generated from IMAGE_DOCKERFILES now, so a typo here
    no longer fails loudly at build time -- it produces a matrix leg pointing at a
    path that does not exist, which is a build failure with a confusing message,
    or worse a missing leg. Checking against the filesystem is cheap and the
    declaration is the only place the mapping is written down.
    '''
    assert set(IMAGE_DOCKERFILES) == set(IMAGE_IMPORTS), (
        f'images and dockerfiles disagree: {set(IMAGE_DOCKERFILES) ^ set(IMAGE_IMPORTS)}'
    )
    missing = sorted(path for path in IMAGE_DOCKERFILES.values()
                     if not (REPO_ROOT / path).is_file())
    assert not missing, f'declared Dockerfiles that do not exist: {missing}'
    duplicated = len(set(IMAGE_DOCKERFILES.values())) != len(IMAGE_DOCKERFILES)
    assert not duplicated, (
        f'two images share a Dockerfile: {sorted(IMAGE_DOCKERFILES.values())}. '
        f'The build matrix keys on it, so they would build the same thing twice.'
    )


def test_image_closure_is_current():
    '''
    docs/image-closure.json matches a live measurement.

    This one is load-bearing in a way the markdown table is not: ci.yml reads it
    to decide which images to build, so a stale copy does not merely misinform a
    reader, it skips a build. A module that moved into the bot's import chain
    since the last regeneration would be seen as not affecting the bot.
    '''
    rendered = render_closure()
    if os.environ.get('UPDATE_IMAGE_DEPS'):
        CLOSURE_DOC.write_text(rendered, encoding='utf-8')
    assert CLOSURE_DOC.read_text(encoding='utf-8') == rendered, (
        'docs/image-closure.json is out of date, and CI keys its build filter on it.\n'
        'Regenerate with:\n'
        '    UPDATE_IMAGE_DEPS=1 pytest tests/cli/test_import_boundaries.py'
    )


def test_every_module_is_claimed_by_some_image():
    '''
    No first-party module is reachable from no entrypoint.

    This is the property the CI filter depends on: if a file belongs to no
    image's closure, "which images does this change affect" has no safe answer,
    and the filter fails the build rather than guessing. Asserting it here means
    the answer arrives when the unreachable module is added, not weeks later on
    an unrelated PR that happens to touch it.
    '''
    closure = json.loads(CLOSURE_DOC.read_text(encoding='utf-8'))
    claimed = set().union(*(set(image['modules']) for image in closure['images']))
    orphans = []
    for path in sorted((REPO_ROOT / 'discord_bot').rglob('*.py')):
        module = str(path.relative_to(REPO_ROOT).with_suffix('')).replace('/', '.')
        if module.endswith('.__init__'):
            module = module[: -len('.__init__')]
        if module not in claimed:
            orphans.append(module)
    assert not orphans, (
        f'modules no entrypoint reaches: {orphans}. They ship in every image and no '
        f'test exercises them through a real import chain. Delete them, or move them '
        f'under tests/ if they are doubles.'
    )
