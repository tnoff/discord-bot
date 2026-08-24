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
import os
import re

import pytest

from tests.cli._image_deps import (
    IMAGE_IMPORTS, OWNERSHIP_DOC, REPO_ROOT, VOCABULARY, measure, render_table,
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
BOT_FORBIDDEN_MODULES = (
    'discord_bot.workers.asyncio_broker',
    'discord_bot.servers.broker_server',
    'discord_bot.clients.broker_client',
    'discord_bot.workers.asyncio_download_worker',
    'discord_bot.workers.asyncio_youtube_music_search_worker',
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
