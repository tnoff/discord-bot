'''
Per-image import boundaries — what each published image is allowed to import.

Each entry below is a contract: importing that image's entrypoint must not pull
the listed packages into ``sys.modules``. The check is the enforcement mechanism
for the per-image dependency split (projects/discord-bot-ha-only) — a folder
layout cannot prevent ``from discord_bot.utils.integrations.youtube_music import
...``, but this can, and this is what caught the ytmusicapi leak during the
search-pod work before it could CrashLoop a pod
(see reference_slim_pod_import_chain_leak).

Two things make these tests worth their weight:

- **A violation is a pod-start crash, not a test failure.** The slim images
  install only their own extra, so an import that reaches a package the image
  does not ship is an ImportError at startup — discovered in prod, on a rollout.
- **They are the discovery tool for the extras split.** The gap between what an
  image *imports* and what its extra *installs* is what the split acts on.

Each runs in a subprocess: the rest of the suite has already imported the cog, so
with the module cache pre-poisoned nothing here would be observable in-process.

The lists are deliberately what is TRUE TODAY, not the eventual goal. Where an
image still imports something it arguably should not, that is recorded as a
comment rather than a skipped assertion — a guard that does not run is worse than
an honest gap.
'''
import subprocess  # nosec B404 - fixed argv, no shell, test-only import probe
import sys
import tomllib
from pathlib import Path

import pytest

# Packages no image should import unless it genuinely uses them. Kept as one
# vocabulary so a new image starts from "forbid everything" and opts in.
YT_DLP = 'yt_dlp'
MOVIEPY = 'moviepy'
YTMUSICAPI = 'ytmusicapi'
SPOTIPY = 'spotipy'
GOOGLEAPI = 'googleapiclient'
SQLALCHEMY = 'sqlalchemy'
BOTO3 = 'boto3'
BS4 = 'bs4'
DAPPERTABLE = 'dappertable'
DISCORD = 'discord'

SEARCH_CLIENTS = (YTMUSICAPI, SPOTIPY, GOOGLEAPI)
MEDIA = (YT_DLP, MOVIEPY)

IMAGE_BOUNDARIES = [
    pytest.param(
        'discord_bot.cli.bot',
        (YTMUSICAPI,) + MEDIA,
        # yt_dlp and moviepy joined the list when the download dual path was
        # collapsed: the cog no longer builds an in-process worker, and the
        # DownloadClient Protocol + HttpDownloadClient moved to modules that do
        # not import the engine. spotipy, googleapiclient and bs4 are still here
        # — the cog builds the SearchClient source-expansion member at module
        # scope, and that is media_search's to move.
        #
        # The broker collapse added nothing to this list, and that is the answer
        # to the open question in projects/discord-bot-ha-only: the bot keeps
        # boto3 and sqlalchemy on their own merits, not the broker's. boto3
        # reaches it through music_player's integrations.s3 get_file — under HA
        # the broker's checkout returns an s3_key and the BOT fetches the file
        # before playback — and sqlalchemy through delete_messages, markov and
        # the playlist tables. What the collapse did drop is three heavy modules:
        # workers/asyncio_broker, servers/broker_server and clients/broker_client
        # are no longer imported by the bot process at all.
        id='bot',
    ),
    pytest.param(
        'discord_bot.cli.dispatcher',
        (SQLALCHEMY, BOTO3, BS4, DAPPERTABLE) + MEDIA + SEARCH_CLIENTS,
        # The strictest image: it installs no extras at all, only the base
        # dependencies. That has been true by discipline; this makes it a contract.
        #
        # discord is NOT forbidden here, and that is deliberate: unlike the
        # broker, downloader and search pods, the dispatcher sends and edits real
        # messages (workers/message_dispatcher imports Message, Bot and NotFound).
        # It is the one non-gateway image that genuinely needs discord.py, which
        # is why its startup log has always carried the PyNaCl/davey warnings.
        id='dispatcher',
    ),
    pytest.param(
        'discord_bot.cli.broker',
        (BS4, DISCORD) + MEDIA + SEARCH_CLIENTS,
        # Keeps sqlalchemy + boto3: it owns the video cache and S3 checkout.
        # discord joined the list when command_wrapper moved to
        # utils/otel_command — utils/otel used to import Context at module scope
        # for a runtime isinstance, which put discord.py in every image.
        id='broker',
    ),
    pytest.param(
        'discord_bot.cli.downloader',
        (SQLALCHEMY, BS4, DAPPERTABLE, MOVIEPY, DISCORD) + SEARCH_CLIENTS,
        # Keeps yt_dlp (it downloads) and boto3 (it uploads finished media).
        # sqlalchemy was reaching it until this change, twice over — see the
        # module docstring on interfaces/broker_client_protocol.
        id='downloader',
    ),
    pytest.param(
        'discord_bot.cli.search',
        (SQLALCHEMY, BOTO3, BS4, DAPPERTABLE, SPOTIPY, GOOGLEAPI, DISCORD) + MEDIA,
        # Keeps ytmusicapi. spotipy + googleapiclient move OUT of this list when
        # media_search folds its providers into this pod.
        id='search',
    ),
]


@pytest.mark.parametrize('entrypoint,forbidden', IMAGE_BOUNDARIES)
def test_image_import_boundary(entrypoint, forbidden):
    '''Importing an image entrypoint must not pull its forbidden packages.'''
    probe = (
        f'import importlib, sys; importlib.import_module({entrypoint!r}); '
        f'print(",".join(sorted(m for m in {list(forbidden)!r} if m in sys.modules)))'
    )
    result = subprocess.run([sys.executable, '-c', probe],  # nosec B603 - fixed argv, no shell
                            capture_output=True, text=True, check=True)
    leaked = [m for m in result.stdout.strip().split(',') if m]
    assert not leaked, (
        f'{entrypoint} imported {leaked}, which its image does not install. '
        f'On a slim image this is an ImportError at pod start, not a test failure. '
        f'Find the chain with: python -c "import {entrypoint}" under a tracing '
        f'__import__ hook, then split the light type out of the heavy module — '
        f'the same move as CheckoutResult, ClearGuildResult and BrokerClient.'
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
    probe = (
        'import importlib, sys; importlib.import_module("discord_bot.cli.bot"); '
        f'print(",".join(sorted(m for m in {list(BOT_FORBIDDEN_MODULES)!r} if m in sys.modules)))'
    )
    result = subprocess.run([sys.executable, '-c', probe],  # nosec B603 - fixed argv, no shell
                            capture_output=True, text=True, check=True)
    leaked = [m for m in result.stdout.strip().split(',') if m]
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
    passes just as green as a correct one. Dropping the ``discord-bot-min``
    alias is exactly the event this test claims to notice, and the literal
    version would not have.
    '''
    pyproject = tomllib.loads(
        (Path(__file__).resolve().parents[2] / 'pyproject.toml').read_text(encoding='utf-8')
    )
    published = {target.split(':', 1)[0]
                 for target in pyproject['project']['scripts'].values()}
    covered = {p.values[0] for p in IMAGE_BOUNDARIES}
    assert covered == published, (
        f'console scripts and image boundaries disagree: '
        f'{published ^ covered}. Every published script ships as an image, so '
        f'every one needs a boundary above.'
    )
