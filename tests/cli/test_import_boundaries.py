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

SEARCH_CLIENTS = (YTMUSICAPI, SPOTIPY, GOOGLEAPI)
MEDIA = (YT_DLP, MOVIEPY)

IMAGE_BOUNDARIES = [
    pytest.param(
        'discord_bot.cli.bot',
        (YTMUSICAPI,),
        # Only ytmusicapi today. The bot still imports yt_dlp, spotipy,
        # googleapiclient and bs4 because the cog constructs the in-process
        # download worker and the SearchClient source-expansion member at module
        # scope. Those become forbidden when single-process retires — that is the
        # point of projects/discord-bot-ha-only, and this list is where it lands.
        id='bot',
    ),
    pytest.param(
        'discord_bot.cli.dispatcher',
        (SQLALCHEMY, BOTO3, BS4, DAPPERTABLE) + MEDIA + SEARCH_CLIENTS,
        # The strictest image: it installs no extras at all, only the base
        # dependencies. That has been true by discipline; this makes it a contract.
        id='dispatcher',
    ),
    pytest.param(
        'discord_bot.cli.broker',
        (BS4,) + MEDIA + SEARCH_CLIENTS,
        # Keeps sqlalchemy + boto3: it owns the video cache and S3 checkout.
        id='broker',
    ),
    pytest.param(
        'discord_bot.cli.downloader',
        (SQLALCHEMY, BS4, DAPPERTABLE, MOVIEPY) + SEARCH_CLIENTS,
        # Keeps yt_dlp (it downloads) and boto3 (it uploads finished media).
        # sqlalchemy was reaching it until this change, twice over — see the
        # module docstring on interfaces/broker_client_protocol.
        id='downloader',
    ),
    pytest.param(
        'discord_bot.cli.search',
        (SQLALCHEMY, BOTO3, BS4, DAPPERTABLE, SPOTIPY, GOOGLEAPI) + MEDIA,
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


def test_every_published_image_has_a_boundary():
    '''
    Every console script that ships as an image is covered above.

    Without this, adding a sixth image silently gets no boundary at all — the
    failure mode being guarded against is an image nobody thought to guard.
    Every published script is now covered: ``discord-bot`` is cli.bot, since
    projects/discord-bot-ha-only retired the single-process entrypoint that used
    to own that name.
    '''
    covered = {p.values[0] for p in IMAGE_BOUNDARIES}
    assert covered == {
        'discord_bot.cli.bot',
        'discord_bot.cli.dispatcher',
        'discord_bot.cli.broker',
        'discord_bot.cli.downloader',
        'discord_bot.cli.search',
    }
