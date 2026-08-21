'''
The cog-facing DownloadClient Protocol, on its own.

Split out of interfaces/download_protocols.py so that annotating a download
handle does not import the download ENGINE.  download_protocols pulls yt_dlp,
moviepy (via utils/audio), boto3 (via integrations/s3) and the whole worker
machinery — none of which a process that only SUBMITS downloads has any use for.
The bot is exactly that process since the download dual path was collapsed
(projects/discord-bot-ha-only).

Same split, and the same stated reason, as BrokerClient, CheckoutResult and
ClearGuildResult before it.  Re-exported from download_protocols, so existing
imports keep working.

RETRY_BACKOFF_SECONDS_MINIMUM comes along because the cog's config model defaults
to it, which would otherwise drag the engine in through a default value.
'''
import asyncio
from typing import Callable, Protocol

from discord_bot.types.clear_guild_result import ClearGuildResult
from discord_bot.types.media_request import MediaRequest

__all__ = ['DownloadClient', 'RETRY_BACKOFF_SECONDS_MINIMUM']

# Hold-off before a failed YouTube download is eligible again, doubling per attempt.
#
# Pool mode replaced the pod-global backoff with a PER-EXIT one, which rotates exits
# but paces a single request not at all: a retry re-queued instantly just leases the
# next free exit. When failures are correlated across exits — a bot-check wave, which
# is what the 2026-08-13 incident was — that is no pacing whatsoever, and the whole
# 16-exit pool drained in ~45s while seven distinct videos failed on every one of
# them. Worse, each lease locks its exit for wait_period_minimum (90s) whether it
# succeeds or fails, so instant retries burn the pool's throughput ceiling
# (16 exits / 90s) on attempts that cannot succeed yet, starving requests that could.
#
# This is the per-request half the pool migration dropped. DIRECT items are exempt:
# they are not YouTube-rate-limited and bypass backoff everywhere else too.
RETRY_BACKOFF_SECONDS_MINIMUM = 30


class DownloadClient(Protocol):
    '''
    Cog-facing handle for the download pipeline.

    The producer surface (submit / block_guild / clear_guild_queue /
    queue_size) is async so a Redis-backed client can do its I/O inline; the
    cog drives the consumer via run() as a background loop.  failure_summary /
    backoff_seconds_remaining stay synchronous — they read cached backoff state
    the run() loop refreshes — for logging and metrics.
    '''
    async def submit(self, guild_id: int, media_request: MediaRequest,
                     priority: int | None = None) -> None:
        '''Enqueue a MediaRequest for download; results are reported to the broker.'''

    async def block_guild(self, guild_id: int) -> bool:
        '''Block new submissions for a guild (used during shutdown/cleanup).'''

    async def clear_guild_queue(self, guild_id: int,
                                preserve_predicate: Callable[[MediaRequest], bool] | None = None,
                                ) -> ClearGuildResult:
        '''Clear the input queue for a guild.

        Returns a ClearGuildResult carrying the dropped requests plus the
        bundle_uuids of any items the predicate preserved (so the cog can skip
        deleting those bundles, including in HA where the predicate runs on the
        downloader pod, not the bot).'''

    async def queue_size(self, guild_id: int) -> int:
        '''Return the number of pending requests for a guild, or 0 if none.'''

    @property
    def failure_summary(self) -> str:
        '''Human-readable summary of the download failure queue.'''

    @property
    def backoff_seconds_remaining(self) -> int | None:
        '''Seconds remaining in the current backoff period, or None if not set.'''

    async def run(self, shutdown_event: asyncio.Event) -> None:
        '''Consume one queued request and download it; driven as a background loop.'''
