'''
Metric names the bot process emits, and nothing else.

Split out of utils/otel.py (2026-09-18). These five are emitted by the bot
alone, so keeping them in the shared enum meant a bot-only metric addition
edited a module all six images import -- and rebuilt six images to ship a
string one of them uses.

Reached by discord_bot.cli.bot alone, which is the property that makes the
split worth anything. It is checked rather than asserted in prose:
tests/utils/test_metric_ownership.py reads docs/image-closure.json and fails
if any name in the shared enum is emitted by exactly one image.
'''
from enum import Enum


class BotMetricNaming(Enum):
    '''Metric names emitted by the bot process.'''
    # The bot's probe of its peers. Not a pod reporting its own health -- that is
    # pod_ready_check, which every pod emits and which therefore stays shared.
    PEER_READY_CHECK = 'peer_ready_check'
    # One metric, two ways of counting the same thing, separated by `tracked_by`:
    # `player` counts MusicPlayer objects the cog holds, `voice_client` counts
    # discord.py's raw sockets. A socket with no player behind it is a stranded
    # bot, and that divergence cannot be expressed without both series.
    VOICE_SESSIONS = 'voice_sessions'
    # Registered only when the cache really is a filesystem -- with a storage
    # bucket, disk_usage on the download dir reports the pod root instead. Named
    # for the only case that creates them rather than for storage in general.
    CACHE_FILESYSTEM_MAX_BYTES = 'cache_filesystem_max_bytes'
    CACHE_FILESYSTEM_USED_BYTES = 'cache_filesystem_used_bytes'
    DISPATCH_RESULT_QUEUE_DEPTH = 'dispatch_result_queue_depth'
