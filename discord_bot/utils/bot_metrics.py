'''
Metric names the bot process emits.

Split out of utils/otel.py (2026-09-17). These seven names are emitted by the
bot and nothing else, so keeping them in the shared naming enum meant every
bot-only metric addition edited a module all six images import -- and rebuilt
all six to ship a string one of them uses.

Reached by discord_bot.cli.bot alone, which is the property that makes the
split worth anything; check it with the closure in docs/image-closure.json
rather than by reading imports.
'''
from enum import Enum


class BotMetricNaming(Enum):
    '''Metric names emitted by the bot process.'''
    ACTIVE_PLAYERS = 'active_players'
    VOICE_CLIENTS_CONNECTED = 'voice_clients_connected'
    CACHE_FILESYSTEM_MAX = 'cache_filesystem_max'
    CACHE_FILESYSTEM_USED = 'cache_filesystem_used'
    DISPATCHER_READY_CHECK = 'dispatcher_ready_check'
    # The bot's TCP probe of the db POD, added with MR 4b. Deliberately not
    # 'database_ready_check': that name belongs to the db pod's own /health
    # outcome (servers/database_health_server) and is what the
    # discord-db-postgres-unreachable alert watches. Reusing it would make a
    # bot-side network failure fire an alert that means 'postgres is
    # unreachable from the db pod' -- two different faults, one page.
    DATABASE_PEER_READY_CHECK = 'database_peer_ready_check'
    DISPATCH_RESULT_QUEUE_DEPTH = 'dispatch_result_queue_depth'
