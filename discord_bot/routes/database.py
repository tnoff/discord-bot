'''
The database seam: the persistence tier's route families.

Imported by `servers/database_server.py` and the four `clients/http_*_store.py`
modules via `clients/http_store_base.py` — fanout 3 (bot, broker, db).

**Grouped, and generated rather than listed one per constant.** All 33 routes are
POST under `/database/<group>/<name>`, and `database_server` registers them in a
loop; the project spec is explicit that this style should not be normalised into
the broker's explicit-per-route shape as part of this work. So the registry keeps
the same shape the code already has and simply becomes its single definition.

**The groups are CONDITIONAL, which is why the route advertisement had to be a
runtime read.** `database_server` registers a group only if its store was
constructed, so a db pod's real route set is not a static property of this file:
this module says what the tier CAN serve, and `GET /_contract/routes` says what a
particular pod DOES serve. A 404 on this seam has always meant "that store is not
configured here", a supported state, and it still does.

**Route names are resolved through the registry, not interpolated.** The store
clients still address routes by name -- `self._call('get_analytics')` -- because
that is the generated style and converting 33 call sites to symbols would be the
normalisation the spec rules out. The difference is that the name is now looked up
in the group, so an unknown one raises KeyError at the call site instead of
producing a 404 against a route that never existed.
'''
from dataclasses import dataclass

from discord_bot.routes.route import Route

ROUTE_PREFIX = '/database'


@dataclass(frozen=True)
class DatabaseGroup:
    '''One store's route family under /database/<name>.'''
    name: str
    routes: dict

    @property
    def prefix(self) -> str:
        '''The path this group's routes hang off, e.g. /database/markov.'''
        return f'{ROUTE_PREFIX}/{self.name}'

    @property
    def all(self) -> tuple[Route, ...]:
        '''Every route in this group, in definition order.'''
        return tuple(self.routes.values())

    def bind(self, handlers: dict) -> dict:
        '''Pair this group's routes with handlers, keyed by Route.

        Requires EXACT correspondence and says which side is wrong. A handler for
        a route the registry does not define, or a route with no handler, is a
        seam defect that would otherwise surface as a 404 in production -- and on
        this seam a 404 is indistinguishable from the supported
        "store not configured here", so it would surface as nothing at all.
        '''
        missing = set(self.routes) - set(handlers)
        unknown = set(handlers) - set(self.routes)
        if missing or unknown:
            raise ValueError(
                f'database group {self.name!r} handler mismatch: '
                f'no handler for {sorted(missing)}, '
                f'handler for undefined {sorted(unknown)}')
        return {self.routes[name]: handler for name, handler in handlers.items()}


def _group(name: str, *route_names: str) -> DatabaseGroup:
    '''Build a group; every route on this seam is a POST under the group prefix.'''
    prefix = f'{ROUTE_PREFIX}/{name}'
    return DatabaseGroup(name, {rn: Route('POST', f'{prefix}/{rn}') for rn in route_names})


GUILD_ANALYTICS = _group(
    'guild_analytics',
    'get_analytics', 'record_play',
)

MARKOV = _group(
    'markov',
    'list_channels', 'list_guild_channel_ids', 'get_channel', 'add_channel',
    'remove_channel', 'reset_channel', 'save_messages', 'generate_words',
    'prune_relations_before',
)

PLAYLIST = _group(
    'playlist',
    'list_playlists', 'count_playlists', 'get_playlist', 'get_playlist_by_name',
    'get_history_playlist', 'ensure_history_playlist', 'create_playlist',
    'delete_playlist', 'rename_playlist', 'mark_queued', 'get_playlist_size',
    'list_items', 'add_items', 'delete_item', 'delete_item_by_index',
    'record_history_item',
)

VIDEO_CACHE = _group(
    'video_cache',
    'iterate_file', 'get_webpage_url_item', 'remove_video_cache', 'ready_remove',
    'get_deletable_entries', 'get_cache_count',
)

#: Every group the tier can serve. A pod serves a subset — see the docstring.
GROUPS = (GUILD_ANALYTICS, MARKOV, PLAYLIST, VIDEO_CACHE)

ALL = tuple(route for group in GROUPS for route in group.all)
