'''
Result type for a queue client's clear_guild_queue call.

Lives in types/ rather than interfaces/download_protocols.py (its original home)
because the YouTube-Music search client returns the same shape, and
download_protocols imports yt_dlp — the search pod must never pull yt-dlp into
its import chain just to name a dataclass.  download_protocols re-exports it, so
`from discord_bot.interfaces.download_protocols import ClearGuildResult` keeps
working.
'''
from dataclasses import dataclass, field

from discord_bot.types.media_request import MediaRequest


@dataclass(frozen=True)
class ClearGuildResult:
    '''Result of a queue client's clear_guild_queue.

    dropped
        Requests removed from the guild's input queue; the cog pushes a
        DISCARDED lifecycle state for each.
    preserved_bundle_uuids
        bundle_uuids of items the preserve predicate KEPT (metadata-only
        playlist-add requests still in flight).  The cog skips deleting those
        bundles.  Both clients populate this — the in-process client from the
        predicate it runs, the HTTP client from the worker pod's response — so
        bundle preservation works in HA, where the predicate can't run on the
        bot side.
    '''
    dropped: list[MediaRequest]
    preserved_bundle_uuids: set[str] = field(default_factory=set)
