'''
Public boundary for the ytmusicapi wrapper.

This module deliberately imports NOTHING heavy. `YoutubeMusicClient` is resolved
from `_youtube_music_impl` on first access (PEP 562 module `__getattr__`), so
ytmusicapi is imported when a caller actually builds a client — not when a
process merely imports something that mentions one.

**Why it matters.** `cogs/music.py` builds a client only in single-process mode;
under HA the search pod owns it. But the cog is imported by every bot process
through `cli/_lib/cog_registry.py`, so a module-scope `from ... import
YoutubeMusicClient` pulled ytmusicapi into the HA bot pod on every deployment —
the exact dependency the search tier exists to isolate. Attribute access defers
it; the ytmusicapi import still belongs at top level, in `_youtube_music_impl`.

Both access styles work and both are correct:

- `from ...youtube_music import YoutubeMusicClient` — resolves immediately, so
  the dependency loads at import time. Right for `cli/search.py`: the pod needs
  ytmusicapi and should fail loudly at startup if it is missing.
- `from ...integrations import youtube_music` then `youtube_music.YoutubeMusicClient()`
  — defers until construction. Right for `cogs/music.py`, whose HA path never
  constructs one.

`YoutubeMusicRetryException` is a plain re-export: it is a bare stdlib exception
and lives in `discord_bot.exceptions`, so catching it costs nothing. Import it
from there in new code.
'''
from importlib import import_module
from typing import TYPE_CHECKING

from discord_bot.exceptions import YoutubeMusicRetryException

if TYPE_CHECKING:  # pragma: no cover
    # Never executed. Present so static analysis (pylint, IDEs) can resolve the
    # name that __getattr__ supplies at runtime — without it, every
    # `from ...youtube_music import YoutubeMusicClient` reads as no-name-in-module.
    from discord_bot.utils.integrations._youtube_music_impl import YoutubeMusicClient

__all__ = ['YoutubeMusicClient', 'YoutubeMusicRetryException']


def __getattr__(name: str):
    '''
    Resolve YoutubeMusicClient lazily, importing ytmusicapi only now.

    `import_module` rather than an inline `import` statement: the deferral is the
    point of this module, and a function-scope import statement would be a
    pylint suppression rather than a design.
    '''
    if name == 'YoutubeMusicClient':
        return getattr(import_module('discord_bot.utils.integrations._youtube_music_impl'), name)
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
