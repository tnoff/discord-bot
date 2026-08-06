'''
Egress-exit pool for per-download exit selection.

Some deployments want each download to leave through a different exit (so a single
flagged/blocked exit only affects a fraction of downloads and can be dropped from
rotation in-app).  This module provides the provider-agnostic machinery for that:

- ``ExitPool`` leases a free, non-backed-off exit per download.  It deals in
  **opaque exit identifiers** and knows nothing about VPNs or proxies.
- ``ExitProxyResolver`` is the one provider-specific seam: it maps an exit id to the
  yt-dlp proxy URL that routes a download out that exit.  ``MullvadSocks5Resolver``
  is the first implementation (a Mullvad server's in-tunnel SOCKS5 proxy); add a VPN
  or proxy provider by subclassing it and registering in ``EXIT_PROXY_RESOLVERS``,
  then select it with the ``music.download.egress_mode`` config key.
- ``ExitClients`` builds + caches one yt-dlp client per exit, each pinned to that
  exit's proxy via the resolver.

The lease is in-process (a single downloader pod runs N concurrent downloads over
one tunnel); backoff stays in redis, injected as a callable so the pool never
touches redis directly and is trivially testable.
'''
from abc import ABC, abstractmethod
from typing import NamedTuple

from yt_dlp import YoutubeDL

from discord_bot.exceptions import DiscordBotException

# Default egress: one yt-dlp client through a single fixed proxy (today's model).
# Any other egress_mode names an exit-proxy provider in EXIT_PROXY_RESOLVERS below,
# and the worker builds an ExitPool + ExitClients instead of a single client.
EGRESS_MODE_HTTP_PROXY = 'http-proxy'

MULLVAD_SOCKS5_SUFFIX = 'relays.mullvad.net'
MULLVAD_SOCKS5_PORT = 1080


def mullvad_socks5_endpoint(server: str) -> str:
    '''
    Map a Mullvad WireGuard server name to its SOCKS5 proxy hostname.

    ``us-nyc-wg-301`` -> ``us-nyc-wg-socks5-301.relays.mullvad.net`` — Mullvad
    inserts ``socks5-`` before the trailing index (see their help page
    "different entry/exit node using WireGuard and SOCKS5 proxy").

    Raises ValueError on a name that doesn't end in ``-<digits>`` so a typo in the
    exit pool fails loudly at startup rather than yielding a dead proxy host.
    '''
    head, sep, index = server.rpartition('-')
    if not sep or not index.isdigit():
        raise ValueError(f'Unrecognized Mullvad WG server name: {server!r}')
    return f'{head}-socks5-{index}.{MULLVAD_SOCKS5_SUFFIX}'


class ExitProxyResolver(ABC):
    '''
    Provider-agnostic seam: map an exit identifier to the yt-dlp proxy URL that
    routes a download out that exit.  The only VPN/proxy-provider-specific piece;
    everything else (pool, lease, client cache) is generic.
    '''

    @abstractmethod
    def proxy_url(self, exit_name: str) -> str:
        '''Return the yt-dlp ``proxy`` value that egresses via ``exit_name``.'''


class MullvadSocks5Resolver(ExitProxyResolver):
    '''Routes an exit through that Mullvad server's in-tunnel SOCKS5 proxy.'''

    def proxy_url(self, exit_name: str) -> str:
        '''``socks5h://`` (remote DNS) to the server's SOCKS5 endpoint on 1080.'''
        return f'socks5h://{mullvad_socks5_endpoint(exit_name)}:{MULLVAD_SOCKS5_PORT}'


# Non-default egress modes -> exit-proxy resolver. Register a VPN/proxy provider
# here and select it with music.download.egress_mode.
EXIT_PROXY_RESOLVERS = {
    'mullvad-socks5': MullvadSocks5Resolver,
}


def build_exit_resolver(egress_mode: str) -> ExitProxyResolver:
    '''
    Build the exit-proxy resolver for a non-default egress mode.

    Raises DiscordBotException on an unknown mode so a config typo fails loudly at
    startup rather than silently falling back.
    '''
    try:
        resolver_cls = EXIT_PROXY_RESOLVERS[egress_mode]
    except KeyError as exc:
        known = ', '.join(sorted(EXIT_PROXY_RESOLVERS))
        raise DiscordBotException(
            f'Unknown egress_mode {egress_mode!r}; known: {EGRESS_MODE_HTTP_PROXY}, {known}') from exc
    return resolver_cls()


class ExitPool:
    '''Round-robin lease of free + healthy exits, one per download. Provider-agnostic.'''

    def __init__(self, exits):
        '''exits : iterable of opaque exit identifiers (e.g. Mullvad server names).'''
        self._exits = list(exits)
        if not self._exits:
            raise ValueError('ExitPool requires at least one exit')
        self._leased: set[str] = set()
        self._cursor = 0

    async def lease(self, reserve) -> str | None:
        '''
        Reserve and return a free exit, or None if none is available.

        reserve : async callable ``(exit) -> bool`` supplied by the worker.  It
        atomically claims the exit (cross-pod, via its shared redis window) and
        returns True on success, or False if the exit is backed off / already
        claimed elsewhere.  Iterates round-robin from the cursor so exits rotate
        instead of hammering the first healthy one.

        The exit is added to the in-pod leased set BEFORE the ``await`` so two
        concurrent leases on the same pod can't both select it (the await is a
        yield point); a lost reserve frees it again and the scan moves on.
        '''
        count = len(self._exits)
        for offset in range(count):
            exit_name = self._exits[(self._cursor + offset) % count]
            if exit_name in self._leased:
                continue
            self._leased.add(exit_name)
            if await reserve(exit_name):
                self._cursor = (self._cursor + offset + 1) % count
                return exit_name
            self._leased.discard(exit_name)
        return None

    def release(self, exit_name: str) -> None:
        '''Free a leased exit so it can be handed out again (idempotent).'''
        self._leased.discard(exit_name)

    @property
    def leased(self) -> frozenset:
        '''Snapshot of the currently-leased exits.'''
        return frozenset(self._leased)

    @property
    def exit_names(self) -> tuple:
        '''All exit ids this pool rotates through (order stable).'''
        return tuple(self._exits)


class ExitClients:
    '''
    Lazily builds and caches one yt-dlp client per exit, each pinned to that exit's
    proxy (via the resolver).

    Each in-flight download runs through the exit it leased, so it needs a client
    whose ``proxy`` is that exit.  Clients are built on first use, cached for the
    process's life, and bounded by the exit pool.  All access is synchronous on the
    single-threaded event loop, so no lock is needed.
    '''

    def __init__(self, base_opts: dict, resolver: ExitProxyResolver, client_factory=YoutubeDL):
        '''
        base_opts : shared yt-dlp options WITHOUT a proxy (the resolver adds it per exit).
        resolver : maps an exit id to its proxy URL.
        client_factory : builds a client from an opts dict (injectable for tests).
        '''
        self._base_opts = base_opts
        self._resolver = resolver
        self._client_factory = client_factory
        self._by_exit: dict = {}

    def for_exit(self, exit_name: str):
        '''Return the cached client whose proxy routes out ``exit_name``.'''
        client = self._by_exit.get(exit_name)
        if client is None:
            opts = {**self._base_opts, 'proxy': self._resolver.proxy_url(exit_name)}
            client = self._client_factory(opts)
            self._by_exit[exit_name] = client
        return client


class DownloadEgress(NamedTuple):
    '''
    What a single download uses: its yt-dlp client and the exit it leaves from.

    ``exit_name`` is None for the fixed http proxy (the exit is known only via the
    background probe); a pool mode fills it with the leased exit.
    '''
    client: object
    exit_name: str | None


class Egress(ABC):
    '''
    Strategy for how the worker acquires a client + exit per download.  One per
    worker, never None — the download path asks it and never branches on a mode.
    '''

    @abstractmethod
    async def acquire(self, reserve) -> 'DownloadEgress | None':
        '''
        Acquire a client + exit for one download, or None if no exit is currently
        available (all leased or backed off).  ``reserve`` is an async
        ``(exit) -> bool`` the pool consults to atomically claim an exit; the http
        proxy ignores it.
        '''

    @abstractmethod
    def release(self, egress: 'DownloadEgress') -> None:
        '''Release what a matching acquire() returned.'''

    @property
    def is_pool(self) -> bool:
        '''True when downloads fan out across a pool of exits (so concurrent
        drivers pay off); False for the single fixed-proxy client.'''
        return False

    @property
    def exit_names(self) -> tuple:
        '''Exit ids available to fan out across; empty for the fixed proxy.'''
        return ()


class HttpProxyEgress(Egress):
    '''Every download goes through one fixed-proxy client (today's model).'''

    def __init__(self, client):
        self._client = client

    async def acquire(self, reserve) -> 'DownloadEgress':
        '''Always the single client; exit is discovered out-of-band by the probe.'''
        return DownloadEgress(self._client, None)

    def release(self, egress: 'DownloadEgress') -> None:
        '''Nothing to release — the client is shared.'''


class PoolEgress(Egress):
    '''Lease a distinct exit per download and route it through that exit's client.'''

    def __init__(self, pool: ExitPool, clients: ExitClients):
        self._pool = pool
        self._clients = clients

    async def acquire(self, reserve) -> 'DownloadEgress | None':
        '''Reserve a free, available exit and hand back its client, or None.'''
        exit_name = await self._pool.lease(reserve)
        if exit_name is None:
            return None
        return DownloadEgress(self._clients.for_exit(exit_name), exit_name)

    def release(self, egress: 'DownloadEgress') -> None:
        '''Return the leased exit to the pool.'''
        self._pool.release(egress.exit_name)

    @property
    def is_pool(self) -> bool:
        return True

    @property
    def exit_names(self) -> tuple:
        '''The pool's exit ids (used to size the driver fleet + gate on soonest-free).'''
        return self._pool.exit_names
