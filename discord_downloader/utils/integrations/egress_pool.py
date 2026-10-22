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
import socket
from abc import ABC, abstractmethod
from typing import NamedTuple

from yt_dlp import YoutubeDL

from discord_core.exceptions import DiscordBotException

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

    @classmethod
    def from_config(cls, _egress_proxy: dict) -> 'ExitProxyResolver':
        '''
        Build the resolver from ``music.download.egress_proxy`` (``{}`` when unset).
        Resolvers that need no config keep this default; ones that do override it
        and fail loudly on a missing or malformed key.
        '''
        return cls()

    @abstractmethod
    def proxy_url(self, exit_name: str) -> str:
        '''Return the yt-dlp ``proxy`` value that egresses via ``exit_name``.'''

    def is_proxy_unreachable(self, error: BaseException) -> bool:  # pylint: disable=unused-argument
        '''
        True when ``error`` means the proxy itself could not be reached, so the
        download never got as far as an exit and the exit is not at fault.  Defaults
        to False: only a resolver whose proxy is a separate in-cluster hop can tell.
        '''
        return False


class MullvadSocks5Resolver(ExitProxyResolver):
    '''Routes an exit through that Mullvad server's in-tunnel SOCKS5 proxy.'''

    def proxy_url(self, exit_name: str) -> str:
        '''
        ``socks5h://`` (remote DNS) to the server's SOCKS5 endpoint on 1080.

        Do NOT "fix" this to ``socks5://`` to force local DNS -- it is the same
        thing to yt-dlp and changes nothing.  ``clean_proxies`` in
        ``yt_dlp/utils/networking.py`` carries
        ``replace_scheme = {'socks5': 'socks5h'}  # compat`` and rewrites the
        proxy URL on every request, so ``make_socks_proxy_opts`` never sees the
        ``socks5`` scheme and ``rdns`` is always True.  Verified on the pinned
        yt-dlp 2026.7.4.  This was tried in !227 and reverted here as a no-op.

        Consequence: the relay always resolves the destination and always picks
        the address family (it answers over IPv6).  yt-dlp's ``--force-ipv4``
        cannot reach that either -- ``source_address`` only constrains the hop
        TO the relay.  Forcing IPv4 egress needs something outside yt-dlp.
        '''
        return f'socks5h://{mullvad_socks5_endpoint(exit_name)}:{MULLVAD_SOCKS5_PORT}'


class ClusterSocks5Resolver(ExitProxyResolver):
    '''
    Routes an exit through a per-exit port on an in-cluster proxy that owns the VPN
    tunnel (the ``discord-vpn`` pod), instead of reaching the exit's SOCKS5 relay
    directly.  yt-dlp takes one proxy per client and cannot chain, so the second hop
    (that port -> the exit's Mullvad relay, inside the tunnel) lives in the proxy.
    The downloader pod itself needs no tunnel.
    '''

    def __init__(self, host: str, ports: dict):
        '''
        host : the proxy's in-cluster hostname (a Service name).
        ports : exit id -> the port whose listener forwards out that exit.
        '''
        self._host = host
        self._ports = dict(ports)

    @classmethod
    def from_config(cls, egress_proxy: dict) -> 'ClusterSocks5Resolver':
        '''Needs ``host`` and an ``exit_ports`` map; raises DiscordBotException otherwise.'''
        host = egress_proxy.get('host')
        ports = egress_proxy.get('exit_ports')
        if not host or not ports:
            raise DiscordBotException(
                'egress_mode cluster-socks5 requires music.download.egress_proxy '
                'with host and exit_ports')
        return cls(host, {name: int(port) for name, port in ports.items()})

    def is_proxy_unreachable(self, error: BaseException) -> bool:
        '''
        True when the TCP connection to the proxy Service was refused or its name did
        not resolve -- what a ``discord-vpn`` roll looks like (no ready endpoint, so
        kube-proxy rejects).  The proxy is the only hop yt-dlp dials directly, so a
        refused or unresolvable connect can only be that hop; anything further along
        (the exit relay, YouTube) is reported by the proxy as a SOCKS error instead.
        Timeouts are deliberately not matched: they are ambiguous.
        '''
        seen = set()
        while error is not None and id(error) not in seen:
            if isinstance(error, (ConnectionRefusedError, socket.gaierror)):
                return True
            seen.add(id(error))
            # yt-dlp's DownloadError keeps the underlying error in exc_info; its
            # networking errors use .cause; everything else chains via __cause__.
            exc_info = getattr(error, 'exc_info', None)
            error = ((exc_info[1] if exc_info else None)
                     or getattr(error, 'cause', None) or error.__cause__)
        return False

    def proxy_url(self, exit_name: str) -> str:
        '''``socks5h://`` (remote DNS, see MullvadSocks5Resolver) to the exit's port.'''
        try:
            port = self._ports[exit_name]
        except KeyError as exc:
            raise DiscordBotException(f'No egress_proxy port configured for exit {exit_name!r}') from exc
        return f'socks5h://{self._host}:{port}'


# Non-default egress modes -> exit-proxy resolver. Register a VPN/proxy provider
# here and select it with music.download.egress_mode.
EXIT_PROXY_RESOLVERS = {
    'mullvad-socks5': MullvadSocks5Resolver,
    'cluster-socks5': ClusterSocks5Resolver,
}


def build_exit_resolver(egress_mode: str, egress_proxy: dict | None = None) -> ExitProxyResolver:
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
    return resolver_cls.from_config(egress_proxy or {})


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

    def is_proxy_unreachable(self, error: BaseException) -> bool:
        '''Whether the resolver's proxy could not be reached (see ExitProxyResolver).'''
        return self._resolver.is_proxy_unreachable(error)

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

    def is_proxy_unreachable(self, _error: BaseException) -> bool:
        '''
        True when a download failed because the proxy in front of the exits could
        not be reached, as opposed to anything an exit did.  Only a pool whose proxy
        is a separate service can tell; the default is False.
        '''
        return False

    def client_for_exit(self, _exit_name: str):
        '''
        The client pinned to one exit, for out-of-band use (IP attribution).

        Exposed on the strategy so callers never reach through to the client
        cache: a pool owns how an exit maps to a client, and the fixed proxy has
        no per-exit notion at all.  Defaults to None for the modes that don't.
        '''
        return None


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

    def is_proxy_unreachable(self, error: BaseException) -> bool:
        '''Delegate to the resolver via the client cache.'''
        return self._clients.is_proxy_unreachable(error)

    def client_for_exit(self, exit_name: str):
        '''The cached client routing out ``exit_name`` (same one its downloads use).'''
        return self._clients.for_exit(exit_name)
