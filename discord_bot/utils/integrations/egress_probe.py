'''
Background probe for the live network egress exit.

The downloader tunnels yt-dlp through an HTTP proxy (``extra_ytdlp_options
['proxy']``, e.g. ``http://discord-vpn:8888``).  When YouTube flags a download,
the culprit is usually the egress IP that proxy is currently routed through, not
the bot.  An ExitProbe periodically asks an IP-reporting endpoint — THROUGH THE
SAME PROXY — which exit it is on, and caches the answer so the download path can
stamp every failure span/log with the egress that was live.

``ExitProbe`` is the provider-agnostic base: it owns the proxy plumbing, the
cache, and the poll loop.  A concrete probe only declares its endpoint
(``PROBE_URL``) and how to pull ``(hostname, ip)`` out of the payload
(``_parse``).  ``MullvadExitProbe`` is the first implementation.  Add another VPN
or proxy by subclassing + registering in ``EXIT_PROBE_TYPES``, then select it with
the ``music.download.egress_probe`` config key (absent => attribution disabled).

Mirrors workers/download_metrics.py: a ``run(stop_event, interval)`` loop that
refreshes a cached value plus sync cached accessors.  The exit is stable for the
proxy pod's life, so a slow interval (default 300s) is plenty.

Robustness is the whole point: a probe failure must NEVER fail a download.
refresh() raises on HTTP/parse errors, but run() swallows them and keeps the
last-known value (``None`` until the first success), exactly like DownloadMetrics.
'''
import asyncio
import json
import logging
from abc import ABC, abstractmethod

import aiohttp

from discord_bot.exceptions import DiscordBotException

logger = logging.getLogger(__name__)

MULLVAD_JSON_URL = 'https://am.i.mullvad.net/json'
#: Field in the Mullvad payload holding the caller's public (exit) IP.
MULLVAD_IP_FIELD = 'ip'
DEFAULT_POLL_INTERVAL_SECONDS = 300.0
DEFAULT_REQUEST_TIMEOUT_SECONDS = 10.0
UNKNOWN_EXIT = 'unknown'


def _default_session_factory() -> aiohttp.ClientSession:
    '''Build a short-timeout aiohttp session for a single probe request.'''
    timeout = aiohttp.ClientTimeout(total=DEFAULT_REQUEST_TIMEOUT_SECONDS)
    return aiohttp.ClientSession(timeout=timeout)


def _clean_str(value) -> str | None:
    '''
    Normalize a probe field to a non-empty, stripped string, or None.

    The IP-reporting endpoint is external and untrusted: a field may be absent,
    null, a number, or blank.  Anything that isn't a usable string collapses to
    None (=> UNKNOWN_EXIT downstream) so the download path never stamps junk.
    '''
    if not isinstance(value, str):
        return None
    value = value.strip()
    return value or None


class ExitProbe(ABC):
    '''
    Provider-agnostic background probe for the live network egress exit.

    Subclasses declare ``PROBE_URL`` (the IP-reporting endpoint) and ``_parse``
    (pull ``(hostname, ip)`` out of its JSON).  The base owns everything else:
    issuing the request through the download proxy, caching the last-known exit,
    and the run() poll loop that never lets a probe failure reach the download
    path.
    '''

    #: IP-reporting endpoint, requested through the proxy; set by each subclass.
    PROBE_URL: str = ''

    def __init__(self, proxy: str | None, session_factory=_default_session_factory):
        '''
        proxy : the same HTTP proxy string yt-dlp uses (extra_ytdlp_options['proxy']);
                the probe request is issued through it so the answer matches the exit
                the download traffic actually leaves from.  None => direct request.
        session_factory : builds the aiohttp session (injectable for tests).
        '''
        self._proxy = proxy
        self._session_factory = session_factory
        self._exit_hostname: str | None = None
        self._exit_ip: str | None = None

    @property
    def exit_hostname(self) -> str | None:
        '''Cached egress exit hostname (e.g. us-lax-wg-101), or None if unknown.'''
        return self._exit_hostname

    @property
    def exit_ip(self) -> str | None:
        '''Cached egress exit IP, or None if never successfully probed.'''
        return self._exit_ip

    @abstractmethod
    def _parse(self, data: dict) -> tuple[str | None, str | None]:
        '''Pull ``(exit_hostname, exit_ip)`` out of the provider's JSON payload.'''

    async def _fetch_json(self) -> dict:
        '''Issue the probe request through the proxy and return the parsed JSON.'''
        async with self._session_factory() as session:
            async with session.get(self.PROBE_URL, proxy=self._proxy) as resp:
                resp.raise_for_status()
                return await resp.json()

    async def refresh(self) -> None:
        '''
        Fetch the current exit and update the cache.

        Failsafe against an unexpected payload: a non-dict body or non-string /
        blank fields normalize to None (=> UNKNOWN_EXIT downstream) instead of
        caching junk.  Genuine request/JSON errors still raise — run() is
        responsible for catching those and keeping the previous values.
        '''
        data = await self._fetch_json()
        hostname, exit_ip = self._parse(data if isinstance(data, dict) else {})
        self._exit_hostname = _clean_str(hostname)
        self._exit_ip = _clean_str(exit_ip)

    async def run(self, stop_event: asyncio.Event,
                  interval: float = DEFAULT_POLL_INTERVAL_SECONDS) -> None:
        '''
        Refresh on each tick until stop_event is set.

        A probe failure is logged and the last-known value is kept; it never
        propagates into the download path.  The inter-tick wait is a bounded wait
        on stop_event so a shutdown is picked up immediately.
        '''
        while not stop_event.is_set():
            try:
                await self.refresh()
            except Exception as exc:
                # Best-effort probe: a proxy blip / gluetun tunnel re-establish
                # (ConnectionRefused) or an odd response is expected and tolerated,
                # so keep the last-known exit and log at WARNING with a one-line
                # summary — not logger.exception(), whose ERROR + full stacktrace
                # every tick would spam error dashboards for a non-error condition.
                logger.warning('%s :: refresh failed (%s); keeping last value',
                               type(self).__name__, exc)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass


class MullvadExitProbe(ExitProbe):
    '''Reports the live Mullvad exit via am.i.mullvad.net/json (through the proxy).'''

    PROBE_URL = MULLVAD_JSON_URL

    def _parse(self, data: dict) -> tuple[str | None, str | None]:
        '''
        Mullvad's JSON names the exit ``mullvad_exit_ip_hostname`` + ``ip``.

        ``data`` is always a dict (the base coerces a non-dict body to ``{}``) and
        the base normalizes the returned values, so a plain ``.get`` is safe here.
        '''
        return data.get('mullvad_exit_ip_hostname'), data.get('ip')


class PoolExitIpProbe:
    '''
    Resolve and cache the public IP each POOL exit egresses from.

    ``ExitProbe`` answers "which exit is the one fixed proxy on".  A pool needs a
    different question answered — "what IP does each leased exit leave from" —
    because every download picks its own exit, so a single cached answer would be
    wrong for all but one of them.

    It cannot reuse ExitProbe's aiohttp transport: pool exits are SOCKS5 and
    aiohttp's ``proxy=`` speaks only HTTP proxies (aiohttp-socks is not a
    dependency).  Probing through the exit's own yt-dlp client avoids adding one
    and, more to the point, asks the question over the exact transport the
    download uses — so the answer is the IP the origin actually sees, which is
    the only version of it worth stamping on a span.

    Same robustness contract as ExitProbe: a probe failure NEVER reaches the
    download path.  ``ip_for`` returns None until an exit has been resolved, and
    a failed refresh keeps the last-known value.
    '''

    def __init__(self, exit_names, client_for_exit,
                 probe_url: str = MULLVAD_JSON_URL, ip_field: str = MULLVAD_IP_FIELD):
        '''
        exit_names : the pool's exit ids to resolve.
        client_for_exit : ``(exit_name) -> yt-dlp client`` pinned to that exit's proxy.
        probe_url / ip_field : IP-reporting endpoint and the field holding the IP.
        '''
        self._exit_names = tuple(exit_names)
        self._client_for_exit = client_for_exit
        self._probe_url = probe_url
        self._ip_field = ip_field
        self._by_exit: dict = {}

    def ip_for(self, exit_name: str) -> str | None:
        '''Cached IP for one exit, or None until it has been probed successfully.'''
        return self._by_exit.get(exit_name)

    def _fetch_ip(self, exit_name: str) -> str | None:
        '''
        Blocking: fetch the probe payload through this exit's yt-dlp client.

        Runs off the event loop (see refresh) because yt-dlp's urlopen is sync.
        '''
        client = self._client_for_exit(exit_name)
        with client.urlopen(self._probe_url) as response:
            data = json.loads(response.read())
        if not isinstance(data, dict):
            return None
        return _clean_str(data.get(self._ip_field))

    async def refresh(self) -> None:
        '''
        Re-probe every exit, keeping the last-known IP for any that fails.

        Exits are probed one at a time rather than gathered: this is background
        attribution, and firing every exit's request at once would put a burst
        through the same relays the downloads are using.
        '''
        for exit_name in self._exit_names:
            try:
                exit_ip = await asyncio.to_thread(self._fetch_ip, exit_name)
            except Exception as exc:
                # Same call as ExitProbe.run: a relay blip is expected, so warn with
                # a one-line summary instead of an ERROR + stacktrace every tick.
                logger.warning('PoolExitIpProbe :: %s probe failed (%s); keeping last value',
                               exit_name, exc)
                continue
            if exit_ip:
                self._by_exit[exit_name] = exit_ip

    async def run(self, stop_event: asyncio.Event,
                  interval: float = DEFAULT_POLL_INTERVAL_SECONDS) -> None:
        '''Refresh on each tick until stop_event is set; failures never propagate.'''
        while not stop_event.is_set():
            try:
                await self.refresh()
            except Exception as exc:
                logger.warning('PoolExitIpProbe :: refresh failed (%s); keeping last values', exc)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass


#: Selectable egress probes, keyed by the ``music.download.egress_probe`` config value.
EXIT_PROBE_TYPES = {
    'mullvad': MullvadExitProbe,
}


def build_exit_probe(probe_type: str | None, proxy: str | None) -> 'ExitProbe | None':
    '''
    Build the configured egress probe, or None when attribution is disabled.

    probe_type : the ``music.download.egress_probe`` config value (e.g. 'mullvad').
                 Falsy => no probe; the download path attributes to UNKNOWN_EXIT.
    proxy      : the yt-dlp proxy string the probe requests through.

    Raises DiscordBotException on an unknown probe_type so a config typo fails
    loudly at startup rather than silently disabling exit attribution.
    '''
    if not probe_type:
        return None
    try:
        probe_cls = EXIT_PROBE_TYPES[probe_type]
    except KeyError as exc:
        known = ', '.join(sorted(EXIT_PROBE_TYPES))
        raise DiscordBotException(
            f'Unknown egress_probe {probe_type!r}; known types: {known}') from exc
    return probe_cls(proxy)


def cached_exit_attributes(probe: 'ExitProbe | None') -> tuple[str, str]:
    '''
    Return (exit_hostname, exit_ip) from the probe, falling back to UNKNOWN_EXIT.

    Tolerates a None probe (the in-process/bot path constructs no probe) and a
    probe that has not yet had a successful refresh.
    '''
    if probe is None:
        return UNKNOWN_EXIT, UNKNOWN_EXIT
    return (probe.exit_hostname or UNKNOWN_EXIT, probe.exit_ip or UNKNOWN_EXIT)


def cached_exit_hostname(probe: 'ExitProbe | None') -> str:
    '''Return the cached exit hostname, or UNKNOWN_EXIT when unavailable.'''
    return cached_exit_attributes(probe)[0]
