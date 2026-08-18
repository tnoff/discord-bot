'''Tests for the per-download egress exit pool + pluggable proxy resolvers.'''
import pytest

from discord_bot.exceptions import DiscordBotException
from discord_bot.utils.integrations.egress_pool import (
    EGRESS_MODE_HTTP_PROXY, ExitClients, ExitPool, HttpProxyEgress, PoolEgress,
    MullvadSocks5Resolver, build_exit_resolver, mullvad_socks5_endpoint,
)


class _FakeClient:
    '''Stand-in for a YoutubeDL, recording the opts it was built with.'''
    def __init__(self, opts):
        self.opts = opts


async def _reserve_ok(_exit):
    '''reserve stub: every exit is reservable.'''
    return True


def _reserve_except(*exits):
    '''Build a reserve callable that refuses (can't claim) the given exits.'''
    blocked = set(exits)

    async def _check(exit_name):
        return exit_name not in blocked

    return _check


# --------------------------------------------------------------------------- #
# Mullvad resolver + provider registry
# --------------------------------------------------------------------------- #

def test_mullvad_socks5_endpoint_derivation():
    '''The server name gains a socks5- segment before its trailing index.'''
    assert mullvad_socks5_endpoint('us-nyc-wg-301') == 'us-nyc-wg-socks5-301.relays.mullvad.net'
    assert mullvad_socks5_endpoint('se-mma-wg-004') == 'se-mma-wg-socks5-004.relays.mullvad.net'


@pytest.mark.parametrize('bad', ['us-nyc-wg-abc', 'nodash', 'us-nyc-wg-'])
def test_mullvad_socks5_endpoint_rejects_bad_names(bad):
    '''A name not ending in -<digits> fails loudly rather than yielding a dead host.'''
    with pytest.raises(ValueError):
        mullvad_socks5_endpoint(bad)


def test_mullvad_resolver_proxy_url():
    '''The Mullvad resolver maps an exit to its socks5h endpoint on 1080.'''
    assert MullvadSocks5Resolver().proxy_url('us-lax-wg-001') == \
        'socks5h://us-lax-wg-socks5-001.relays.mullvad.net:1080'


def test_build_exit_resolver_selects_mullvad_socks5():
    '''egress_mode=mullvad-socks5 builds a MullvadSocks5Resolver.'''
    assert isinstance(build_exit_resolver('mullvad-socks5'), MullvadSocks5Resolver)


def test_build_exit_resolver_unknown_mode_raises():
    '''An unknown egress_mode fails loudly, naming the known modes.'''
    with pytest.raises(DiscordBotException) as exc:
        build_exit_resolver('nordvpn-socks5')
    assert EGRESS_MODE_HTTP_PROXY in str(exc.value)
    assert 'mullvad-socks5' in str(exc.value)


# --------------------------------------------------------------------------- #
# ExitPool — generic, provider-agnostic
# --------------------------------------------------------------------------- #

def test_empty_pool_raises():
    '''An empty exit pool is a config error.'''
    with pytest.raises(ValueError):
        ExitPool([])


@pytest.mark.asyncio
async def test_lease_returns_free_healthy_and_marks_leased():
    '''lease() hands out a healthy exit and records it as leased.'''
    pool = ExitPool(['a', 'b'])
    got = await pool.lease(_reserve_ok)
    assert got in {'a', 'b'}
    assert pool.leased == frozenset({got})


@pytest.mark.asyncio
async def test_lease_round_robins_across_calls():
    '''Successive leases rotate through the pool rather than repeating one.'''
    pool = ExitPool(['a', 'b', 'c'])
    got = [await pool.lease(_reserve_ok) for _ in range(3)]
    assert got == ['a', 'b', 'c']


@pytest.mark.asyncio
async def test_lease_skips_leased_then_none_when_exhausted():
    '''Once every exit is leased, lease() returns None.'''
    pool = ExitPool(['a', 'b'])
    await pool.lease(_reserve_ok)
    await pool.lease(_reserve_ok)
    assert pool.leased == frozenset({'a', 'b'})
    assert await pool.lease(_reserve_ok) is None


@pytest.mark.asyncio
async def test_lease_skips_backed_off():
    '''A backed-off exit is skipped; only the healthy one is leased.'''
    pool = ExitPool(['a', 'b'])
    got = await pool.lease(_reserve_except('a'))
    assert got == 'b'
    assert await pool.lease(_reserve_except('a')) is None


@pytest.mark.asyncio
async def test_release_frees_the_exit():
    '''release() returns an exit to the pool (and is idempotent).'''
    pool = ExitPool(['a'])
    await pool.lease(_reserve_ok)
    assert await pool.lease(_reserve_ok) is None
    pool.release('a')
    pool.release('a')
    assert pool.leased == frozenset()
    assert await pool.lease(_reserve_ok) == 'a'


# --------------------------------------------------------------------------- #
# ExitClients — client per exit via the resolver
# --------------------------------------------------------------------------- #

def test_exit_clients_builds_with_resolver_proxy():
    '''for_exit builds a client whose proxy comes from the resolver.'''
    clients = ExitClients({'format': 'bestaudio/best'}, MullvadSocks5Resolver(),
                          client_factory=_FakeClient)
    client = clients.for_exit('us-lax-wg-001')
    assert client.opts['proxy'] == 'socks5h://us-lax-wg-socks5-001.relays.mullvad.net:1080'
    assert client.opts['format'] == 'bestaudio/best'


def test_exit_clients_caches_per_exit():
    '''The same exit returns the same cached client; a different exit gets its own.'''
    built = []
    clients = ExitClients({}, MullvadSocks5Resolver(),
                          client_factory=lambda opts: built.append(opts) or _FakeClient(opts))
    first = clients.for_exit('us-lax-wg-001')
    assert clients.for_exit('us-lax-wg-001') is first
    other = clients.for_exit('us-nyc-wg-301')
    assert other is not first
    assert len(built) == 2


def test_exit_clients_propagates_resolver_error():
    '''A malformed exit fails loudly via the resolver.'''
    clients = ExitClients({}, MullvadSocks5Resolver(), client_factory=_FakeClient)
    with pytest.raises(ValueError):
        clients.for_exit('not-a-server')


# --------------------------------------------------------------------------- #
# Egress strategies
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_http_proxy_egress_always_returns_the_client():
    '''HttpProxyEgress hands back the single client with no exit, and release is a no-op.'''
    sentinel = object()
    egress = HttpProxyEgress(sentinel)
    dl = await egress.acquire(_reserve_ok)
    assert dl.client is sentinel
    assert dl.exit_name is None
    egress.release(dl)  # no raise


@pytest.mark.asyncio
async def test_pool_egress_leases_client_and_releases():
    '''PoolEgress leases an exit, returns its client, and release frees the exit.'''
    pool = ExitPool(['us-lax-wg-001'])
    clients = ExitClients({}, MullvadSocks5Resolver(), client_factory=_FakeClient)
    egress = PoolEgress(pool, clients)
    dl = await egress.acquire(_reserve_ok)
    assert dl.exit_name == 'us-lax-wg-001'
    assert dl.client.opts['proxy'] == 'socks5h://us-lax-wg-socks5-001.relays.mullvad.net:1080'
    assert pool.leased == frozenset({'us-lax-wg-001'})
    egress.release(dl)
    assert pool.leased == frozenset()


@pytest.mark.asyncio
async def test_pool_egress_none_when_no_exit_available():
    '''PoolEgress returns None when every exit is backed off.'''
    egress = PoolEgress(ExitPool(['a']), ExitClients({}, MullvadSocks5Resolver(), client_factory=_FakeClient))
    assert await egress.acquire(_reserve_except('a')) is None


def test_exit_pool_exit_names_lists_all_in_order():
    '''exit_names exposes the pool's exits (used to size drivers + gate on free).'''
    assert ExitPool(['a', 'b', 'c']).exit_names == ('a', 'b', 'c')


@pytest.mark.asyncio
async def test_http_proxy_egress_is_not_pool():
    '''The fixed proxy is single-exit: not a pool, and exposes no exits to fan out.'''
    egress = HttpProxyEgress(object())
    assert egress.is_pool is False
    assert not egress.exit_names


def test_pool_egress_is_pool_and_exposes_exits():
    '''PoolEgress reports pool mode and forwards its exit ids from the pool.'''
    egress = PoolEgress(ExitPool(['a', 'b']),
                        ExitClients({}, MullvadSocks5Resolver(), client_factory=_FakeClient))
    assert egress.is_pool is True
    assert egress.exit_names == ('a', 'b')


def test_pool_egress_exposes_the_client_for_an_exit():
    '''
    The per-exit client is reachable through the strategy, and it is the SAME
    cached client that exit's downloads use.

    IP attribution is only meaningful if it is probed over the identical
    transport — a separately built client could resolve a different exit.
    '''
    clients = ExitClients({}, MullvadSocks5Resolver(), client_factory=_FakeClient)
    egress = PoolEgress(ExitPool(['us-lax-wg-001', 'us-nyc-wg-301']), clients)
    assert egress.client_for_exit('us-lax-wg-001') is clients.for_exit('us-lax-wg-001')
    assert egress.client_for_exit('us-lax-wg-001') is not egress.client_for_exit('us-nyc-wg-301')


def test_http_proxy_egress_has_no_per_exit_client():
    '''The fixed proxy has no per-exit notion, so it reports none.'''
    assert HttpProxyEgress(object()).client_for_exit('a') is None
