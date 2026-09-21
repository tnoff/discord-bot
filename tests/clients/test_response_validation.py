'''
Attribution for a peer response this build cannot parse.

The third class of seam failure, and the one the route check structurally
cannot see. Criteria five and six cover a route that stopped existing; this
covers a route that still exists and answers, whose body has drifted. A
route-set comparison has nothing to compare there -- only a real response shows
it, and only after it arrives.

The mechanism is deliberately not a new representation. `seam` and `prefix` are
read off the client, which already declares them for the route check, so a new
client gets correct attribution from the declaration it had to make anyway.
'''
import ast
from pathlib import Path

import pytest
from pydantic import BaseModel, ValidationError

from discord_bot.seams.broker.clients.http_client_base import HttpClientMixin
from discord_bot.services.bot.clients.http_markov_store import HttpMarkovStore
from discord_bot.core.exceptions import SeamResponseInvalid

#: Every `clients` package in the tree, not one hard-coded directory. The
#: per-image-code-split moved the helper and two more clients into
#: `seams/*/clients/`, and a scan pinned to `discord_bot/clients/` silently
#: stopped covering them -- a guard that keeps passing while checking less is
#: the failure this file exists to prevent.
REPO_ROOT = Path(__file__).resolve().parents[2]
CLIENT_DIRS = sorted(REPO_ROOT.glob('discord_bot/**/clients'))

#: The one module allowed to call model_validate directly -- it is the helper.
VALIDATION_HOME = 'http_client_base.py'


def _client_modules():
    '''Every client module in the tree, helper excluded.'''
    return sorted(path for directory in CLIENT_DIRS for path in directory.glob('*.py')
                  if path.name not in {VALIDATION_HOME, '__init__.py'})


def _validation_home():
    '''The helper, wherever it currently lives.'''
    found = [directory / VALIDATION_HOME for directory in CLIENT_DIRS
             if (directory / VALIDATION_HOME).is_file()]
    assert len(found) == 1, f'expected exactly one {VALIDATION_HOME}, found {found}'
    return found[0]


class _Body(BaseModel):
    count: int


class _Double(HttpClientMixin):
    """Stands in for the eight real clients.

    `parse` exists so the tests reach the helper the way a client does -- from
    inside a subclass -- rather than poking a protected member from outside.
    """
    SEAM = 'database'
    ROUTE_PREFIX = '/database/markov'

    def parse(self, model, payload):
        return self._validate(model, payload)


class _RootSeamDouble(_Double):
    """Four of the five seams are served by one pod at the root, with no prefix."""
    SEAM = 'broker'
    ROUTE_PREFIX = ''


def _direct_validate_calls(path: Path):
    '''Every `Something.model_validate(...)` in one module, as line numbers.'''
    tree = ast.parse(path.read_text())
    return [node.lineno for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == 'model_validate']


def test_no_client_validates_a_response_outside_the_helper():
    '''The guard that makes this un-driftable.

    Eighteen call sites were converted by hand. Without this, the nineteenth --
    added with a new store, or a new model on an existing one -- silently goes
    back to raising a bare ValidationError that names no peer, and nothing fails.
    That is the same shape as the eight dark gauges: correct code, quietly not
    doing the thing it was written for.
    '''
    scanned = _client_modules()
    assert len(scanned) > 10, f'only {len(scanned)} client modules scanned -- the glob missed some'
    offenders = {}
    for path in scanned:
        lines = _direct_validate_calls(path)
        if lines:
            offenders[str(path.relative_to(REPO_ROOT))] = lines
    assert not offenders, (
        f'call self._validate(Model, payload) instead of Model.model_validate: '
        f'{offenders}')


def test_the_helper_is_the_only_exemption_and_still_validates():
    '''Guards the guard: an exemption that stopped being used would hide a gap.'''
    assert _direct_validate_calls(_validation_home()), \
        'the exemption is unused -- delete it or find where validation moved'


def test_a_good_body_is_returned_unwrapped():
    assert _Double().parse(_Body, {'count': 3}).count == 3


def test_a_bad_body_names_the_peer_not_just_the_model():
    '''The whole point. A bare ValidationError says a _Body was malformed; it
    does not say which of three stores behind one envelope type sent it.'''
    with pytest.raises(SeamResponseInvalid) as caught:
        _Double().parse(_Body, {'count': 'not-an-int'})
    error = caught.value
    assert error.seam == 'database'
    assert error.prefix == '/database/markov'
    assert error.model is _Body
    assert 'database/database/markov' in str(error)
    assert '_Body' in str(error)


def test_the_original_validation_error_is_kept():
    '''The field-level report is the actionable part; wrapping must not lose it.'''
    with pytest.raises(SeamResponseInvalid) as caught:
        _Double().parse(_Body, {'count': 'not-an-int'})
    assert isinstance(caught.value.validation_error, ValidationError)
    assert isinstance(caught.value.__cause__, ValidationError)


def test_a_seam_with_no_prefix_reads_cleanly():
    '''Four of the five seams are served by one pod at the root.'''
    with pytest.raises(SeamResponseInvalid) as caught:
        _RootSeamDouble().parse(_Body, {})
    assert caught.value.prefix == ''
    assert str(caught.value).startswith('peer on seam broker ')


def test_the_counter_is_labelled_per_seam(monkeypatch):
    '''Attribution has to reach Mimir, not only the log line.'''
    recorded = []
    monkeypatch.setattr(
        'discord_bot.seams.broker.clients.http_client_base._RESPONSE_INVALID_COUNTER',
        type('_C', (), {'add': lambda _self, amount, attrs: recorded.append((amount, attrs))})())
    with pytest.raises(SeamResponseInvalid):
        _Double().parse(_Body, {})
    assert recorded == [(1, {'seam': 'database',
                             'seam_prefix': '/database/markov',
                             'seam_response_model': '_Body'})]


@pytest.mark.asyncio(loop_scope='session')
async def test_a_drifted_row_is_blamed_on_the_db_peer(monkeypatch):
    '''End to end on the path that matters: the db pod answers, the ENVELOPE is
    valid, and the row inside it is what drifted. The envelope check in
    HttpStoreBase._call cannot catch this -- DatabaseResponse.result is Any --
    so covering only that choke point would have missed exactly this case.
    '''
    store = HttpMarkovStore('http://discord-db:8085')

    async def _fake_call_route(_route, _body=None, **_kwargs):
        return {'result': {'not': 'a markov entry'}}

    monkeypatch.setattr(store, '_call_route', _fake_call_route)
    with pytest.raises(SeamResponseInvalid) as caught:
        await store.get_channel(1, 2)
    assert caught.value.seam == 'database'
    assert caught.value.prefix == '/database/markov'
