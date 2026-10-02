'''
`declared_core_pin` and `find_mismatches` are pure, so most cases are stated
directly against small `pyproject.toml` fixtures or plain dicts -- the same
approach `test_package_versions.py` uses for `touched_packages`. `main`
reads real files under a throwaway tree instead, the same way
`test_package_versions.py` exercises `main` against a throwaway git repo:
there is no fixture tree in this repo small enough to assert fixed outcomes
against, and the real six-pod tree is what this check runs against in CI,
not what its own tests should depend on the shape of.
'''
import pytest

import tests.cli._package_pins as package_pins
from tests.cli._package_pins import declared_core_pin, find_mismatches, main


def _write_pyproject(root, name=None, dependencies=None):
    '''A minimal pyproject.toml under `root`, with an optional [project] name
    and/or dependencies list -- whichever this test actually needs.'''
    root.mkdir(parents=True, exist_ok=True)
    lines = ['[project]']
    if name is not None:
        lines.append(f'name = "{name}"')
    if dependencies is not None:
        lines.append('dependencies = [')
        lines.extend(f'    "{dep}",' for dep in dependencies)
        lines.append(']')
    (root / 'pyproject.toml').write_text('\n'.join(lines) + '\n', encoding='utf-8')


@pytest.fixture
def repo(tmp_path, monkeypatch):
    '''A throwaway tree: discord_core (declaring its own project name) plus
    three fake pods (`pod_a`, `pod_b`, `pod_c`), wired in as
    `_package_pins.REPO_ROOT` / `CORE_ROOT` / `POD_ROOTS` so these tests never
    depend on this repo's real six pods.'''
    monkeypatch.setattr(package_pins, 'REPO_ROOT', tmp_path)
    monkeypatch.setattr(package_pins, 'CORE_ROOT', 'discord_core')
    monkeypatch.setattr(package_pins, 'POD_ROOTS',
                         {'a': 'pod_a', 'b': 'pod_b', 'c': 'pod_c'})
    _write_pyproject(tmp_path / 'discord_core', name='discord-core')
    return tmp_path


def test_declared_core_pin_reads_the_matching_entry(repo):  # pylint: disable=redefined-outer-name
    '''The one `discord-core @ ...` entry in a pod's own dependencies list.'''
    _write_pyproject(repo / 'pod_a', dependencies=[
        'discord-core @ https://example.test/core-v1.0.0.tar.gz',
        'other-package==1.0',
    ])
    assert (declared_core_pin('pod_a')
            == 'discord-core @ https://example.test/core-v1.0.0.tar.gz')


def test_declared_core_pin_is_not_hardcoded_to_the_literal_name(repo):  # pylint: disable=redefined-outer-name
    '''The prefix it matches on comes from discord_core's OWN declared name,
    not a literal "discord-core" baked into this module.'''
    _write_pyproject(repo / 'discord_core', name='widget-core')
    _write_pyproject(repo / 'pod_a', dependencies=[
        'widget-core @ https://example.test/core-v1.0.0.tar.gz',
    ])
    assert (declared_core_pin('pod_a')
            == 'widget-core @ https://example.test/core-v1.0.0.tar.gz')


def test_declared_core_pin_returns_none_when_absent(repo):  # pylint: disable=redefined-outer-name
    '''A pod with no discord-core dependency at all -- not an error here,
    `find_mismatches` is what turns that into a failure.'''
    _write_pyproject(repo / 'pod_a', dependencies=['other-package==1.0'])
    assert declared_core_pin('pod_a') is None


def test_declared_core_pin_raises_on_a_duplicate_entry(repo):  # pylint: disable=redefined-outer-name
    '''Two discord-core entries in the same pod is a different bug (pip would
    already refuse to install it) -- surfaced loudly, not silently resolved
    by picking the first match.'''
    _write_pyproject(repo / 'pod_a', dependencies=[
        'discord-core @ https://example.test/core-v1.0.0.tar.gz',
        'discord-core @ https://example.test/core-v1.0.1.tar.gz',
    ])
    with pytest.raises(ValueError, match='more than once'):
        declared_core_pin('pod_a')


@pytest.mark.parametrize('pins, expected', [
    # Every pod agrees: no mismatches at all.
    ({'a': 'core-v1.0.0', 'b': 'core-v1.0.0', 'c': 'core-v1.0.0'}, {}),
    # One pod lags behind the other two.
    ({'a': 'core-v1.0.0', 'b': 'core-v1.0.0', 'c': 'core-v0.9.0'},
     {'c': 'core-v0.9.0'}),
    # One pod declares no pin at all.
    ({'a': 'core-v1.0.0', 'b': None, 'c': 'core-v1.0.0'}, {'b': None}),
    # No majority to defer to: every value differs, so every pod not picked
    # as the (arbitrary) majority is reported -- the honest answer when there
    # is no real consensus to measure against.
    ({'a': 'core-v1.0.0', 'b': 'core-v1.1.0', 'c': 'core-v1.2.0'},
     {'b': 'core-v1.1.0', 'c': 'core-v1.2.0'}),
    # A missing pin alongside an actual disagreement -- both are reported.
    ({'a': 'core-v1.0.0', 'b': None, 'c': 'core-v1.2.0'},
     {'b': None, 'c': 'core-v1.2.0'}),
    # Nobody declares a pin at all.
    ({'a': None, 'b': None}, {'a': None, 'b': None}),
])
def test_find_mismatches(pins, expected):
    '''The pods that disagree with the majority pin, by value.'''
    assert find_mismatches(pins) == expected


def test_main_passes_when_every_pod_agrees(repo, capsys):  # pylint: disable=redefined-outer-name
    '''The happy path: all three fake pods pin the identical tag.'''
    for pod in ('pod_a', 'pod_b', 'pod_c'):
        _write_pyproject(repo / pod, dependencies=[
            'discord-core @ https://example.test/core-v1.0.0.tar.gz',
        ])

    assert main([]) == 0
    assert 'every pod pins the same discord-core tag' in capsys.readouterr().err


def test_main_fails_and_names_the_disagreeing_pod(repo, capsys):  # pylint: disable=redefined-outer-name
    '''The drift case this job exists to catch: one pod's pin disagrees.'''
    _write_pyproject(repo / 'pod_a', dependencies=[
        'discord-core @ https://example.test/core-v1.0.0.tar.gz',
    ])
    _write_pyproject(repo / 'pod_b', dependencies=[
        'discord-core @ https://example.test/core-v1.0.0.tar.gz',
    ])
    _write_pyproject(repo / 'pod_c', dependencies=[
        'discord-core @ https://example.test/core-v0.9.0.tar.gz',
    ])

    assert main([]) == 1
    err = capsys.readouterr().err
    problem = err.split('ERROR:', maxsplit=1)[1]
    assert 'pod_c' in problem
    assert 'core-v0.9.0' in problem
    # The agreeing pods are not named as part of the problem section -- only
    # in the unconditional "declared pins" log above it.
    assert 'pod_a: ' not in problem
    assert 'pod_b: ' not in problem


def test_main_fails_when_a_pod_declares_no_pin_at_all(repo, capsys):  # pylint: disable=redefined-outer-name
    '''A pod that forgot the dependency entirely, not just a wrong tag.'''
    _write_pyproject(repo / 'pod_a', dependencies=[
        'discord-core @ https://example.test/core-v1.0.0.tar.gz',
    ])
    _write_pyproject(repo / 'pod_b', dependencies=[
        'discord-core @ https://example.test/core-v1.0.0.tar.gz',
    ])
    _write_pyproject(repo / 'pod_c', dependencies=['other-package==1.0'])

    assert main([]) == 1
    err = capsys.readouterr().err
    assert 'pod_c' in err
    assert 'no discord-core dependency declared at all' in err
