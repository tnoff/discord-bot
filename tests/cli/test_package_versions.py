'''
`touched_packages` is pure path-string mapping, so those cases are stated
directly, the same way `test_affected_images.py` states `module_for`'s cases
without staging a single commit. `main` shells out to `git diff`, so its
pass/fail behaviour is exercised against a real throwaway git repo instead --
there is no fixture git history in this repo small enough to assert fixed
outcomes against, and a hand-built closure the way `test_affected_images.py`
uses for `affected()` is not available here: this module has no measured
graph to fake, only a changed-file list and a VERSION-file convention.
'''
import subprocess  # nosec B404 - fixed argv, no shell, test-only git setup

import pytest

import tests.cli._package_versions as package_versions
from tests.cli._package_versions import main, touched_packages


@pytest.mark.parametrize('paths, expected', [
    # A core file.
    (['discord_core/utils/otel.py'], {'discord_core'}),
    # A pod file.
    (['discord_search/clients/youtube.py'], {'discord_search'}),
    # A package's own nested tests/ -- counts the same as production code.
    (['discord_core/tests/utils/test_otel.py'], {'discord_core'}),
    # A central tests/ file under no declared root.
    (['tests/helpers.py'], set()),
    # Root-level doc and workflow files, under no root either.
    (['README.md', '.github/workflows/ci.yml'], set()),
    # Two roots touched by the same change set.
    (['discord_core/utils/otel.py', 'discord_search/clients/youtube.py'],
     {'discord_core', 'discord_search'}),
    # No files at all.
    ([], set()),
])
def test_touched_packages(paths, expected):
    '''Paths map onto the distinct package roots they sit under.'''
    assert touched_packages(paths) == expected


def _git(cwd, *args):
    subprocess.run(['git', *args], cwd=cwd, check=True,  # nosec B603 B607 - fixed argv
                   capture_output=True, text=True)


def _head(cwd):
    return subprocess.run(  # nosec B603 B607 - fixed argv
        ['git', 'rev-parse', 'HEAD'], cwd=cwd, check=True,
        capture_output=True, text=True).stdout.strip()


@pytest.fixture
def repo(tmp_path, monkeypatch):
    '''A throwaway git repo, wired in as `_package_versions.REPO_ROOT`, with
    one base commit holding two packages' VERSION files.'''
    monkeypatch.setattr(package_versions, 'REPO_ROOT', tmp_path)
    _git(tmp_path, 'init', '-q', '-b', 'main')
    _git(tmp_path, 'config', 'user.email', 'test@example.com')
    _git(tmp_path, 'config', 'user.name', 'Test')
    (tmp_path / 'discord_core').mkdir()
    (tmp_path / 'discord_core' / 'VERSION').write_text('1.0.0\n', encoding='utf-8')
    (tmp_path / 'discord_search').mkdir()
    (tmp_path / 'discord_search' / 'VERSION').write_text('1.0.0\n', encoding='utf-8')
    (tmp_path / 'README.md').write_text('hello\n', encoding='utf-8')
    _git(tmp_path, 'add', '-A')
    _git(tmp_path, 'commit', '-q', '-m', 'base')
    return tmp_path, _head(tmp_path)


def test_main_passes_when_every_touched_package_bumps_its_own_version(repo, capsys):  # pylint: disable=redefined-outer-name
    '''The convention's happy path: one package touched, its own VERSION too.'''
    tmp_path, base_sha = repo
    (tmp_path / 'discord_search' / 'clients.py').write_text('x = 1\n', encoding='utf-8')
    (tmp_path / 'discord_search' / 'VERSION').write_text('1.0.1\n', encoding='utf-8')
    _git(tmp_path, 'add', '-A')
    _git(tmp_path, 'commit', '-q', '-m', 'bump search')

    assert main(['--base', base_sha]) == 0
    assert 'every touched package bumped' in capsys.readouterr().err


def test_main_fails_when_a_touched_package_does_not_bump_its_version(repo, capsys):  # pylint: disable=redefined-outer-name
    '''The forgotten-bump case this job exists to catch.'''
    tmp_path, base_sha = repo
    (tmp_path / 'discord_search' / 'clients.py').write_text('x = 1\n', encoding='utf-8')
    _git(tmp_path, 'add', '-A')
    _git(tmp_path, 'commit', '-q', '-m', 'forgot to bump')

    assert main(['--base', base_sha]) == 1
    assert 'discord_search' in capsys.readouterr().err


def test_main_passes_when_nothing_package_shaped_changed(repo):  # pylint: disable=redefined-outer-name
    '''A doc-only change touches no declared root, so nothing is required.'''
    tmp_path, base_sha = repo
    (tmp_path / 'README.md').write_text('hello again\n', encoding='utf-8')
    _git(tmp_path, 'add', '-A')
    _git(tmp_path, 'commit', '-q', '-m', 'docs only')

    assert main(['--base', base_sha]) == 0


def test_main_requires_only_the_touched_packages_own_version(repo):  # pylint: disable=redefined-outer-name
    '''Touching two packages requires both VERSIONs, not a third untouched one.'''
    tmp_path, base_sha = repo
    (tmp_path / 'discord_core' / 'utils.py').write_text('x = 1\n', encoding='utf-8')
    (tmp_path / 'discord_core' / 'VERSION').write_text('1.0.1\n', encoding='utf-8')
    (tmp_path / 'discord_search' / 'clients.py').write_text('x = 1\n', encoding='utf-8')
    (tmp_path / 'discord_search' / 'VERSION').write_text('1.0.1\n', encoding='utf-8')
    _git(tmp_path, 'add', '-A')
    _git(tmp_path, 'commit', '-q', '-m', 'bump both')

    assert main(['--base', base_sha]) == 0
