'''
`_affected_tests` decides which package test trees a change can affect, and the one way it
fails that matters is UNDER-selecting: a test that should have run, did not, and CI is green.

So most of this is stated against a small synthetic tree whose shape is the point:

    pkg_core   the core package; pods drop their edge to it
    pod_a      a pod whose source others use
    pod_b      imports pod_a directly in its tests
    pod_c      reaches pod_a only THROUGH a shared fake in the central tests/ tree -- the
               case a root-level scan misses, and the reason the graph follows `tests.*`
    pod_d      reaches pod_a only through a string literal (a patch target)

and the last block checks properties of the REAL tree, since a synthetic fixture cannot
notice the real one drifting: the root conftest staying light (the coupling the helpers
split removed), every pod still pinning core (the premise for dropping that edge), and no
pod's tests importing another pod without being selected for its changes.
'''
import re

import pytest

from tests.cli import _affected_tests as affected_tests
from tests.cli._affected_tests import (CONFTEST, PACKAGES, ImportGraph, Selection, changed_node,
                                       main, referenced_modules, select)
from tests.cli._package_pins import REPO_ROOT as PINS_REPO_ROOT, POD_ROOTS, declared_core_pin
from tests.cli._roots import CORE_ROOT, SERVICE_ROOTS
from tests.cli._scratch_repo import commit, init_scratch_repo

SYNTHETIC = ('pkg_core', 'pod_a', 'pod_b', 'pod_c', 'pod_d')


def _write(root, path, text=''):
    target = root / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding='utf-8')


@pytest.fixture(name='tree')
def tree_fixture(tmp_path):
    '''The synthetic repo described in the module doc, returned as an ImportGraph over it.'''
    for package in SYNTHETIC:
        _write(tmp_path, f'{package}/__init__.py')
        _write(tmp_path, f'{package}/mod.py', 'VALUE = 1\n')
        _write(tmp_path, f'{package}/tests/__init__.py')
        _write(tmp_path, f'{package}/tests/test_mod.py', f'from {package}.mod import VALUE\n')
    _write(tmp_path, CONFTEST, 'import pkg_core\nfrom tests.helpers import thing\n')
    _write(tmp_path, 'tests/__init__.py')
    _write(tmp_path, 'tests/helpers.py', 'import pkg_core\nthing = 1\n')
    _write(tmp_path, 'tests/fakes/__init__.py')
    _write(tmp_path, 'tests/fakes/a_fake.py', 'import pod_a.mod\n')
    # pod_b: a direct import of another pod.
    _write(tmp_path, 'pod_b/tests/test_uses_a.py', 'from pod_a.mod import VALUE\n')
    # pod_c: only through a shared fake.
    _write(tmp_path, 'pod_c/tests/test_uses_fake.py', 'from tests.fakes.a_fake import something\n')
    # pod_d: only through the target string of a patch.
    _write(tmp_path, 'pod_d/tests/test_patches.py',
           'def test_it(monkeypatch):\n    monkeypatch.setattr("pod_a.mod.VALUE", 2)\n')
    # pod_a's own tests import core, which is pinned and so must not count.
    _write(tmp_path, 'pod_a/tests/test_core.py', 'import pkg_core\n')
    return ImportGraph(tmp_path, SYNTHETIC, 'pkg_core')


def _packages(graph, *changed):
    selection = select(list(changed), graph)
    assert not selection.everything, selection.reason
    return selection.packages


def test_source_change_selects_every_package_that_can_reach_it(tree):  # pylint: disable=redefined-outer-name
    '''Direct import (b), through a shared fake (c), through a patch string (d), and itself.'''
    assert _packages(tree, 'pod_a/mod.py') == ('pod_a', 'pod_b', 'pod_c', 'pod_d')


def test_source_change_is_gated_only_where_source_changed(tree):  # pylint: disable=redefined-outer-name
    '''The 100% gate applies to the package whose source moved, not to the ones that merely ran.'''
    assert select(['pod_a/mod.py'], tree).gated == ('pod_a',)


def test_a_package_nobody_imports_selects_only_itself(tree):  # pylint: disable=redefined-outer-name
    '''The payoff: pod_b's source is reached by nothing but pod_b.'''
    assert _packages(tree, 'pod_b/mod.py') == ('pod_b',)


def test_test_only_change_selects_only_that_tree_and_is_not_gated(tree):  # pylint: disable=redefined-outer-name
    '''Editing pod_a's tests cannot change what pod_b's tests see, and coverage of pod_a's source is
    not guaranteed by pod_a's tests alone, so no gate.'''
    selection = select(['pod_a/tests/test_core.py'], tree)
    assert selection.packages == ('pod_a',)
    assert selection.gated == ()


def test_core_change_selects_only_core(tree):  # pylint: disable=redefined-outer-name
    '''Pods run against the pinned release, never this checkout, so core's source reaches core alone,
    even though the conftest and every pod's tests import it.'''
    selection = select(['pkg_core/mod.py'], tree)
    assert selection.packages == ('pkg_core',)
    assert selection.gated == ('pkg_core',)


def test_shared_fake_change_selects_only_who_uses_it(tree):  # pylint: disable=redefined-outer-name
    '''tests/fakes/a_fake.py is reached by pod_c only.'''
    assert _packages(tree, 'tests/fakes/a_fake.py') == ('pod_c',)


def test_conftest_change_selects_every_package(tree):  # pylint: disable=redefined-outer-name
    '''Every package's tests run under the root conftest.'''
    assert _packages(tree, CONFTEST) == SYNTHETIC


def test_central_only_change_selects_no_package(tree):  # pylint: disable=redefined-outer-name
    '''A central test nobody imports changes only the central tree, which always runs.'''
    _write(tree.repo_root, 'tests/test_central.py', 'import pod_a\n')
    assert _packages(tree, 'tests/test_central.py') == ()


def test_documentation_and_metadata_are_ignored(tree):  # pylint: disable=redefined-outer-name
    '''The same allow-list that lets tox skip entirely, so the two cannot disagree.'''
    assert _packages(tree, 'docs/guide.md', 'README.md', 'renovate.json', 'pod_b/mod.py') == ('pod_b',)


@pytest.mark.parametrize('path', [
    'tox.ini', 'pyproject.toml', '.pylintrc', 'docker/Dockerfile.x', 'alembic/env.py',
    '.github/workflows/ci.yml', 'catalog-info.yaml', 'tests/data.json', 'docs/architecture.md',
    'something/nobody/classified.py',
])
def test_anything_unclassified_runs_everything(tree, path):  # pylint: disable=redefined-outer-name
    '''An allow-list: a path no rule names cannot be reasoned about, so nothing is skipped.'''
    _write(tree.repo_root, path)
    selection = select([path, 'pod_b/mod.py'], tree)
    assert selection.everything
    assert path in selection.reason


def test_deleted_file_runs_everything(tree):  # pylint: disable=redefined-outer-name
    '''A deleted module's importers are what break, and they may be in a tree that is not selected.'''
    selection = select(['pod_a/gone.py'], tree)
    assert selection.everything
    assert 'deleted' in selection.reason


def test_deleted_documentation_is_ignored(tree):  # pylint: disable=redefined-outer-name
    '''Nothing imports prose, so removing it is as harmless as editing it.'''
    assert _packages(tree, 'docs/old.md', 'pod_b/mod.py') == ('pod_b',)


def test_no_changes_runs_everything(tree):  # pylint: disable=redefined-outer-name
    '''An empty or unreadable diff must not read as "nothing changed, nothing to test".'''
    assert select([], tree).everything
    assert select(['', ''], tree).everything


def test_literals_in_ci_tooling_are_not_edges(tree):  # pylint: disable=redefined-outer-name
    '''tests/cli/_*.py holds tables of module names as DATA. Counting them would make every test that
    merely calls the tooling depend on every pod.'''
    _write(tree.repo_root, 'tests/cli/__init__.py')
    _write(tree.repo_root, 'tests/cli/_table.py', 'IMAGES = {"a": "pod_a.mod"}\n')
    _write(tree.repo_root, 'pod_d/tests/test_patches.py', 'from tests.cli._table import IMAGES\n')
    assert _packages(tree, 'pod_a/mod.py') == ('pod_a', 'pod_b', 'pod_c')


def test_real_imports_in_ci_tooling_are_still_edges(tree):  # pylint: disable=redefined-outer-name
    '''The exemption is for string constants only.'''
    _write(tree.repo_root, 'tests/cli/__init__.py')
    _write(tree.repo_root, 'tests/cli/_table.py', 'import pod_a.mod\n')
    _write(tree.repo_root, 'pod_d/tests/test_patches.py', 'from tests.cli._table import x\n')
    assert 'pod_d' in _packages(tree, 'pod_a/mod.py')


def test_importing_a_parent_package_initialiser_is_an_edge(tree):  # pylint: disable=redefined-outer-name
    '''`import tests.fakes.x` runs tests/fakes/__init__.py first, so changing it affects the importer.'''
    _write(tree.repo_root, 'tests/fakes/__init__.py', 'import pod_a\n')
    assert _packages(tree, 'tests/fakes/__init__.py') == ('pod_c',)


def test_changed_node_classification(tree):  # pylint: disable=redefined-outer-name
    '''One node per file kind; None for ignorable, empty string for "cannot say".'''
    assert changed_node('pod_a/mod.py', tree) == ('root', 'pod_a')
    assert changed_node('pod_a/changelog.d/x.md', tree) == ('root', 'pod_a')
    assert changed_node('pod_a/tests/test_mod.py', tree) == ('tests', 'pod_a')
    assert changed_node('pod_a/tests', tree) == ('root', 'pod_a')
    assert changed_node(CONFTEST, tree) == ('file', CONFTEST)
    assert changed_node('tests/helpers.py', tree) == ('file', 'tests/helpers.py')
    assert changed_node('tests/data.json', tree) == ''
    assert changed_node('README.md', tree) is None
    assert changed_node('docs/images/x.png', tree) is None


@pytest.mark.parametrize('source, package, expected', [
    ('import a.b', 'p', {'a.b'}),
    ('from a.b import c, d', 'p', {'a.b', 'a.b.c', 'a.b.d'}),
    # Relative imports resolve against the importing file's package.
    ('from . import x', 'pkg.sub', {'pkg.sub', 'pkg.sub.x'}),
    ('from .y import z', 'pkg.sub', {'pkg.sub.y', 'pkg.sub.y.z'}),
    ('from ..y import z', 'pkg.sub', {'pkg.y', 'pkg.y.z'}),
    # A dotted string is a candidate (a patch target); prose and bare words are not.
    ('x = "pod_a.mod.VALUE"', 'p', {'pod_a.mod.VALUE'}),
    ('x = "some prose, with.dots"', 'p', set()),
    ('x = "plain"', 'p', set()),
])
def test_referenced_modules(source, package, expected):
    '''What a file names, as candidate dotted modules.'''
    assert referenced_modules(source, package) == expected


def test_referenced_modules_can_ignore_literals():
    '''literals=False is the tooling exemption.'''
    assert referenced_modules('import a\nx = "b.c"', 'p', literals=False) == {'a'}


def test_main_prints_the_selection(tmp_path, monkeypatch, capsys):
    '''End to end against a throwaway git repo: stdout carries only the machine-readable scope.'''
    init_scratch_repo(tmp_path, monkeypatch)
    _write(tmp_path, 'README.md', 'x\n')
    _write(tmp_path, CONFTEST)
    commit(tmp_path, 'base')
    _write(tmp_path, 'tox.ini', 'x\n')
    commit(tmp_path, 'change')
    monkeypatch.setattr(affected_tests, 'ImportGraph', lambda: ImportGraph(tmp_path, SYNTHETIC, 'pkg_core'))
    assert main(['--base', 'HEAD~1']) == 0
    assert capsys.readouterr().out.strip() == 'scope=all'

    _write(tmp_path, 'pod_b/__init__.py')
    _write(tmp_path, 'pod_b/tests/__init__.py')
    commit(tmp_path, 'only b')
    assert main(['--base', 'HEAD~1']) == 0
    assert capsys.readouterr().out.strip() == 'scope=packages pod_b'


# --- properties of the REAL tree ----------------------------------------------------------------


def test_packages_are_exactly_the_ones_on_disk():
    '''PACKAGES comes from _roots; a package missing there would never be selected.'''
    on_disk = {path.parent.parent.name for path in PINS_REPO_ROOT.glob('discord_*/tests/__init__.py')}
    assert set(PACKAGES) == on_disk


def test_every_pod_pins_core():
    '''The premise for dropping the discord_core edge: a pod's tests never run against this
    checkout's core, because each pod installs a pinned release. If one stopped pinning, a core
    change WOULD reach it and this selection would under-select.'''
    for root in POD_ROOTS.values():
        assert declared_core_pin(root) is not None, f'{root} no longer pins {CORE_ROOT}'


def test_root_conftest_reaches_no_pod():
    '''The coupling that made narrowing pointless: conftest.py imported tests/helpers.py, which
    imported fakes for six pods, so EVERY package's test run depended on all of them. Splitting
    helpers (tests/db_helpers.py, tests/fakes/in_process_stack.py) removed it. If this fails, find
    the import that put a pod back on the conftest's path -- every package's selection widens.'''
    reached = {name for kind, name in ImportGraph().closure([('file', CONFTEST)]) if kind == 'root'}
    assert reached <= {CORE_ROOT}, sorted(reached - {CORE_ROOT})


def test_a_pod_change_selects_every_package_whose_tests_import_it_directly():
    '''Independent of the graph: scan each package's tests for an `import discord_x` / `from
    discord_x` line and require that changing discord_x selects it. A line regex is cruder than the
    AST walk, so any package it finds that the graph does not is under-selection.'''
    graph = ImportGraph()
    pods = sorted(SERVICE_ROOTS.values())
    for pod in pods:
        selected = set(select([f'{pod}/__init__.py'], graph).packages)
        for package in PACKAGES:
            if package in (pod, CORE_ROOT):
                continue
            mentions = any(re.search(rf'^\s*(?:from|import)\s+{pod}\b', path.read_text(encoding='utf-8'), re.M)
                           for path in (PINS_REPO_ROOT / package / 'tests').rglob('*.py'))
            if mentions:
                assert package in selected, f'{package} imports {pod} but is not selected for it'


def test_selection_is_a_selection_value():
    '''A change to a real pod yields a narrowed, ordered selection rather than a crash.'''
    selection = select(['discord_search/__init__.py'], ImportGraph())
    assert isinstance(selection, Selection)
    assert list(selection.packages) == sorted(selection.packages, key=PACKAGES.index)
