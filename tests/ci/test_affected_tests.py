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

plus, in the central tests/ tree, two files that import a pod (selectable), one that reads the
repo directly and one that reads it through a shared module (both always run).

and the last block checks properties of the REAL tree, since a synthetic fixture cannot
notice the real one drifting: the root conftest staying light (the coupling the helpers
split removed), every pod still pinning core (the premise for dropping that edge), and no
pod's tests importing another pod without being selected for its changes.
'''
import ast
import re

import pytest

from ci import _affected_tests as affected_tests
from ci._affected_tests import (CENTRAL_INPUTS, CONFTEST, PACKAGE_INPUTS, PACKAGES, ImportGraph,
                                       Selection, changed_node, is_central_input, main, package_input_owner,
                                       referenced_modules, repo_reads, select)
from ci._package_pins import REPO_ROOT as PINS_REPO_ROOT, POD_ROOTS, declared_core_pin
from ci._roots import CORE_ROOT, SERVICE_ROOTS
from ci._scratch_repo import commit, init_scratch_repo

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
    # Central test files: two that only import, one that reads the repo, one that does so via a helper.
    _write(tmp_path, 'tests/test_uses_a.py', 'import pod_a.mod\n\n\ndef test_x():\n    pass\n')
    _write(tmp_path, 'tests/test_uses_b.py', 'import pod_b.mod\n\n\ndef test_x():\n    pass\n')
    _write(tmp_path, 'tests/test_reads_docs.py', 'from pathlib import Path\nREPO_ROOT = Path(".")\n\n\ndef test_x():\n    pass\n')
    _write(tmp_path, 'tests/fakes/reader_helper.py', 'HERE = __file__\n')
    _write(tmp_path, 'tests/test_reads_via_helper.py',
           'from tests.fakes.reader_helper import HERE\n\n\ndef test_x():\n    pass\n')
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
    'tox.ini', 'pyproject.toml', 'tests/data.json', 'something/nobody/classified.py',
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
    '''ci/_*.py holds tables of module names as DATA. Counting them would make every test that
    merely calls the tooling depend on every pod.'''
    _write(tree.repo_root, 'ci/__init__.py')
    _write(tree.repo_root, 'ci/_table.py', 'IMAGES = {"a": "pod_a.mod"}\n')
    _write(tree.repo_root, 'pod_d/tests/test_patches.py', 'from ci._table import IMAGES\n')
    assert _packages(tree, 'pod_a/mod.py') == ('pod_a', 'pod_b', 'pod_c')


def test_real_imports_in_ci_tooling_are_still_edges(tree):  # pylint: disable=redefined-outer-name
    '''The exemption is for string constants only.'''
    _write(tree.repo_root, 'ci/__init__.py')
    _write(tree.repo_root, 'ci/_table.py', 'import pod_a.mod\n')
    _write(tree.repo_root, 'pod_d/tests/test_patches.py', 'from ci._table import x\n')
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
    assert capsys.readouterr().out.split() == ['scope=packages', 'pod_b', 'central=0']


# --- central tests, selected per file ------------------------------------------------------------


def test_central_inputs_select_no_package_and_only_the_files_that_always_run(tree):  # pylint: disable=redefined-outer-name
    '''A generated doc or workflow is read only by the repo-reading tests, which always run anyway.'''
    selection = select(['docs/openapi/db.yaml', '.github/workflows/ci.yml'], tree)
    assert not selection.everything
    assert selection.packages == ()
    assert selection.central == ('tests/test_reads_docs.py', 'tests/test_reads_via_helper.py')


@pytest.mark.parametrize('path, expected', [
    ('docs/openapi/db.yaml', True), ('docs/monitoring/metrics_reference.md', True),
    ('docs/architecture.md', True), ('docs/configuration.md', True), ('.github/workflows/ci.yml', True),
    ('docker/Dockerfile.db', True), ('catalog-info.yaml', True), ('.dockerignore', True),
    ('.pylintrc', True), ('.pylintrc.test', True), ('.pylintrc.bak', False),
    ('ci/image-closure.json', True),
    # Close but not on the list: a prefix needs its slash, a file is exact.
    ('docs/openapi', False), ('docker', False), ('.github', False), ('docs/music.md', False),
    ('catalog-info.yaml.bak', False), ('alembic/env.py', False), ('pyproject.toml', False),
])
def test_is_central_input(path, expected):
    '''Prefixes end in a slash, files are exact.'''
    assert is_central_input(path) is expected


def test_a_change_to_the_ci_tooling_is_a_node_and_reaches_the_central_files_that_import_it(tree):  # pylint: disable=redefined-outer-name
    '''ci/ is CI tooling, not a package: a .py change in it is an ordinary graph node (not "everything"), and what it
    reaches is the central test that imports it, plus the files that read the repo.'''
    _write(tree.repo_root, 'ci/__init__.py')
    _write(tree.repo_root, 'ci/_tool.py', 'VALUE = 1\n')
    _write(tree.repo_root, 'tests/test_uses_tool.py', 'from ci._tool import VALUE\n\n\ndef test_x():\n    assert VALUE\n')
    assert changed_node('ci/_tool.py', tree) == ('file', 'ci/_tool.py')
    selection = select(['ci/_tool.py'], tree)
    assert not selection.everything
    assert selection.packages == ()
    assert selection.central == ('tests/test_reads_docs.py', 'tests/test_reads_via_helper.py', 'tests/test_uses_tool.py')


def test_a_pod_change_runs_the_central_files_that_import_it_plus_the_readers(tree):  # pylint: disable=redefined-outer-name
    '''tests/test_uses_a.py imports pod_a, so it is selected; tests/test_uses_b.py is not.'''
    selection = select(['pod_a/mod.py'], tree)
    assert selection.central == ('tests/test_reads_docs.py', 'tests/test_reads_via_helper.py',
                                 'tests/test_uses_a.py')


def test_an_unrelated_pod_change_skips_central_files_that_do_not_reach_it(tree):  # pylint: disable=redefined-outer-name
    '''The point of the exercise: pod_b moves, so pod_a's central test is not selected.'''
    central = select(['pod_b/mod.py'], tree).central
    assert 'tests/test_uses_a.py' not in central
    assert 'tests/test_uses_b.py' in central


def test_a_core_change_skips_central_files_that_only_reach_core(tree):  # pylint: disable=redefined-outer-name
    '''Central tests run in the first pass, against the pinned core, like a pod's.'''
    _write(tree.repo_root, 'tests/test_core_only.py', 'import pkg_core\n\n\ndef test_x():\n    pass\n')
    assert 'tests/test_core_only.py' not in select(['pkg_core/mod.py'], tree).central


def test_changing_a_central_file_selects_that_file(tree):  # pylint: disable=redefined-outer-name
    '''A file is in its own reach.'''
    assert 'tests/test_uses_b.py' in select(['tests/test_uses_b.py'], tree).central


def test_a_shared_module_change_selects_the_central_files_that_import_it(tree):  # pylint: disable=redefined-outer-name
    '''tests/fakes/a_fake.py reaches pod_a, and test_uses_fake-style files that import it follow.'''
    _write(tree.repo_root, 'tests/test_uses_fake.py',
           'from tests.fakes.a_fake import x\n\n\ndef test_x():\n    pass\n')
    central = select(['tests/fakes/a_fake.py'], tree).central
    assert 'tests/test_uses_fake.py' in central
    assert 'tests/test_uses_b.py' not in central


def test_a_conftest_that_reads_the_repo_makes_every_central_file_always_run(tree):  # pylint: disable=redefined-outer-name
    '''The narrowing turns itself off rather than hide a dependency the graph cannot see.'''
    _write(tree.repo_root, CONFTEST, 'import pkg_core\nfrom tests.helpers import thing\nHERE = __file__\n')
    selection = select(['pod_b/mod.py'], tree)
    assert set(selection.central) == set(ImportGraph(tree.repo_root, SYNTHETIC, 'pkg_core').central_tests())


def test_central_tests_are_what_pytest_would_collect(tmp_path):
    '''A Test* class and a module-level test count; a differently named class, a factory-nested test,
    a helper, and a module with no tests do not.'''
    _write(tmp_path, 'tests/__init__.py')
    _write(tmp_path, 'tests/test_function.py', 'def test_a():\n    pass\n')
    _write(tmp_path, 'tests/test_async.py', 'async def test_a():\n    pass\n')
    _write(tmp_path, 'tests/test_class.py', 'class TestThing:\n    def test_a(self):\n        pass\n')
    _write(tmp_path, 'tests/suite_class.py', 'class Suite:\n    def test_a(self):\n        pass\n')
    _write(tmp_path, 'tests/suite_factory.py', 'def make():\n    def test_a():\n        pass\n    return test_a\n')
    _write(tmp_path, 'tests/helpers.py', 'def helper():\n    pass\n')
    graph = ImportGraph(tmp_path, (), 'x')
    assert graph.central_tests() == ['tests/test_async.py', 'tests/test_class.py', 'tests/test_function.py']


@pytest.mark.parametrize('source, expected', [
    ('x = 1', False),
    ('REPO_ROOT / "a"', True),
    ('p = __file__', True),
    ('import subprocess', True),
    ('from subprocess import run', True),
    ('import tomllib', True),
    ('os.walk(".")', True),
    ('Path.cwd()', True),
    ('p = "docs/openapi/x.yaml"', True),
    ('p = "discord_db/database.py"', True),
    ('p = "image-closure.json"', True),
    ('p = f"{root}/docs/{name}"', True),
    ('p = "docs"', True),
    # Not repo reads: a temp-dir fixture, a module name, a plain word, a config key.
    ('tmp_path.glob("*.py")', False),
    ('import discord_db.database', False),
    ('x = "monitoring"', False),
    ('x = "documentation"', False),
    ('ast.walk(tree)', False),
    ('import shutil\nshutil.rmtree(tmp)', False),
])
def test_repo_reads(source, expected):
    '''What counts as reading the repo. Over-flagging costs a test that runs anyway; missing one
    would skip a test that should have run.'''
    assert bool(repo_reads(source)) is expected


# --- package inputs: a path read by one package's own tests ---------------------------------------


def test_a_package_input_selects_only_the_owning_packages_tests(tree, monkeypatch):  # pylint: disable=redefined-outer-name
    '''The owner's tree runs; the packages that merely import the owner do not, because they never read
    the file. pod_b imports pod_a in this tree, so it is the one that must NOT be selected.'''
    monkeypatch.setattr(affected_tests, 'PACKAGE_INPUTS', {'data/': 'pod_a'})
    _write(tree.repo_root, 'data/schema.sql', 'x\n')
    selection = select(['data/schema.sql'], tree)
    assert not selection.everything
    assert selection.packages == ('pod_a',)
    assert selection.gated == ()


@pytest.mark.parametrize('path, owner', [
    ('alembic/env.py', 'discord_db'), ('alembic/versions/0001_x.py', 'discord_db'),
    ('alembic.ini', 'discord_db'),
    # A prefix needs its slash and a file is exact.
    ('alembic', None), ('alembic.ini.bak', None), ('alembicx/env.py', None), ('discord_db/alembic.ini', None),
])
def test_package_input_owner(path, owner):
    '''Prefixes end in a slash, files are exact.'''
    assert package_input_owner(path) == owner


def test_a_package_input_of_a_package_this_tree_does_not_have_is_unclassified(tree):  # pylint: disable=redefined-outer-name
    '''The synthetic tree has no discord_db, so alembic/ there cannot be placed and must run everything.'''
    _write(tree.repo_root, 'alembic/env.py')
    assert select(['alembic/env.py'], tree).everything


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


def test_the_root_conftest_and_shared_helpers_read_nothing_from_the_repo():
    '''Same shape as test_root_conftest_reaches_no_pod, for the other way a file leaks into every
    central test's reach: a repo read in the conftest would flag all of them and end the narrowing.'''
    graph = ImportGraph()
    leaked = {}
    for kind, name in graph.closure([('file', CONFTEST)]):
        if kind == 'file':
            reads = repo_reads((PINS_REPO_ROOT / name).read_text(encoding='utf-8'))
            if reads:
                leaked[name] = sorted(reads)
    assert not leaked, f'these are on every test\'s path and read the repo: {leaked}'


def test_the_known_repo_readers_always_run_and_plenty_still_narrow():
    '''Canaries both ways: the checks that look at the whole repo must be flagged, and the flagging
    must not have swallowed everything.'''
    graph = ImportGraph()
    always = {path for path in graph.central_tests() if graph.reads_repo(path)}
    for reader in ('tests/checks/test_import_boundaries.py', 'tests/checks/test_package_pins.py',
                   'tests/checks/test_catalog_apis.py', 'tests/routes/test_openapi_docs.py'):
        assert reader in always, f'{reader} reads the repo but would be skipped'
    selectable = set(graph.central_tests()) - always
    assert len(selectable) >= 10, f'only {len(selectable)} central files can still narrow: {sorted(selectable)}'


def test_central_tests_do_not_include_shared_suites():
    '''tests/*_suite.py define tests in non-Test classes or factories; pytest collects them only
    through their users, so passing one by name would collect nothing.'''
    assert not [path for path in ImportGraph().central_tests() if path.endswith('_suite.py')]


def test_every_central_input_exists():
    '''A stale entry would silently stop matching.'''
    for entry in CENTRAL_INPUTS:
        assert (PINS_REPO_ROOT / entry).exists(), f'{entry} is in CENTRAL_INPUTS but is not in the repo'


# Package test code may import these from a central module that otherwise reads the repo: the one call
# made there does not.
_ALLOWED_READER_IMPORTS = {'ci._image_deps': {'measure'}}
_INPUT_WORDS = ('docs', 'docker', '.github', 'catalog-info.yaml', '.dockerignore', '.pylintrc', '.pylintrc.test',
                'image-closure.json')


def _names_a_central_input(value: str) -> bool:
    '''True for a string that names one of the central-input locations.'''
    return (value in _INPUT_WORDS
            or any(value.startswith(f'{word}/') or f'/{word}/' in value for word in _INPUT_WORDS)
            or 'catalog-info.yaml' in value or 'image-closure.json' in value)


def _reader_import_problem(node: ast.ImportFrom):
    '''A message if `node` imports more than the allowed names from a central module that reads the repo.'''
    module_file = PINS_REPO_ROOT / (node.module.replace('.', '/') + '.py')
    if not (module_file.is_file() and repo_reads(module_file.read_text(encoding='utf-8'))):
        return None
    extra = {alias.name for alias in node.names} - _ALLOWED_READER_IMPORTS.get(node.module, set())
    return f'imports {sorted(extra)} from {node.module}, which reads the repo' if extra else None


def _central_input_problems(source) -> list:
    '''What in one package test file touches a central input, directly or through a repo-reading import.'''
    problems = []
    for node in ast.walk(ast.parse(source.read_text(encoding='utf-8'))):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and len(node.value) < 200:
            if _names_a_central_input(node.value):
                problems.append(f'names {node.value!r}')
        elif isinstance(node, ast.ImportFrom) and (node.module or '').startswith('tests.'):
            problem = _reader_import_problem(node)
            if problem:
                problems.append(problem)
    return problems


def test_no_package_test_reads_a_central_input():
    '''CENTRAL_INPUTS lets a change to, say, docs/openapi/ skip every package, which is only true
    while no package's own tests read one. Fails when one starts to: add the path to nothing, or
    drop it from CENTRAL_INPUTS.'''
    problems = [f'{source.relative_to(PINS_REPO_ROOT).as_posix()}: {problem}'
                for tests_dir in sorted(PINS_REPO_ROOT.glob('discord_*/tests'))
                for source in sorted(tests_dir.rglob('*.py'))
                for problem in _central_input_problems(source)]
    assert not problems, '\n'.join(problems)


def test_the_central_input_guard_would_catch_a_package_test_that_reads_one(tmp_path):
    '''The guard above is vacuous if it cannot fail: feed it the very reads it is there to stop.'''
    reads = tmp_path / 'test_bad.py'
    reads.write_text('from pathlib import Path\nP = Path("docs") / "openapi" / "db.yaml"\n', encoding='utf-8')
    assert _central_input_problems(reads) == ["names 'docs'"]
    assert _names_a_central_input('.github/workflows/ci.yml')
    assert _names_a_central_input(f'{tmp_path}/docker/Dockerfile')
    assert not _names_a_central_input('monitoring')


def test_every_package_input_exists_and_belongs_to_a_real_package():
    '''A stale path or a misspelt owner would silently stop matching.'''
    for entry, package in PACKAGE_INPUTS.items():
        assert (PINS_REPO_ROOT / entry).exists(), f'{entry} is in PACKAGE_INPUTS but is not in the repo'
        assert package in PACKAGES, f'{entry} is owned by {package}, which is not a package'


def _names_a_package_input(value: str, entry: str) -> bool:
    '''True for a string that names `entry` (a prefix or a file), not merely the word.'''
    bare = entry.rstrip('/')
    return value == entry or value == bare or value.startswith(bare + '/') or f'/{bare}/' in value or value.endswith('/' + bare)


def test_only_the_owning_package_tests_read_a_package_input():
    '''PACKAGE_INPUTS lets a change to alembic/ run only discord_db's tests, which is only true while no
    OTHER package's tests read it. Fails when one starts to: move the path to CENTRAL_INPUTS if only the
    always-running files read it, or widen the mapping.'''
    problems = []
    for entry, owner in PACKAGE_INPUTS.items():
        for tests_dir in sorted(PINS_REPO_ROOT.glob('discord_*/tests')):
            if tests_dir.parent.name == owner:
                continue
            for source in sorted(tests_dir.rglob('*.py')):
                for node in ast.walk(ast.parse(source.read_text(encoding='utf-8'))):
                    if (isinstance(node, ast.Constant) and isinstance(node.value, str) and len(node.value) < 200
                            and _names_a_package_input(node.value, entry)):
                        problems.append(f'{source.relative_to(PINS_REPO_ROOT).as_posix()}: names {node.value!r}')
    assert not problems, 'a package other than the owner reads a package input:\n' + '\n'.join(problems)


def test_the_package_input_guard_can_tell_a_read_from_a_word():
    '''The guard above is vacuous if it flags nothing, and useless if it flags the library name.'''
    assert _names_a_package_input('alembic.ini', 'alembic.ini')
    assert _names_a_package_input('alembic/versions/x.py', 'alembic/')
    assert _names_a_package_input('/repo/alembic/env.py', 'alembic/')
    assert _names_a_package_input('alembic', 'alembic/')
    assert not _names_a_package_input('alembicx', 'alembic/')
    assert not _names_a_package_input('not alembic enough', 'alembic.ini')
