'''
Decide which packages' test trees a set of changed files can affect.

CI's tox run used to execute every package's tests on every PR. The repo is eight
packages with a test tree each, so most PRs touch one and need not run the rest --
but only if "the rest" really cannot see the change. This module answers that from
the code, not from a hand-kept table:

    python -m tests.cli._affected_tests --base origin/main

prints `scope=all` or `scope=packages discord_db discord_gateway` on stdout
(diagnostics on stderr). `_run_tests.py` is the real consumer; this is the same
decision made inspectable.

WHY NOT "THE PACKAGES THE PR TOUCHED"
-------------------------------------
Package test trees are not self-contained. Measured on the tree this was written
against: gateway's tests import broker, db and downloader; broker's import gateway;
commands' import broker, db, downloader and search. Worse, most of that goes through
the SHARED fakes and suites in the central `tests/` tree (`tests.fakes.*`,
`tests.helpers`, ...), and those import other pods in turn, so a package's real
dependencies are two hops away from its own files. A root-level import scan would
miss them and under-select, silently.

So the graph is built by parsing every first-party file with `ast`:

  ('root', R)    the source of package R (everything under R/ except R/tests/)
  ('tests', R)   R's own test tree
  ('file', P)    one file of the central tree: `conftest.py` or `tests/**.py`

Edges are the modules a node imports -- `import`, `from ... import`, relative
imports, and string literals that look like a dotted first-party path (the targets
of `monkeypatch.setattr('discord_x.y.z', ...)` and `patch(...)` import a module
without ever naming it in an import statement, and so does the argument of
`_image_deps.measure('discord_x.y')`). A package's test tree needs to run when any
node in its transitive closure changed.

One exemption: string literals in `tests/cli/_*.py` are not read. Those are CI
tooling, not test code, and `_image_deps.py` is a table naming every image's
entrypoint as a string. Counting those as edges made every test that merely calls
`measure(...)` look like it depends on all six pods. The test that calls it names
the module it measures in its OWN literal, which is still read.

discord_core IS NOT A DEPENDENCY OF THE PODS' TESTS
---------------------------------------------------
Each pod pins `discord-core` to a released tag, and `tox.ini`'s first pytest pass
runs the pods against THAT install, never this checkout (the checkout is installed
editable only afterwards, for `discord_core/tests`). So a PR that edits
`discord_core/` cannot change what a pod's test run sees, and following the
`discord_core` edge would just rebuild "run everything" for the commonest kind of
change. The edge is dropped for every package but core itself.
tests/cli/test_affected_tests.py checks the premise (every pod pins core).

AN ALLOW-LIST, AND EVERYTHING ELSE IS EVERYTHING
------------------------------------------------
Only these map to a node: a file inside a package, `conftest.py`, and a `.py`
under `tests/`. Documentation no test reads, and `renovate.json`, are ignored
(`_skippable_changes` owns that list). ANY other path -- `pyproject.toml`,
`tox.ini`, `.pylintrc*`, `docker/`, `alembic/`, `.github/`, a data file under
`tests/` -- selects everything, because nothing here can say who reads it. So does
a deleted or moved file that IS one of the above kinds: its importers are the ones
that would break, and they may be in a tree that is not selected. (Deleting ignorable
documentation is as harmless as editing it, and is ignored.)

THE CENTRAL tests/ TREE IS SELECTED PER FILE, AND SOME OF IT ALWAYS RUNS
------------------------------------------------------------------------
Each central test file is a unit of its own, with its own reach (the file, the shared
`tests.*` modules it imports, and the pods those import). It runs when something in that
reach changed, exactly as a package's tree does -- except a file that READS THE REPO,
which always runs. Those are the import-boundary, pin, catalog, census and OpenAPI-doc
checks: they look at files by path (docs, workflows, Dockerfiles, other packages'
source), and an import graph cannot see a dependency that is not an import.

Which files read the repo is DERIVED each run, not listed by hand (`repo_reads`): a file
counts if it, or anything central it imports, mentions REPO_ROOT or `__file__`, runs
`subprocess`, reads TOML, walks the working directory, or holds a string that names a
repo path (`docs/...`, `.github/...`, `discord_x/...`). That is deliberately generous --
over-flagging only costs a test that runs anyway -- and it includes the root conftest.py,
so a repo read added THERE flags every file and turns the narrowing off, loudly, in the
tests that pin it, rather than hiding.

CENTRAL INPUTS
--------------
A few paths are read only by those always-running files: the generated OpenAPI docs and
monitoring docs, `.github/`, `docker/`, `catalog-info.yaml`, the measured image closure.
Changing one therefore needs no package at all, just the files that always run, and no
longer falls back to "everything". `test_no_package_test_reads_a_central_input` is what
keeps that true: it fails if a package's own tests start naming one of them.
'''
import ast
import re
import sys
from pathlib import Path
from typing import NamedTuple

from tests.cli._git_diff import REPO_ROOT, base_head_parser, diff_paths, report_changed
from tests.cli._roots import CORE_ROOT, SERVICE_ROOTS
from tests.cli._skippable_changes import METADATA_ONLY, is_documentation

#: Every package, core first. The test run is keyed on these names.
PACKAGES = (CORE_ROOT, *sorted(SERVICE_ROOTS.values()))

CONFTEST = 'conftest.py'
CENTRAL_TESTS = 'tests'

# A string that reads as a dotted module path, e.g. a patch() target. Deliberately generic:
# anything that does not resolve to a first-party node is dropped by `_modules_to_nodes`.
_DOTTED = re.compile(r'[A-Za-z_]\w*(?:\.\w+)+')

# CI tooling whose string constants are data, not module references; see the module doc.
_TOOLING_PREFIX = 'tests/cli/_'

#: Paths read only by the central tests that always run (see "CENTRAL INPUTS"). Prefixes end in
#: '/'; the rest are exact files. Deliberately short: a path belongs here only if the guard in
#: test_affected_tests.py can show no package's own tests name it.
CENTRAL_INPUTS = (
    'docs/openapi/',
    'docs/monitoring/',
    'docs/architecture.md',
    'docs/configuration.md',
    '.github/',
    'docker/',
    'catalog-info.yaml',
    'tests/cli/image-closure.json',
)

# A string constant that names a path inside the repo. Generous on purpose: see `repo_reads`.
_REPO_PATH = re.compile(
    r'^(?:docs|docker|alembic|\.github|tests|catalog-info\.yaml|pyproject\.toml|tox\.ini|renovate\.json'
    r'|VERSION|CHANGELOG\.md|README\.md|AGENTS\.md|CONTRIBUTING\.md|DEVELOPMENT\.md|\.pylintrc(?:\.test)?)'
    r'(?:/|\.|$)|^discord_[a-z]+/|(?:^|/)image-closure\.json$|/(?:docs|docker|alembic|\.github)(?:/|$)')

# Calls that read the working tree, as (module, attribute).
_TREE_CALLS = {('os', 'walk'), ('os', 'getcwd'), ('os', 'listdir'), ('os', 'scandir'), ('Path', 'cwd')}
_TREE_MODULES = {'subprocess', 'tomllib'}

Node = tuple[str, str]


class Selection(NamedTuple):
    '''Which test trees a change set needs.

    everything: run the whole suite, `packages` and `gated` are then meaningless.
    packages: the package roots whose own `tests/` tree must run, sorted.
    gated: the subset of those whose SOURCE changed, i.e. where the 100% coverage gate
        applies. When a package's source changed, every test that can execute it is in
        `packages` (that is what the closure means), so its coverage equals what a full
        run would report. When only its tests changed that is not true -- another
        package's tests may cover it -- so it is not gated, and the weekly full run
        in scheduled.yml is what catches a coverage drop from a test-only edit.
    reason: one line saying why, for the CI log.
    central: the central test FILES to run (repo-relative), sorted: every one that reads the
        repo, plus every one whose reach contains something that changed.
    '''
    everything: bool
    packages: tuple
    gated: tuple
    reason: str
    central: tuple = ()


def _package_of(path: str) -> str:
    '''The dotted package a repo-relative file sits in, for resolving its relative imports.'''
    return '.'.join(path.split('/')[:-1])


def referenced_modules(source: str, package: str, literals: bool = True) -> set:
    '''Every dotted module name `source` imports or names, as a candidate.

    `from a.b import c` yields both `a.b` and `a.b.c`: `c` may be a submodule, and
    the resolver keeps only the candidates that exist on disk. `literals=False`
    skips string constants, for tooling whose strings are data.
    '''
    names = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                parts = package.split('.')[:len(package.split('.')) - (node.level - 1)]
                base = '.'.join([*parts, *([node.module] if node.module else [])])
            else:
                base = node.module
            if base:
                names.add(base)
                names.update(f'{base}.{alias.name}' for alias in node.names)
        elif (literals and isinstance(node, ast.Constant) and isinstance(node.value, str)
              and _DOTTED.fullmatch(node.value)):
            names.add(node.value)
    return names


def repo_reads(source: str) -> set:
    '''What `source` does that reads the repo rather than importing from it, as short labels.

    Empty means "nothing here a changed file could affect except through an import". The
    labels are only for messages; the callers just test for emptiness.
    '''
    found = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Name) and node.id in ('REPO_ROOT', '__file__'):
            found.add(node.id)
        elif (isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name)
              and (node.value.id, node.attr) in _TREE_CALLS):
            found.add(f'{node.value.id}.{node.attr}')
        elif isinstance(node, ast.Import):
            found.update(f'import {alias.name}' for alias in node.names
                         if alias.name.split('.')[0] in _TREE_MODULES)
        elif isinstance(node, ast.ImportFrom) and (node.module or '').split('.')[0] in _TREE_MODULES:
            found.add(f'from {node.module}')
        elif (isinstance(node, ast.Constant) and isinstance(node.value, str)
              and len(node.value) < 200 and _REPO_PATH.search(node.value)):
            found.add(f'path {node.value[:40]!r}')
    return found


def is_central_input(path: str) -> bool:
    '''True for a path only the always-running central tests read; see CENTRAL_INPUTS.'''
    return any(path.startswith(entry) if entry.endswith('/') else path == entry for entry in CENTRAL_INPUTS)


class ImportGraph:
    '''The first-party import graph of a checkout, at the granularity described above.'''

    def __init__(self, repo_root: Path = REPO_ROOT, packages: tuple = PACKAGES, core: str = CORE_ROOT):
        self.repo_root = repo_root
        self.packages = packages
        self.core = core
        self._edges: dict = {}
        self._reads: dict = {}

    def _py_files(self, directory: str, tests: bool) -> list:
        '''Repo-relative .py files under a package: its tests/ tree, or everything else.'''
        root = self.repo_root / directory
        files = []
        for path in sorted(root.rglob('*.py')):
            relative = path.relative_to(self.repo_root)
            in_tests = relative.parts[1] == CENTRAL_TESTS
            if in_tests == tests:
                files.append(relative.as_posix())
        return files

    def _modules_to_nodes(self, module: str) -> set:
        '''The graph nodes a dotted module name stands for; empty for anything third-party.'''
        parts = module.split('.')
        if parts[0] in self.packages:
            nodes = {('root', parts[0])}
            if len(parts) > 1 and parts[1] == CENTRAL_TESTS:
                nodes.add(('tests', parts[0]))
            return nodes
        if parts[0] != CENTRAL_TESTS:
            return set()
        nodes = set()
        for end in range(1, len(parts) + 1):
            stem = '/'.join(parts[:end])
            if (self.repo_root / f'{stem}.py').is_file():
                nodes.add(('file', f'{stem}.py'))
                break
            if not (self.repo_root / stem / '__init__.py').is_file():
                break
            nodes.add(('file', f'{stem}/__init__.py'))
        return nodes

    def edges(self, node: Node) -> set:
        '''The nodes `node` imports.'''
        if node not in self._edges:
            kind, name = node
            if kind == 'file':
                files = [name]
            else:
                files = self._py_files(name, tests=kind == 'tests')
            targets = set()
            for path in files:
                source = (self.repo_root / path).read_text(encoding='utf-8')
                for module in referenced_modules(source, _package_of(path),
                                                 literals=not path.startswith(_TOOLING_PREFIX)):
                    targets |= self._modules_to_nodes(module)
            self._edges[node] = targets
        return self._edges[node]

    def closure(self, starts, skipped=frozenset()) -> set:
        '''Every node reachable from `starts` (themselves included), never entering `skipped`.'''
        seen = set()
        stack = list(starts)
        while stack:
            node = stack.pop()
            if node in seen or node in skipped:
                continue
            seen.add(node)
            stack.extend(self.edges(node))
        return seen

    def reach(self, package: str) -> set:
        '''Every node a package's test tree can execute, itself included.

        Drops the discord_core nodes for every package but core: see the module doc.
        '''
        skipped = set() if package == self.core else {('root', self.core), ('tests', self.core)}
        return self.closure([('tests', package), ('file', CONFTEST)], skipped)

    def central_tests(self) -> list:
        '''Repo-relative central test files: those under `tests/` that pytest would collect a test from.

        That is a module-level `test*` function, or a `test*` method of a top-level `Test*` class (pytest's
        defaults; pyproject does not override them). A shared suite whose tests live in a differently named
        class, or inside a factory function, is not collected on its own, only through whoever uses it,
        so it is not a unit here (its users are).
        '''
        def defines_test(body) -> bool:
            for node in body:
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith('test'):
                    return True
                if isinstance(node, ast.ClassDef) and node.name.startswith('Test') and defines_test(node.body):
                    return True
            return False

        return [path.relative_to(self.repo_root).as_posix()
                for path in sorted((self.repo_root / CENTRAL_TESTS).rglob('*.py'))
                if defines_test(ast.parse(path.read_text(encoding='utf-8')).body)]

    def central_reach(self, path: str) -> set:
        '''Every node a central test file can execute. Run in the first pytest pass, against the
        pinned core, so the core nodes are dropped as for a pod.'''
        return self.closure([('file', path), ('file', CONFTEST)], {('root', self.core), ('tests', self.core)})

    def reads_repo(self, path: str) -> set:
        '''What a central test file, the central modules it imports, or the root conftest.py read
        from the repo (see `repo_reads`). The conftest is included on purpose: a repo read there
        reaches every file.'''
        found = set()
        for kind, name in self.central_reach(path):
            if kind == 'file':
                if name not in self._reads:
                    self._reads[name] = repo_reads((self.repo_root / name).read_text(encoding='utf-8'))
                found |= self._reads[name]
        return found


def changed_node(path: str, graph: ImportGraph):
    '''The node a changed path is, None if it cannot affect any test, or '' if unknown.

    '' (not None) is the "I cannot say" answer that makes the caller run everything.
    '''
    if path in METADATA_ONLY or is_documentation(path) or is_central_input(path):
        return None
    parts = path.split('/')
    if len(parts) > 1 and parts[0] in graph.packages:
        return ('tests', parts[0]) if len(parts) > 2 and parts[1] == CENTRAL_TESTS else ('root', parts[0])
    if path == CONFTEST or (parts[0] == CENTRAL_TESTS and path.endswith('.py')):
        return ('file', path)
    return ''


def select(changed, graph: ImportGraph) -> Selection:
    '''Map changed paths onto the package test trees they can affect.'''
    paths = [path for path in changed if path]
    if not paths:
        return Selection(True, (), (), 'no changed files: running everything')
    nodes = set()
    for path in paths:
        node = changed_node(path, graph)
        if node is None:
            continue
        if not (graph.repo_root / path).exists():
            return Selection(True, (), (), f'{path} was deleted or moved')
        if node == '':
            return Selection(True, (), (), f'{path} is not a file this selection can reason about')
        nodes.add(node)
    packages = tuple(package for package in graph.packages if nodes & graph.reach(package))
    gated = tuple(package for package in packages if ('root', package) in nodes)
    central = tuple(path for path in graph.central_tests()
                    if graph.reads_repo(path) or nodes & graph.central_reach(path))
    return Selection(False, packages, gated,
                     f'{len(nodes)} changed node(s) reach {len(packages)} package(s) '
                     f'and {len(central)} central test file(s)', central)


def main(argv=None) -> int:
    '''Print the selection for the diff base...head.'''
    args = base_head_parser(__doc__).parse_args(argv)
    changed = diff_paths(args.base, args.head, '--no-renames')
    report_changed(changed)
    selection = select(changed, ImportGraph())
    print(selection.reason, file=sys.stderr)
    if selection.everything:
        print('scope=all')
    else:
        print(f'scope=packages {" ".join(selection.packages)}'.rstrip())
        print(f'central={len(selection.central)}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
