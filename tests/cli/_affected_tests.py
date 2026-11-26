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

The central `tests/` tree is not selected, it always runs: it holds the import-
boundary, pin, catalog and census checks, which look at the whole repo.
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
    '''
    everything: bool
    packages: tuple
    gated: tuple
    reason: str


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


class ImportGraph:
    '''The first-party import graph of a checkout, at the granularity described above.'''

    def __init__(self, repo_root: Path = REPO_ROOT, packages: tuple = PACKAGES, core: str = CORE_ROOT):
        self.repo_root = repo_root
        self.packages = packages
        self.core = core
        self._edges: dict = {}

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


def changed_node(path: str, graph: ImportGraph):
    '''The node a changed path is, None if it cannot affect any test, or '' if unknown.

    '' (not None) is the "I cannot say" answer that makes the caller run everything.
    '''
    if path in METADATA_ONLY or is_documentation(path):
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
    return Selection(False, packages, gated,
                     f'{len(nodes)} changed node(s) reach {len(packages)} package(s)')


def main(argv=None) -> int:
    '''Print the selection for the diff base...head.'''
    args = base_head_parser(__doc__).parse_args(argv)
    changed = diff_paths(args.base, args.head, '--no-renames')
    report_changed(changed)
    selection = select(changed, ImportGraph())
    print(selection.reason, file=sys.stderr)
    print('scope=all' if selection.everything else f'scope=packages {" ".join(selection.packages)}'.rstrip())
    return 0


if __name__ == '__main__':
    sys.exit(main())
