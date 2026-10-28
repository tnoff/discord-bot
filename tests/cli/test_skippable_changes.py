'''
`classify` is pure path-string mapping, so those cases are stated directly. `main`
shells out to `git diff`, so it is exercised against a real throwaway repo, the
same way test_package_versions.py does it.

The part that earns its keep is the guard at the bottom: _skippable_changes.py is
an allow-list of documentation that no test reads, and an allow-list is only
safe while that stays true. The guard reads every test source, finds each path
built from REPO_ROOT, and fails if one of them is a file the list would skip.
'''
import ast
import subprocess  # nosec B404 - fixed argv, no shell, test-only git setup
from pathlib import Path

import pytest

import tests.cli._metric_census as metric_census
import tests.cli._skippable_changes as skippable_changes
import tests.cli._span_census as span_census
from tests.cli._config_reference import render_configuration_doc
from tests.cli._seam_topology import render_architecture_doc
from tests.cli._skippable_changes import (DOC_DIRS_READ_BY_TESTS, DOCS_READ_BY_TESTS, METADATA_ONLY,
                                          ROOT_DOCS, Skippable, classify, edits_outside_generated_blocks,
                                          is_documentation, main)


@pytest.mark.parametrize('paths, expected', [
    # Nothing changed: run everything rather than assume it is safe.
    ([], Skippable(everything=False, tests=False)),
    # The original rule, unchanged: these two skip every job.
    (['catalog-info.yaml'], Skippable(everything=True, tests=True)),
    (['renovate.json', 'catalog-info.yaml'], Skippable(everything=True, tests=True)),
    # Prose no test reads skips tox, but not the cheap checks.
    (['docs/guide.md'], Skippable(everything=False, tests=True)),
    (['docs/images/basic_play.png'], Skippable(everything=False, tests=True)),
    (['docs/music/media_broker.md', 'docs/monitoring/loop_health.md'], Skippable(everything=False, tests=True)),
    (['README.md', 'AGENTS.md', 'LICENSE'], Skippable(everything=False, tests=True)),
    (['docs/guide.md', 'catalog-info.yaml'], Skippable(everything=False, tests=True)),
    # Docs a test compares against the code are inputs, not prose.
    (['docs/architecture.md'], Skippable(everything=False, tests=False)),
    (['docs/configuration.md'], Skippable(everything=False, tests=False)),
    (['docs/monitoring/metrics_reference.md'], Skippable(everything=False, tests=False)),
    (['docs/monitoring/trace_linking.md'], Skippable(everything=False, tests=False)),
    (['docs/openapi/broker.yaml'], Skippable(everything=False, tests=False)),
    # One real change in the set is enough to run everything.
    (['docs/guide.md', 'discord_core/utils/otel.py'], Skippable(everything=False, tests=False)),
    (['docs/guide.md', '.github/workflows/ci.yml'], Skippable(everything=False, tests=False)),
    # Prose that lives inside a package is the package changing (its VERSION must bump).
    (['discord_gateway/changelog.d/fix.md'], Skippable(everything=False, tests=False)),
    (['discord_gateway/README.md'], Skippable(everything=False, tests=False)),
    # A file nobody has classified is not skipped.
    (['docs'], Skippable(everything=False, tests=False)),
    (['SECURITY.md'], Skippable(everything=False, tests=False)),
    (['docs.md'], Skippable(everything=False, tests=False)),
])
def test_classify(paths, expected):
    '''Only an explicitly allowed path set skips anything.'''
    assert classify(paths) == expected


def test_every_listed_path_exists():
    '''A renamed or deleted entry would silently stop protecting (or skipping) anything.'''
    root = skippable_changes.REPO_ROOT
    for path in DOCS_READ_BY_TESTS | ROOT_DOCS | METADATA_ONLY:
        assert (root / path).is_file(), path
    for directory in DOC_DIRS_READ_BY_TESTS:
        assert (root / directory).is_dir(), directory


_BLOCK = '<!-- BEGIN GENERATED(x) by t. Do not edit.\n-->\nbody\n<!-- END GENERATED(x) -->'


@pytest.mark.parametrize('before, after, expected', [
    # Prose around an untouched block: the case this rule exists for.
    (f'intro\n{_BLOCK}\noutro', f'a new intro\n{_BLOCK}\na new outro', True),
    # Two blocks, both untouched.
    (f'{_BLOCK}\nmiddle\n{_BLOCK}', f'{_BLOCK}\nedited middle\n{_BLOCK}', True),
    # A hand edit inside a block is exactly what the test exists to catch.
    (f'intro\n{_BLOCK}', f'intro\n{_BLOCK.replace("body", "body, edited")}', False),
    # A marker that moves, goes, or appears changes what is compared.
    (f'intro\n{_BLOCK}', 'intro\nbody', False),
    ('intro\nbody', f'intro\n{_BLOCK}', False),
    (f'intro\n{_BLOCK}', f'intro\n{_BLOCK}\n{_BLOCK}', False),
    # A doc with no block has nothing the test compares, so the premise does not hold.
    ('intro', 'intro, edited', False),
])
def test_edits_outside_generated_blocks(before, after, expected):
    '''Only a change that leaves every generated block byte-identical counts.'''
    assert edits_outside_generated_blocks('docs/architecture.md', before, after) is expected


@pytest.mark.parametrize('path, before, after', [
    # Not one of the compared docs: is_documentation decides those, not this.
    ('docs/guide.md', f'intro\n{_BLOCK}', f'intro, edited\n{_BLOCK}'),
    # A doc that is new or deleted in the change.
    ('docs/architecture.md', None, f'intro\n{_BLOCK}'),
    ('docs/architecture.md', f'intro\n{_BLOCK}', None),
])
def test_edits_outside_generated_blocks_is_false_where_it_does_not_apply(path, before, after):
    '''The rule applies to the four compared docs, and only to an edit of one that exists on both sides.'''
    assert edits_outside_generated_blocks(path, before, after) is False


def test_classify_uses_the_prose_only_answer_for_a_compared_doc():
    '''A compared doc skips tox only when the caller vouches that its blocks are untouched.'''
    assert classify(['docs/architecture.md']).tests is False
    assert classify(['docs/architecture.md'], prose_only=lambda _path: False).tests is False
    assert classify(['docs/architecture.md'], prose_only=lambda _path: True).tests is True
    # The answer is not consulted for a path it does not cover, so it cannot widen the list.
    assert classify(['docs/openapi/broker.yaml'], prose_only=lambda _path: True).tests is False
    assert classify(['discord_core/x.py'], prose_only=lambda _path: True).tests is False


def _git(cwd, *args):
    subprocess.run(['git', *args], cwd=cwd, check=True,  # nosec B603 B607 - fixed argv
                   capture_output=True, text=True)


def _head(cwd):
    return subprocess.run(  # nosec B603 B607 - fixed argv
        ['git', 'rev-parse', 'HEAD'], cwd=cwd, check=True,
        capture_output=True, text=True).stdout.strip()


def _commit(cwd, message):
    _git(cwd, 'add', '-A')
    _git(cwd, 'commit', '-q', '-m', message)


@pytest.fixture(name='repo')
def _repo(tmp_path, monkeypatch):
    '''A throwaway git repo wired in as REPO_ROOT, with a doc, a module and a catalog committed.'''
    monkeypatch.setattr(skippable_changes, 'REPO_ROOT', tmp_path)
    _git(tmp_path, 'init', '-q', '-b', 'main')
    _git(tmp_path, 'config', 'user.email', 'test@example.com')
    _git(tmp_path, 'config', 'user.name', 'Test')
    (tmp_path / 'docs').mkdir()
    (tmp_path / 'docs' / 'guide.md').write_text('guide\n', encoding='utf-8')
    (tmp_path / 'docs' / 'architecture.md').write_text(f'intro\n{_BLOCK}\noutro\n', encoding='utf-8')
    (tmp_path / 'discord_core').mkdir()
    (tmp_path / 'discord_core' / 'module.py').write_text('X = 1\n' * 20, encoding='utf-8')
    (tmp_path / 'catalog-info.yaml').write_text('kind: Component\n', encoding='utf-8')
    _commit(tmp_path, 'base')
    return tmp_path, _head(tmp_path)


def _outputs(capsys):
    '''stdout must be only the GITHUB_OUTPUT lines; diagnostics belong on stderr.'''
    return capsys.readouterr().out.splitlines()


def test_main_skips_tests_for_a_docs_only_change(repo, capsys):
    '''The case this exists for: edit a doc, tox is skipped, the cheap checks are not.'''
    tmp_path, base = repo
    (tmp_path / 'docs' / 'guide.md').write_text('guide, edited\n', encoding='utf-8')
    _commit(tmp_path, 'docs only')

    assert main(['--base', base]) == 0
    assert _outputs(capsys) == ['only=false', 'tests_skippable=true']


def test_main_skips_everything_for_a_metadata_only_change(repo, capsys):
    '''The original rule still holds.'''
    tmp_path, base = repo
    (tmp_path / 'catalog-info.yaml').write_text('kind: Component\nspec: {}\n', encoding='utf-8')
    _commit(tmp_path, 'catalog only')

    assert main(['--base', base]) == 0
    assert _outputs(capsys) == ['only=true', 'tests_skippable=true']


def test_main_runs_tests_when_code_changes_with_docs(repo, capsys):
    '''A doc edit does not carry a code edit past tox.'''
    tmp_path, base = repo
    (tmp_path / 'docs' / 'guide.md').write_text('guide, edited\n', encoding='utf-8')
    (tmp_path / 'discord_core' / 'module.py').write_text('X = 2\n', encoding='utf-8')
    _commit(tmp_path, 'docs and code')

    assert main(['--base', base]) == 0
    assert _outputs(capsys) == ['only=false', 'tests_skippable=false']


def test_main_does_not_let_a_rename_hide_the_deleted_module(repo, capsys):
    '''Moving a module into docs/ is a code change. Without --no-renames git reports only
    the new path, which would read as a docs-only change.'''
    tmp_path, base = repo
    _git(tmp_path, 'mv', 'discord_core/module.py', 'docs/module.md')
    _commit(tmp_path, 'hide a module in docs')

    assert main(['--base', base]) == 0
    assert _outputs(capsys) == ['only=false', 'tests_skippable=false']


def test_main_skips_tests_for_a_prose_edit_to_a_compared_doc(repo, capsys):
    '''The #1152 case: architecture.md is a test input, but only inside its generated block.'''
    tmp_path, base = repo
    (tmp_path / 'docs' / 'architecture.md').write_text(f'a new intro\n{_BLOCK}\nand a new outro\n', encoding='utf-8')
    _commit(tmp_path, 'prose in architecture.md')

    assert main(['--base', base]) == 0
    assert _outputs(capsys) == ['only=false', 'tests_skippable=true']


def test_main_runs_tests_for_an_edit_inside_a_generated_block(repo, capsys):
    '''A hand edit to a generated block is the thing the test compares.'''
    tmp_path, base = repo
    (tmp_path / 'docs' / 'architecture.md').write_text(
        f'intro\n{_BLOCK.replace("body", "body, edited")}\noutro\n', encoding='utf-8')
    _commit(tmp_path, 'edit a generated block')

    assert main(['--base', base]) == 0
    assert _outputs(capsys) == ['only=false', 'tests_skippable=false']


def test_main_runs_tests_when_a_compared_doc_is_deleted(repo, capsys):
    '''A doc gone at head leaves the test with nothing to read.'''
    tmp_path, base = repo
    _git(tmp_path, 'rm', '-q', 'docs/architecture.md')
    _commit(tmp_path, 'delete architecture.md')

    assert main(['--base', base]) == 0
    assert _outputs(capsys) == ['only=false', 'tests_skippable=false']


def test_main_runs_everything_when_nothing_changed(repo, capsys):
    '''An empty diff is not evidence of safety.'''
    _, base = repo

    assert main(['--base', base]) == 0
    assert _outputs(capsys) == ['only=false', 'tests_skippable=false']


# ---------------------------------------------------------------------------
# The premise of edits_outside_generated_blocks, checked against the real docs
# ---------------------------------------------------------------------------

def _render_metrics(text):
    resolved, exempt = metric_census.measure()
    return metric_census.render_metrics_reference_doc(text, resolved, exempt)


def _render_spans(text):
    resolved, exempt = span_census.measure()
    return span_census.render_trace_linking_doc(text, resolved, exempt)


_RENDERERS = {
    'docs/architecture.md': render_architecture_doc,
    'docs/configuration.md': render_configuration_doc,
    'docs/monitoring/metrics_reference.md': _render_metrics,
    'docs/monitoring/trace_linking.md': _render_spans,
}


def test_every_compared_doc_has_a_renderer_here():
    '''A fifth compared doc must come with a check of the premise, not just a list entry.'''
    assert set(_RENDERERS) == set(DOCS_READ_BY_TESTS)


@pytest.mark.parametrize('path', sorted(DOCS_READ_BY_TESTS))
def test_a_prose_edit_does_not_change_what_the_real_doc_check_compares(path):
    '''The check splices a regenerated body into the file and compares. Prose outside the
    blocks passes through it unchanged, so it cannot turn a green test red.'''
    original = (skippable_changes.REPO_ROOT / path).read_text(encoding='utf-8')
    edited = 'A sentence somebody wrote by hand.\n\n' + original + '\nAnd another, at the end.\n'

    assert edits_outside_generated_blocks(path, original, edited) is True
    assert _RENDERERS[path](edited) == edited


@pytest.mark.parametrize('path', sorted(DOCS_READ_BY_TESTS))
def test_an_edit_inside_a_block_does_change_it(path):
    '''The converse, so the rule is not vacuous: the check does catch a hand edit to a block.'''
    original = (skippable_changes.REPO_ROOT / path).read_text(encoding='utf-8')
    edited = original.replace('<!-- END GENERATED', 'A hand edit.\n<!-- END GENERATED', 1)

    assert edits_outside_generated_blocks(path, original, edited) is False
    assert _RENDERERS[path](edited) != edited


# ---------------------------------------------------------------------------
# Guard: no test reads a file this module would skip
# ---------------------------------------------------------------------------

def _test_sources():
    '''Every python file under the central tests/ and each package's own tests/.'''
    root = skippable_changes.REPO_ROOT
    for tests_dir in [root / 'tests', *sorted(root.glob('discord_*/tests'))]:
        yield from sorted(tests_dir.rglob('*.py'))


def _operands(node):
    '''Flatten `a / b / c` into [a, b, c].'''
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
        return _operands(node.left) + _operands(node.right)
    return [node]


def _paths_built_from_repo_root():
    '''Every maximal `REPO_ROOT / 'a' / 'b'` a test source builds, as 'a/b'.

    Stops at the first segment that is not a string literal, so `REPO_ROOT / 'docs' /
    'openapi' / f'{name}.yaml'` reads as the directory `docs/openapi`. Only maximal
    chains count: the inner `REPO_ROOT / 'docs'` of a longer chain is not a read of all
    of docs/.
    '''
    paths = set()
    for source in _test_sources():
        tree = ast.parse(source.read_text(encoding='utf-8'))
        inner = {id(node.left) for node in ast.walk(tree)
                 if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div)}
        for node in ast.walk(tree):
            if not (isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div)) or id(node) in inner:
                continue
            head, *rest = _operands(node)
            if not (isinstance(head, ast.Name) and head.id == 'REPO_ROOT'):
                continue
            segments = []
            for operand in rest:
                if not (isinstance(operand, ast.Constant) and isinstance(operand.value, str)):
                    break
                segments.append(operand.value)
            if segments:
                paths.add('/'.join(segments))
    return paths


def _probe(path):
    '''A file path as is; a directory as a file inside it, since a read of the directory reads them all.'''
    return path if Path(path).suffix else f'{path}/_probe'


def test_the_scan_finds_the_reads_it_is_here_to_protect():
    '''The guard below is vacuous if the scan stops seeing these.'''
    assert {*DOCS_READ_BY_TESTS, 'docs/openapi'} <= _paths_built_from_repo_root()


def test_no_test_reads_a_file_that_skips_tox():
    '''Add a doc a test reads to DOCS_READ_BY_TESTS (or its directory to
    DOC_DIRS_READ_BY_TESTS) in tests/cli/_skippable_changes.py, or CI will skip the
    test that checks it whenever only that file changes.'''
    unprotected = sorted(path for path in _paths_built_from_repo_root() if is_documentation(_probe(path)))
    assert not unprotected, f'tests read files that skip tox: {unprotected}'
