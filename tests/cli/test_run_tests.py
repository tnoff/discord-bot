'''
`_run_tests` turns "what did this PR change" into pytest commands, and the dangerous failure is
a narrowed run when it should have been a full one. So the shape of the safe default is pinned
down first: anything unreadable, unknown or non-PR is the FULL plan, byte for byte what tox.ini
carried before narrowing existed.

The diff logic is exercised against real git repos, including the shape CI has -- a clone at
depth 1 whose HEAD is a merge commit -- because `git fetch --depth=1` followed by a two-dot diff
is the whole reason no shared-workflow change is needed, and a mock cannot prove that.
'''
import subprocess

import pytest

import tests.cli._git_diff as git_diff
import tests.cli._run_tests as run_tests
from tests.cli._affected_tests import Selection
from tests.cli._run_tests import changed_files, decide, full_plan, main, narrowed_plan, plan
from tests.cli._scratch_repo import commit, git, init_scratch_repo

PY = 'python'


def _write(root, name, text='x\n'):
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding='utf-8')


def test_full_plan_is_what_tox_carried_before():
    '''Two passes, one gated total, core installed editable between them.'''
    assert full_plan(PY) == [
        ['python', '-m', 'pytest', '--ignore=discord_core/tests', '--cov', '--cov-report='],
        ['python', '-m', 'pip', 'install', '-e', './discord_core'],
        ['python', '-m', 'pytest', 'discord_core/tests', '--cov', '--cov-append',
         '--cov-report=xml', '--cov-fail-under=100'],
    ]


@pytest.mark.parametrize('selection', [None, Selection(True, (), (), 'why')])
def test_no_selection_or_everything_is_the_full_plan(selection):
    '''The two ways of saying "could not narrow".'''
    assert plan(selection, PY) == full_plan(PY)


def test_narrowed_plan_for_pods_runs_central_tests_and_gates_changed_source():
    '''Central tests always; only the selected pods' trees; the gate on gated packages only.'''
    selection = Selection(False, ('discord_db', 'discord_gateway'), ('discord_db',), 'why')
    commands = narrowed_plan(selection, PY)
    assert commands[0] == ['python', '-m', 'pytest', 'tests', 'discord_db/tests', 'discord_gateway/tests',
                           '--cov', '--cov-report=']
    assert ['python', '-m', 'coverage', 'xml'] in commands
    gate = commands[-1]
    assert gate[:4] == ['python', '-m', 'coverage', 'report']
    assert gate[4].startswith('--include=') and gate[4].endswith('/discord_db/*')
    assert 'discord_gateway' not in gate[4]
    assert gate[5:] == ['--skip-covered', '--fail-under=100']
    assert not any('pip' in command for command in commands)


def test_narrowed_plan_with_core_installs_this_checkout_for_its_own_pass():
    '''Core is tested against THIS checkout, after the pods, with coverage appended.'''
    selection = Selection(False, ('discord_core',), ('discord_core',), 'why')
    commands = narrowed_plan(selection, PY)
    assert commands[0] == ['python', '-m', 'pytest', 'tests', '--cov', '--cov-report=']
    assert commands[1] == ['python', '-m', 'pip', 'install', '-e', './discord_core']
    assert commands[2] == ['python', '-m', 'pytest', 'discord_core/tests', '--cov', '--cov-append',
                           '--cov-report=']


def test_narrowed_plan_with_nothing_selected_still_writes_coverage_xml_and_does_not_gate():
    '''The workflow uploads coverage.xml unconditionally, so it must exist; an empty gate would
    be `--include=` of nothing.'''
    commands = narrowed_plan(Selection(False, (), (), 'why'), PY)
    assert commands == [['python', '-m', 'pytest', 'tests', '--cov', '--cov-report='],
                        ['python', '-m', 'coverage', 'xml']]


def test_test_only_selection_is_not_gated():
    '''Selected but not gated: runs, reports xml, enforces nothing.'''
    commands = narrowed_plan(Selection(False, ('discord_gateway',), (), 'why'), PY)
    assert not any('report' in command for command in commands)


# --- reading the diff ---------------------------------------------------------------------------


def test_not_a_pull_request_is_unknown():
    '''No GITHUB_BASE_REF and no --base: a push, a schedule, a local run. Full suite.'''
    assert changed_files(None, {}) is None


def test_local_base_uses_the_three_dot_diff(tmp_path, monkeypatch):
    '''`--base REF` for trying the narrowing before pushing.'''
    init_scratch_repo(tmp_path, monkeypatch)
    _write(tmp_path, 'a.txt')
    commit(tmp_path, 'base')
    git(tmp_path, 'branch', 'base')
    _write(tmp_path, 'b.txt')
    commit(tmp_path, 'change')
    assert changed_files('base', {}) == ['b.txt']


def test_ci_checkout_shape_diffs_the_merge_commit_against_the_fetched_base_tip(tmp_path, monkeypatch):
    '''What a `pull_request` run looks like: a depth-1 clone whose HEAD is the merge commit.

    The PR branch is merged into main with --no-ff and published as refs/pull/1/merge, the way
    GitHub does, then checked out in a SHALLOW clone -- so the merge commit's parents are not
    available and nothing but the base tip, fetched at depth 1, can be diffed against. The result
    must be the PR's own files, and ONLY those, not whatever else landed on main.'''
    origin = tmp_path / 'origin'
    origin.mkdir()
    init_scratch_repo(origin, monkeypatch)
    _write(origin, 'shared.txt')
    commit(origin, 'base')
    git(origin, 'checkout', '-q', '-b', 'feature')
    _write(origin, 'from_pr.txt')
    commit(origin, 'pr work')
    git(origin, 'checkout', '-q', 'main')
    _write(origin, 'from_main.txt')
    commit(origin, 'someone else lands first')
    git(origin, 'merge', '-q', '--no-ff', '-m', 'merge pr', 'feature')
    git(origin, 'update-ref', 'refs/pull/1/merge', 'HEAD')
    git(origin, 'reset', '-q', '--hard', 'HEAD~1')   # main is back to before the merge

    work = tmp_path / 'work'
    git(tmp_path, 'clone', '-q', '--depth=1', '--no-checkout', f'file://{origin}', str(work))
    git(work, 'fetch', '-q', '--depth=1', '--no-tags', 'origin', 'refs/pull/1/merge')
    git(work, 'checkout', '-q', '--detach', 'FETCH_HEAD')
    assert git_diff.git_output('rev-parse', '--is-shallow-repository', cwd=work).stdout.strip() == 'true'
    monkeypatch.setattr(git_diff, 'REPO_ROOT', work)

    assert changed_files(None, {'GITHUB_BASE_REF': 'main'}) == ['from_pr.txt']


def test_an_unfetchable_base_is_unknown_not_an_error(tmp_path, monkeypatch):
    '''A failed fetch must fall back to the full suite, not crash the tox run.'''
    init_scratch_repo(tmp_path, monkeypatch)
    _write(tmp_path, 'a.txt')
    commit(tmp_path, 'base')
    assert changed_files(None, {'GITHUB_BASE_REF': 'no-such-branch'}) is None


def test_a_bad_local_base_is_unknown(tmp_path, monkeypatch):
    '''Same for --base naming a ref that does not exist.'''
    init_scratch_repo(tmp_path, monkeypatch)
    _write(tmp_path, 'a.txt')
    commit(tmp_path, 'base')
    assert changed_files('no-such-ref', {}) is None


# --- deciding -----------------------------------------------------------------------------------


def test_decide_runs_everything_when_the_diff_is_unknown(monkeypatch, capsys):
    '''Unreadable diff -> None -> full plan, with the reason on stderr.'''
    monkeypatch.setattr(run_tests, 'changed_files', lambda base, environ=None, head='HEAD': None)
    assert decide() is None
    assert 'running everything' in capsys.readouterr().err


def test_decide_runs_everything_when_the_graph_cannot_be_built(monkeypatch, capsys):
    '''A file that does not parse is a failing PR anyway, but must not take the driver down.'''
    monkeypatch.setattr(run_tests, 'changed_files', lambda base, environ=None, head='HEAD': ['discord_db/x.py'])

    def broken(*_args, **_kwargs):
        raise SyntaxError('bad')
    monkeypatch.setattr(run_tests, 'select', broken)
    monkeypatch.setattr(run_tests, 'ImportGraph', lambda: None)
    assert decide() is None
    assert 'import graph' in capsys.readouterr().err


def test_decide_runs_everything_when_selection_says_so(monkeypatch, capsys):
    '''select() saying "everything" is passed through as None.'''
    monkeypatch.setattr(run_tests, 'changed_files', lambda base, environ=None, head='HEAD': ['tox.ini'])
    monkeypatch.setattr(run_tests, 'select', lambda changed, graph: Selection(True, (), (), 'tox.ini is odd'))
    monkeypatch.setattr(run_tests, 'ImportGraph', lambda: None)
    assert decide() is None
    assert 'tox.ini is odd' in capsys.readouterr().err


def test_decide_returns_the_narrowed_selection_and_logs_it(monkeypatch, capsys):
    '''The CI log must say what was narrowed to, and what is gated.'''
    narrowed = Selection(False, ('discord_db',), ('discord_db',), '1 changed node')
    monkeypatch.setattr(run_tests, 'changed_files', lambda base, environ=None, head='HEAD': ['discord_db/x.py'])
    monkeypatch.setattr(run_tests, 'select', lambda changed, graph: narrowed)
    monkeypatch.setattr(run_tests, 'ImportGraph', lambda: None)
    assert decide() is narrowed
    err = capsys.readouterr().err
    assert 'narrowed to discord_db' in err and 'gated on: discord_db' in err


def test_decide_names_the_central_tests_when_no_package_is_selected(monkeypatch, capsys):
    '''A central-only change narrows to nothing but the always-on central tree.'''
    monkeypatch.setattr(run_tests, 'changed_files', lambda base, environ=None, head='HEAD': ['tests/test_x.py'])
    monkeypatch.setattr(run_tests, 'select', lambda changed, graph: Selection(False, (), (), 'r'))
    monkeypatch.setattr(run_tests, 'ImportGraph', lambda: None)
    assert decide() is not None
    err = capsys.readouterr().err
    assert 'the central tests only' in err and 'gated on: nothing' in err


# --- main ---------------------------------------------------------------------------------------


def test_main_dry_run_prints_and_runs_nothing(monkeypatch, capsys):
    '''--dry-run is how to see a plan locally.'''
    monkeypatch.setattr(run_tests, 'decide', lambda base=None, head='HEAD': None)
    monkeypatch.setattr(subprocess, 'run', lambda *a, **k: pytest.fail('ran a command'))
    assert main(['--dry-run']) == 0
    assert 'pytest --ignore=discord_core/tests' in capsys.readouterr().err


def test_main_stops_at_the_first_failing_command(monkeypatch):
    '''Same as tox: a red pass 1 must not be followed by pass 2 and a green exit.'''
    ran = []

    def fake_run(command, **_kwargs):
        ran.append(command)
        return subprocess.CompletedProcess(command, 3 if len(ran) == 1 else 0)
    monkeypatch.setattr(run_tests, 'decide', lambda base=None, head='HEAD': None)
    monkeypatch.setattr(subprocess, 'run', fake_run)
    assert main([]) == 3
    assert len(ran) == 1


def test_main_returns_zero_when_every_command_passes(monkeypatch):
    '''And runs all of them, in order, from the repo root.'''
    ran = []

    def fake_run(command, **kwargs):
        ran.append((command, kwargs['cwd']))
        return subprocess.CompletedProcess(command, 0)
    monkeypatch.setattr(run_tests, 'decide', lambda base=None, head='HEAD': None)
    monkeypatch.setattr(subprocess, 'run', fake_run)
    assert main([]) == 0
    assert [command[2] for command, _ in ran] == ['pytest', 'pip', 'pytest']
    assert all(cwd == run_tests.REPO_ROOT for _, cwd in ran)
