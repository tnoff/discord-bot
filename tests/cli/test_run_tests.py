'''
`_run_tests` turns "what did this PR change" into pytest commands, and the dangerous failure is
a narrowed run when it should have been a full one. So the shape of the safe default is pinned
down first: anything unreadable, unknown or non-PR is the FULL plan, byte for byte what tox.ini
carried before narrowing existed.

The diff logic is exercised against real git repos, including the shape CI has -- a clone at
depth 1 whose HEAD is a merge commit -- because `git fetch --depth=1` followed by a two-dot diff
is the whole reason no shared-workflow change is needed, and a mock cannot prove that.
'''
import json
import subprocess

import pytest

import tests.cli._git_diff as git_diff
import tests.cli._run_tests as run_tests
from tests.cli._affected_tests import Selection
from tests.cli._run_tests import (CORE_COVERAGE_FLOOR, changed_files, core_is_pinned, decide, ensure_pinned_core,
                                  full_plan, installed_core, main, narrowed_plan, plan)
from tests.cli._scratch_repo import commit, git, init_scratch_repo

PY = 'python'
real_core_dependencies = run_tests.core_dependencies   # the fixture below replaces the module's
CORE_DEPS = ['aiohttp>=3.9', 'redis==8.2.0']
PIN = ('discord-core @ https://github.com/example/repo/archive/refs/tags/core-v1.2.3.tar.gz'
       '#subdirectory=discord_core')
PIN_URL = 'https://github.com/example/repo/archive/refs/tags/core-v1.2.3.tar.gz'


@pytest.fixture(autouse=True)
def fixed_core_dependencies(monkeypatch):
    '''Plans install core's declared dependencies; pin them so the tests do not track pyproject.toml.'''
    monkeypatch.setattr(run_tests, 'core_dependencies', lambda root=None: list(CORE_DEPS))


def _write(root, name, text='x\n'):
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding='utf-8')


def test_full_plan_tests_the_pods_against_the_pin_and_core_against_the_checkout():
    '''Pass 1 without the working directory on the path (the pinned core wins), then core's dependencies
    but NOT core itself, then pass 2 the other way round, then every gate.'''
    plan_ = full_plan(PY)
    assert plan_[0] == ['python', '-P', '-m', 'pytest', '--ignore=discord_core/tests', '--cov', '--cov-report=']
    assert plan_[1] == ['python', '-m', 'pip', 'install', *CORE_DEPS]
    assert plan_[2] == ['python', '-m', 'pytest', '--import-mode=prepend', 'discord_core/tests', '--cov',
                        '--cov-append', '--cov-report=xml']
    gates = plan_[3:]
    assert [gate[3] for gate in gates] == ['report', 'report']
    assert gates[0][4].startswith('--omit=') and gates[0][4].endswith('/discord_core/*')
    assert gates[0][-1] == '--fail-under=100'
    assert gates[1][4].startswith('--include=') and gates[1][4].endswith('/discord_core/*')
    assert gates[1][-1] == f'--fail-under={CORE_COVERAGE_FLOOR}'


def test_no_plan_ever_installs_core_itself():
    '''An editable install would replace the pinned core in site-packages, and on a restored .tox cache
    the next run's pass 1 would silently start on the checkout.'''
    selection = Selection(False, ('discord_core', 'discord_db'), ('discord_core',), 'why', ('tests/cli/test_a.py',))
    for command in [*full_plan(PY), *narrowed_plan(selection, PY)]:
        assert '-e' not in command and './discord_core' not in command


def test_core_dependencies_are_read_from_core_and_exclude_core():
    '''The real list, unpatched: core's third-party requirements, never core itself (see the module doc).'''
    deps = real_core_dependencies()
    assert deps, 'core declares no dependencies, so the plan would install nothing and this test checks nothing'
    assert all(not dep.lower().startswith('discord') for dep in deps), deps
    assert any(dep.lower().startswith('aiohttp') for dep in deps), deps


@pytest.mark.parametrize('selection', [None, Selection(True, (), (), 'why')])
def test_no_selection_or_everything_is_the_full_plan(selection):
    '''The two ways of saying "could not narrow".'''
    assert plan(selection, PY) == full_plan(PY)


def test_narrowed_plan_runs_selected_central_files_and_pods_and_gates_changed_source():
    '''The selected central files (one by one), then the selected pods' trees; the gate on gated packages only.'''
    selection = Selection(False, ('discord_db', 'discord_gateway'), ('discord_db',), 'why',
                          ('tests/cli/test_a.py', 'tests/clients/test_b.py'))
    commands = narrowed_plan(selection, PY)
    assert commands[0] == ['python', '-P', '-m', 'pytest', 'tests/cli/test_a.py', 'tests/clients/test_b.py',
                           'discord_db/tests', 'discord_gateway/tests', '--cov', '--cov-report=']
    assert ['python', '-m', 'coverage', 'xml'] in commands
    gate = commands[-1]
    assert gate[:4] == ['python', '-m', 'coverage', 'report']
    assert gate[4].startswith('--include=') and gate[4].endswith('/discord_db/*')
    assert 'discord_gateway' not in gate[4]
    assert gate[5:] == ['--skip-covered', '--fail-under=100']
    assert not any('pip' in command for command in commands)


def test_narrowed_plan_never_passes_the_whole_central_directory():
    '''`tests` as a path would collect every central file and make the per-file selection a lie.'''
    selection = Selection(False, ('discord_db',), (), 'why', ('tests/cli/test_a.py',))
    assert 'tests' not in narrowed_plan(selection, PY)[0]


def test_narrowed_plan_with_core_runs_its_own_tests_against_the_checkout_and_gates_the_floor():
    '''Core is tested after the pods, on its own terms, with coverage appended and the floor as its gate.'''
    selection = Selection(False, ('discord_core',), ('discord_core',), 'why', ('tests/cli/test_a.py',))
    commands = narrowed_plan(selection, PY)
    assert commands[0] == ['python', '-P', '-m', 'pytest', 'tests/cli/test_a.py', '--cov', '--cov-report=']
    assert commands[1] == ['python', '-m', 'pip', 'install', *CORE_DEPS]
    assert commands[2] == ['python', '-m', 'pytest', '--import-mode=prepend', 'discord_core/tests', '--cov',
                           '--cov-append', '--cov-report=']
    assert commands[-1][-1] == f'--fail-under={CORE_COVERAGE_FLOOR}'
    assert commands[-1][4].endswith('/discord_core/*')


def test_a_core_only_selection_runs_core_alone_without_cov_append():
    '''Nothing ran before it, so there is no data to append to.'''
    commands = narrowed_plan(Selection(False, ('discord_core',), ('discord_core',), 'why'), PY)
    assert commands[0] == ['python', '-m', 'pip', 'install', *CORE_DEPS]
    assert commands[1] == ['python', '-m', 'pytest', '--import-mode=prepend', 'discord_core/tests', '--cov',
                           '--cov-report=']


def test_pods_and_core_are_gated_separately():
    '''Pods at 100, core at its floor, in two reports so each can fail on its own.'''
    selection = Selection(False, ('discord_core', 'discord_db'), ('discord_core', 'discord_db'), 'why', ('tests/a.py',))
    gates = [command for command in narrowed_plan(selection, PY) if command[3:4] == ['report']]
    assert len(gates) == 2
    assert gates[0][-1] == '--fail-under=100' and gates[0][4].endswith('/discord_db/*')
    assert gates[1][-1] == f'--fail-under={CORE_COVERAGE_FLOOR}'


def test_narrowed_plan_with_central_files_only_still_writes_coverage_xml_and_does_not_gate():
    '''The workflow uploads coverage.xml unconditionally, so it must exist; an empty gate would
    be `--include=` of nothing.'''
    commands = narrowed_plan(Selection(False, (), (), 'why', ('tests/cli/test_a.py',)), PY)
    assert commands == [['python', '-P', '-m', 'pytest', 'tests/cli/test_a.py', '--cov', '--cov-report='],
                        ['python', '-m', 'coverage', 'xml']]


def test_narrowed_plan_with_nothing_to_run_is_the_full_plan():
    '''Bare `pytest` with no path collects the whole repo while the log says "narrowed". Refuse to
    build that: no paths at all and no core means something is wrong, so run everything.'''
    assert narrowed_plan(Selection(False, (), (), 'why'), PY) == full_plan(PY)


def test_test_only_selection_is_not_gated():
    '''Selected but not gated: runs, reports xml, enforces nothing.'''
    commands = narrowed_plan(Selection(False, ('discord_gateway',), (), 'why'), PY)
    assert not any('report' in command for command in commands)


# --- which core the tests import -----------------------------------------------------------------


def _state(file='/venv/lib/python3.13/site-packages/discord_core/__init__.py', url=PIN_URL,
           subdirectory='discord_core', editable=False):
    return {'file': file, 'url': url, 'subdirectory': subdirectory, 'editable': editable}


def test_the_pinned_core_is_recognised():
    '''Installed from the pinned archive, not editable, and not the checkout.'''
    assert core_is_pinned(_state(), PIN)


def test_a_pinned_core_inside_a_venv_inside_the_repo_is_still_the_pin():
    '''tox keeps its environments in .tox/ INSIDE the repo, so the pinned copy in site-packages is under
    REPO_ROOT. A "not under the repo" test said it was not isolated, in the first real tox run.'''
    inside = run_tests.REPO_ROOT / '.tox' / 'py313' / 'lib' / 'python3.13' / 'site-packages' / 'discord_core' / '__init__.py'
    assert core_is_pinned(_state(file=str(inside)), PIN)


@pytest.mark.parametrize('state', [
    None,
    _state(editable=True),
    _state(file=str(run_tests.REPO_ROOT / 'discord_core' / '__init__.py')),
    _state(url='https://github.com/example/repo/archive/refs/tags/core-v9.9.9.tar.gz'),
    _state(subdirectory=None),
])
def test_anything_else_is_not_the_pinned_core(state):
    '''Editable, the checkout, another tag, another subdirectory, or no core at all.'''
    assert not core_is_pinned(state, PIN)


def _fake_probe(states, ran):
    '''A subprocess.run that answers the core probe from `states` in order and records everything else.'''
    queue = list(states)

    def fake(command, **kwargs):
        if '-c' in command:
            state = queue.pop(0)
            if state is None:
                raise subprocess.CalledProcessError(1, command)
            return subprocess.CompletedProcess(command, 0, stdout='noise\n' + json.dumps(state) + '\n')
        ran.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0)
    return fake


def test_installed_core_reads_the_probe_and_survives_a_failed_import(monkeypatch):
    '''A broken or absent core is None, not an exception.'''
    monkeypatch.setattr(subprocess, 'run', _fake_probe([_state(), None], []))
    assert installed_core(PY)['url'] == PIN_URL
    assert installed_core(PY) is None


def test_isolation_holds_when_the_pin_is_installed(monkeypatch):
    '''Nothing to repair.'''
    ran = []
    monkeypatch.setattr(run_tests, 'pinned_core_requirement', lambda: PIN)
    monkeypatch.setattr(subprocess, 'run', _fake_probe([_state()], ran))
    assert ensure_pinned_core(PY, environ={'TOX_ENV_NAME': 'py313'}) is True
    assert not ran


def test_a_stale_core_in_a_tox_environment_is_repaired(monkeypatch):
    '''The restored-cache case: core is editable (or the wrong tag). Reinstall exactly the pin, no deps.'''
    ran = []
    monkeypatch.setattr(run_tests, 'pinned_core_requirement', lambda: PIN)
    monkeypatch.setattr(subprocess, 'run', _fake_probe([_state(editable=True), _state()], ran))
    assert ensure_pinned_core(PY, environ={'TOX_ENV_NAME': 'py313'}) is True
    assert ran[0][0] == ['python', '-m', 'pip', 'install', '--no-deps', '--force-reinstall', PIN]


def test_a_repair_that_does_not_take_is_reported_not_hidden(monkeypatch):
    '''Still not the pin afterwards: say so (the run then falls back to everything).'''
    monkeypatch.setattr(run_tests, 'pinned_core_requirement', lambda: PIN)
    monkeypatch.setattr(subprocess, 'run', _fake_probe([_state(editable=True), _state(editable=True)], []))
    assert ensure_pinned_core(PY, environ={'TOX_ENV_NAME': 'py313'}) is False


def test_outside_tox_the_environment_is_never_touched(monkeypatch):
    '''A developer's venv normally has core editable. Report it, do not reinstall over it.'''
    ran = []
    monkeypatch.setattr(run_tests, 'pinned_core_requirement', lambda: PIN)
    monkeypatch.setattr(subprocess, 'run', _fake_probe([_state(editable=True)], ran))
    assert ensure_pinned_core(PY, environ={}) is False
    assert not ran


def test_a_dry_run_never_repairs(monkeypatch):
    '''--dry-run has no side effects, even inside tox.'''
    ran = []
    monkeypatch.setattr(run_tests, 'pinned_core_requirement', lambda: PIN)
    monkeypatch.setattr(subprocess, 'run', _fake_probe([_state(editable=True)], ran))
    assert ensure_pinned_core(PY, environ={'TOX_ENV_NAME': 'py313'}, repair=False) is False
    assert not ran


def test_no_single_pin_means_no_isolation_claim(monkeypatch):
    '''If the pods disagree there is nothing to check against; test_package_pins reports that.'''
    monkeypatch.setattr(run_tests, 'pinned_core_requirement', lambda: None)
    assert ensure_pinned_core(PY, environ={'TOX_ENV_NAME': 'py313'}) is False


def test_the_real_pin_is_a_single_archive_requirement():
    '''pinned_core_requirement reads the six pods; they must agree on one `discord-core @ <url>#...`.'''
    pin = run_tests.pinned_core_requirement()
    assert pin is not None and pin.startswith('discord-core @ https://') and '#subdirectory=discord_core' in pin


def test_pytest_is_configured_to_append_the_repo_root():
    '''The setting that stops the checkout's core shadowing the pinned one. If this goes, every test
    quietly imports the checkout again and pass 1 is no longer what it says.'''
    text = (run_tests.REPO_ROOT / 'pyproject.toml').read_text(encoding='utf-8')
    assert 'addopts = ["--import-mode=append"]' in text


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
    narrowed = Selection(False, ('discord_db',), ('discord_db',), '1 changed node', ('tests/cli/test_a.py',))
    monkeypatch.setattr(run_tests, 'changed_files', lambda base, environ=None, head='HEAD': ['discord_db/x.py'])
    monkeypatch.setattr(run_tests, 'select', lambda changed, graph: narrowed)
    monkeypatch.setattr(run_tests, 'ImportGraph', lambda: None)
    assert decide() is narrowed
    err = capsys.readouterr().err
    assert 'narrowed to discord_db and 1 central test file(s)' in err and 'gated on: discord_db' in err


def test_decide_says_no_package_when_only_central_files_are_selected(monkeypatch, capsys):
    '''A change only central tests care about narrows to those files alone.'''
    monkeypatch.setattr(run_tests, 'changed_files', lambda base, environ=None, head='HEAD': ['tests/test_x.py'])
    monkeypatch.setattr(run_tests, 'select', lambda changed, graph: Selection(False, (), (), 'r', ('tests/cli/test_a.py',)))
    monkeypatch.setattr(run_tests, 'ImportGraph', lambda: None)
    assert decide() is not None
    err = capsys.readouterr().err
    assert 'narrowed to no package and 1 central test file(s)' in err and 'gated on: nothing' in err


# --- main ---------------------------------------------------------------------------------------


def test_main_dry_run_prints_and_runs_nothing(monkeypatch, capsys):
    '''--dry-run is how to see a plan locally.'''
    monkeypatch.setattr(run_tests, 'ensure_pinned_core', lambda python, repair=True: True)
    monkeypatch.setattr(run_tests, 'decide', lambda base=None, head='HEAD', isolated=True: None)
    monkeypatch.setattr(subprocess, 'run', lambda *a, **k: pytest.fail('ran a command'))
    assert main(['--dry-run']) == 0
    assert 'pytest --ignore=discord_core/tests' in capsys.readouterr().err


def test_main_stops_at_the_first_failing_command(monkeypatch):
    '''Same as tox: a red pass 1 must not be followed by pass 2 and a green exit.'''
    monkeypatch.setattr(run_tests, 'ensure_pinned_core', lambda python, repair=True: True)
    ran = []

    def fake_run(command, **_kwargs):
        ran.append(command)
        return subprocess.CompletedProcess(command, 3 if len(ran) == 1 else 0)
    monkeypatch.setattr(run_tests, 'decide', lambda base=None, head='HEAD', isolated=True: None)
    monkeypatch.setattr(subprocess, 'run', fake_run)
    assert main([]) == 3
    assert len(ran) == 1


def test_main_returns_zero_when_every_command_passes(monkeypatch):
    '''And runs all of them, in order, from the repo root.'''
    monkeypatch.setattr(run_tests, 'ensure_pinned_core', lambda python, repair=True: True)
    ran = []

    def fake_run(command, **kwargs):
        ran.append((command, kwargs['cwd']))
        return subprocess.CompletedProcess(command, 0)
    monkeypatch.setattr(run_tests, 'decide', lambda base=None, head='HEAD', isolated=True: None)
    monkeypatch.setattr(subprocess, 'run', fake_run)
    assert main([]) == 0
    assert ran[0][0][1:4] == ['-P', '-m', 'pytest']      # pass 1: pinned core
    assert ran[1][0][1:4] == ['-m', 'pip', 'install']    # core's dependencies only
    assert ran[2][0][1:4] == ['-m', 'pytest', '--import-mode=prepend']   # pass 2: the checkout
    assert all(cwd == run_tests.REPO_ROOT for _, cwd in ran)
