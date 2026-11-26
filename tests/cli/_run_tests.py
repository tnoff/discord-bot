'''
Run tox's pytest passes, narrowed to the packages a pull request can affect.

tox.ini's `commands` calls this instead of spelling the pytest invocations out:

    python -m tests.cli._run_tests

WHAT IT DECIDES, AND FROM WHAT
------------------------------
The selection itself lives in `_affected_tests` (see its docstring for the import
graph and what it will not reason about). This module only answers "what changed" and
turns the answer into commands.

It runs EVERYTHING, exactly the commands tox.ini used to carry, unless it is certain
it can narrow:

  * not a pull request (no GITHUB_BASE_REF, no --base): everything. Local `tox` runs,
    pushes and the weekly run in scheduled.yml are all full runs, which is also what
    makes that weekly run the backstop for the one thing a narrowed run cannot see.
  * the diff could not be read for any reason: everything.
  * `_affected_tests.select` says everything: everything.

HOW IT READS THE DIFF WITHOUT HISTORY
-------------------------------------
The shared tox workflow checks out with the default depth of 1, and for a
`pull_request` event that checkout is the MERGE COMMIT of the PR into its base. So
fetching the base branch's tip (also depth 1) and diffing it against HEAD is exactly
the PR's own change, no merge-base needed, and nothing in the shared workflow has to
change. (If the base moved since the merge commit was made the diff can only be a
superset, which over-selects, never under-selects.) A local `--base REF` uses the
ordinary three-dot diff instead, so the same narrowing can be tried before pushing.

THE TWO PYTEST PASSES, AND WHAT KEEPS PASS 1 ON THE PINNED CORE
---------------------------------------------------------------
`discord_core` is its own released, versioned artifact: every pod PINS it to a tag, and a core change
is meant to be tested on its own, with the pods meeting the new core in the PR that bumps their pin.

  * Pass 1 runs the selected central test files and the pods against the PINNED core, installed from
    each pod's pyproject.toml.
  * Pass 2 runs `discord_core/tests` against THIS checkout.

Left to pytest's defaults pass 1 is not what it says. pytest puts the repo root first on `sys.path`, so
`import discord_core` resolves to the checkout's `discord_core/` and shadows the pinned copy in
site-packages -- from a pod's test, a central test, or the conftest -- and for a long time every test
silently ran against the checkout (reproduced in a venv built the way tox builds one). Three things
make the design true:

  * pyproject.toml sets `--import-mode=append`, which appends the repo root instead of prepending it.
  * Pass 1 runs `python -P -m pytest`: plain `python -m` puts the working directory first, which is the
    same shadowing by another route. Pass 2 is the opposite on purpose (`python -m`, prepend), so core's
    own tests import the checkout.
  * Pass 2 does NOT `pip install -e ./discord_core`. That replaces the pinned core in site-packages for
    good, and on a restored `.tox` cache the next run's pass 1 would start on the checkout without
    anyone noticing. It installs only core's declared DEPENDENCIES (so a PR that bumps one still takes
    effect for core's tests) and lets the working directory decide which core is imported.

A restored `.tox` cache can also hold a core that no longer matches the pods' pin (the cache key does not
hash the per-package pyproject.toml files), so before anything runs the driver checks the installed core
against the pin and, inside a tox environment only, reinstalls it if it differs. Outside tox (a developer's
own venv, which normally has core editable) it never touches the environment, and a narrowed run falls back
to everything instead, because the narrowing assumes the pods are tested against the pin.

CORE IS COVERED BY ITS OWN TESTS
--------------------------------
With pass 1 off the checkout, `discord_core`'s coverage comes from core's own tests alone, so the 100% gate on
the whole tree is also a gate that core is tested on its own terms, not through the pods that use it. (It was
77% when pass 1 was switched off the checkout, and `discord_core/tests` was brought up to 100% in the same PR.)

The central tests are narrowed per file as well (see `_affected_tests`): the ones that read the repo
always run, the rest run when something they reach changed. A narrowed run cannot gate the combined total (most of the tree did not run). It
gates each package whose SOURCE changed instead: every test that can execute that
package is in the run, so the number is what a full run would report for it. A
test-only edit is not gated, for the reason in `Selection`. The pylint and bandit
steps in tox.ini are static and are not narrowed.

Shared tox.yml discovers environments by `tox -l`, matching `py\\d+`, and runs exactly
`tox -e pyXY` -- so none of this can be a second tox environment; it is inside each.
'''
import json
import os
import subprocess  # nosec B404 - fixed argv, no shell
import sys
import tomllib
from pathlib import Path
from typing import Optional

from tests.cli._affected_tests import CORE_ROOT, ImportGraph, Selection, select
from tests.cli._git_diff import REPO_ROOT, base_head_parser, diff_paths, git_output
from tests.cli._package_pins import declared_core_pin
from tests.cli._roots import SERVICE_ROOTS

#: Pass 1: no working directory on sys.path, so the pinned core in site-packages wins.
PASS_1 = ['-P', '-m', 'pytest']
#: Pass 2: working directory first and prepend mode, so the checkout's core wins, as core's own tests want.
PASS_2 = ['-m', 'pytest', '--import-mode=prepend']

# What the installed discord-core is and where `import discord_core` finds it, as JSON.
_CORE_PROBE = '''
import importlib.metadata as metadata, json
import discord_core
raw = metadata.distribution('discord-core').read_text('direct_url.json')
info = json.loads(raw) if raw else {}
print(json.dumps({'file': discord_core.__file__, 'url': info.get('url'),
                  'subdirectory': info.get('subdirectory'),
                  'editable': bool(info.get('dir_info', {}).get('editable'))}))
'''


def core_dependencies(root=REPO_ROOT) -> list:
    '''Core's own declared third-party dependencies (not core itself), as pip requirement strings.'''
    declared = tomllib.loads((root / CORE_ROOT / 'pyproject.toml').read_text(encoding='utf-8'))
    return list(declared.get('project', {}).get('dependencies', []))


def pinned_core_requirement() -> Optional[str]:
    '''The `discord-core @ <url>#subdirectory=...` every pod pins, or None if the pods do not agree on one.

    Disagreement is `_package_pins`' job to report; here it just means there is no single pin to check.
    '''
    pins = {declared_core_pin(root) for root in SERVICE_ROOTS.values()}
    pins.discard(None)
    return pins.pop() if len(pins) == 1 else None


def installed_core(python: str) -> Optional[dict]:
    '''Where `import discord_core` resolves from a fresh interpreter with no working directory on the path,
    and what the installed distribution is. None if core cannot be imported at all.'''
    try:
        result = subprocess.run([python, '-P', '-c', _CORE_PROBE], cwd=REPO_ROOT, capture_output=True,  # nosec B603
                                text=True, check=True)
    except (subprocess.CalledProcessError, OSError):
        return None
    return json.loads(result.stdout.strip().splitlines()[-1])


def core_is_pinned(state: Optional[dict], pin: str) -> bool:
    '''True when the installed core is the pinned archive, installed non-editably, and is not the checkout's own copy.'''
    # The checkout's own package directory, NOT "anything under the repo": tox keeps its environments in
    # .tox/ inside the repo, so the pinned copy in site-packages is legitimately under REPO_ROOT.
    if state is None or state['editable'] or Path(state['file']).resolve().parent == (REPO_ROOT / CORE_ROOT).resolve():
        return False
    url, _, fragment = pin.split(' @ ', 1)[1].partition('#')
    subdirectory = dict(part.split('=', 1) for part in fragment.split('&') if '=' in part).get('subdirectory')
    return state['url'] == url and state['subdirectory'] == subdirectory


def ensure_pinned_core(python: str, environ=None, repair: bool = True) -> bool:
    '''Whether pass 1 will test the pods against the pinned core, repairing the install if it may.

    Repair (a force-reinstall of the pin, no dependencies) happens only inside a tox environment, which is
    disposable. Anywhere else the environment is somebody's own and is never touched.
    '''
    environ = os.environ if environ is None else environ
    pin = pinned_core_requirement()
    if pin is None:
        return False
    if core_is_pinned(installed_core(python), pin):
        return True
    if not (repair and environ.get('TOX_ENV_NAME')):
        return False
    print('run-tests: the installed discord-core is not the pods\' pin; reinstalling it', file=sys.stderr)
    subprocess.run([python, '-m', 'pip', 'install', '--no-deps', '--force-reinstall', pin],  # nosec B603
                   cwd=REPO_ROOT, check=True)
    return core_is_pinned(installed_core(python), pin)


def changed_files(base: Optional[str] = None, environ=None, head: str = 'HEAD') -> Optional[list]:
    '''The paths this pull request changes, or None when that cannot be determined.

    base: a local ref to diff `head` against (three-dot). Otherwise, in CI, GITHUB_BASE_REF
    names the branch to fetch, and the PR is the difference between that tip and HEAD.
    '''
    environ = os.environ if environ is None else environ
    try:
        if base:
            return diff_paths(base, head, '--no-renames')
        base_ref = environ.get('GITHUB_BASE_REF')
        if not base_ref:
            return None
        git_output('fetch', '--no-tags', '--depth=1', '--quiet', 'origin', base_ref)
        result = git_output('diff', '--name-only', '--no-renames', 'FETCH_HEAD', 'HEAD')
        return [line.strip() for line in result.stdout.splitlines() if line.strip()]
    except (subprocess.CalledProcessError, OSError):
        return None


def _core_install(python: str) -> list:
    '''Install core's declared dependencies, never core itself (see the module doc). Empty if it has none.'''
    deps = core_dependencies()
    return [[python, '-m', 'pip', 'install', *deps]] if deps else []


def _gate(python: str, gated: Optional[tuple]) -> list:
    '''The coverage report that can fail the run: 100%, over the whole tree or over the gated packages.

    gated=None means the whole tree (a full run). Core is a package like any other here: its own tests are the
    only thing that covers it, so it is gated at 100% too.
    '''
    if gated is None:
        return [[python, '-m', 'coverage', 'report', '--skip-covered', '--fail-under=100']]
    include = ','.join(f'{REPO_ROOT / package}/*' for package in gated)
    return [[python, '-m', 'coverage', 'report', f'--include={include}', '--skip-covered', '--fail-under=100']]


def full_plan(python: str) -> list:
    '''The whole suite: pass 1 against the pinned core, pass 2 against the checkout, then the 100% gate.'''
    return [
        [python, *PASS_1, f'--ignore={CORE_ROOT}/tests', '--cov', '--cov-report='],
        *_core_install(python),
        [python, *PASS_2, f'{CORE_ROOT}/tests', '--cov', '--cov-append', '--cov-report=xml'],
        *_gate(python, None),
    ]


def narrowed_plan(selection: Selection, python: str) -> list:
    '''Commands for only the selected packages and central test files.

    The central files are passed one by one (an explicit file is collected whatever
    `python_files` says). With nothing at all to pass, bare `pytest` would collect the
    whole repo and read as "narrowed" in the log, so that case is the full plan instead.
    '''
    pods = [package for package in selection.packages if package != CORE_ROOT]
    paths = [*selection.central, *(f'{pod}/tests' for pod in pods)]
    core_selected = CORE_ROOT in selection.packages
    if not paths and not core_selected:
        return full_plan(python)
    commands = []
    if paths:
        commands.append([python, *PASS_1, *paths, '--cov', '--cov-report='])
    if core_selected:
        commands.extend(_core_install(python))
        commands.append([python, *PASS_2, f'{CORE_ROOT}/tests', '--cov', *(['--cov-append'] if paths else []),
                         '--cov-report='])
    # The workflow uploads coverage.xml for diff-cover, so it must exist either way.
    commands.append([python, '-m', 'coverage', 'xml'])
    if selection.gated:
        commands.extend(_gate(python, selection.gated))
    return commands


def plan(selection: Optional[Selection], python: str = sys.executable) -> list:
    '''The commands for a selection; None (or "everything") is the full plan.'''
    if selection is None or selection.everything:
        return full_plan(python)
    return narrowed_plan(selection, python)


def decide(base: Optional[str] = None, environ=None, head: str = 'HEAD', isolated: bool = True) -> Optional[Selection]:
    '''The narrowed selection, or None when the full suite has to run (with the reason on stderr).

    `isolated` says pass 1 will really test the pods against the pinned core. The narrowing leaves core out of
    every pod's reach on that basis, so when it does not hold the only safe answer is everything.
    '''
    if not isolated:
        print('run-tests: the installed discord-core is not the pods\' pin (or is editable), so tests would import '
              'the checkout; running everything', file=sys.stderr)
        return None
    changed = changed_files(base, environ, head)
    if changed is None:
        print('run-tests: no readable pull-request diff, running everything', file=sys.stderr)
        return None
    try:
        selection = select(changed, ImportGraph())
    except (SyntaxError, OSError, UnicodeDecodeError) as error:
        print(f'run-tests: could not build the import graph ({error}), running everything', file=sys.stderr)
        return None
    if selection.everything:
        print(f'run-tests: running everything: {selection.reason}', file=sys.stderr)
        return None
    print(f'run-tests: narrowed to {", ".join(selection.packages) or "no package"} and '
          f'{len(selection.central)} central test file(s) '
          f'(coverage gated on: {", ".join(selection.gated) or "nothing"}); {selection.reason}',
          file=sys.stderr)
    return selection


def main(argv=None) -> int:
    '''Decide, then run the plan, stopping at the first failing command.'''
    parser = base_head_parser(__doc__, base_required=False,
                              base_help='diff against this local ref instead of GITHUB_BASE_REF')
    parser.add_argument('--dry-run', action='store_true', help='print the commands instead of running them')
    args = parser.parse_args(argv)
    isolated = ensure_pinned_core(sys.executable, repair=not args.dry_run)
    commands = plan(decide(args.base, head=args.head, isolated=isolated))
    for command in commands:
        print('+', ' '.join(command), file=sys.stderr)
        if args.dry_run:
            continue
        code = subprocess.run(command, cwd=REPO_ROOT, check=False).returncode  # nosec B603 - fixed argv
        if code:
            return code
    return 0


if __name__ == '__main__':
    sys.exit(main())
