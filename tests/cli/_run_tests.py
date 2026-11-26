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

THE TWO PYTEST PASSES, AND WHY THEY ARE TWO
-------------------------------------------
Pass 1 runs the pods against `discord_core` as the pods' own pins resolve it (a
released tag, installed from each pyproject.toml), NOT this checkout. Pass 2 then
installs this checkout's `discord_core` editable and runs only `discord_core/tests`.
An editable install alongside the pins in one `pip install` is a ResolutionImpossible,
which is why core is installed between the passes and not in tox's `deps`. Pass 2's
coverage is appended to pass 1's, so full runs gate ONE combined total at 100%.

A narrowed run cannot gate the combined total (most of the tree did not run). It
gates each package whose SOURCE changed instead: every test that can execute that
package is in the run, so the number is what a full run would report for it. A
test-only edit is not gated, for the reason in `Selection`. The pylint and bandit
steps in tox.ini are static and are not narrowed.

Shared tox.yml discovers environments by `tox -l`, matching `py\\d+`, and runs exactly
`tox -e pyXY` -- so none of this can be a second tox environment; it is inside each.
'''
import os
import subprocess  # nosec B404 - fixed argv, no shell
import sys
from typing import Optional

from tests.cli._affected_tests import CORE_ROOT, ImportGraph, Selection, select
from tests.cli._git_diff import REPO_ROOT, base_head_parser, diff_paths, git_output

PIP_INSTALL_CORE = ['-m', 'pip', 'install', '-e', f'./{CORE_ROOT}']


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


def full_plan(python: str) -> list:
    '''The commands tox.ini carried before narrowing: the whole suite, one gated total.'''
    return [
        [python, '-m', 'pytest', f'--ignore={CORE_ROOT}/tests', '--cov', '--cov-report='],
        [python, *PIP_INSTALL_CORE],
        [python, '-m', 'pytest', f'{CORE_ROOT}/tests', '--cov', '--cov-append',
         '--cov-report=xml', '--cov-fail-under=100'],
    ]


def narrowed_plan(selection: Selection, python: str) -> list:
    '''Commands for only the selected packages. The central `tests/` tree always runs.'''
    pods = [package for package in selection.packages if package != CORE_ROOT]
    commands = [[python, '-m', 'pytest', 'tests', *(f'{pod}/tests' for pod in pods),
                 '--cov', '--cov-report=']]
    if CORE_ROOT in selection.packages:
        commands.append([python, *PIP_INSTALL_CORE])
        commands.append([python, '-m', 'pytest', f'{CORE_ROOT}/tests', '--cov', '--cov-append',
                         '--cov-report='])
    # The workflow uploads coverage.xml for diff-cover, so it must exist either way.
    commands.append([python, '-m', 'coverage', 'xml'])
    if selection.gated:
        include = ','.join(f'{REPO_ROOT / package}/*' for package in selection.gated)
        commands.append([python, '-m', 'coverage', 'report', f'--include={include}',
                         '--skip-covered', '--fail-under=100'])
    return commands


def plan(selection: Optional[Selection], python: str = sys.executable) -> list:
    '''The commands for a selection; None (or "everything") is the full plan.'''
    if selection is None or selection.everything:
        return full_plan(python)
    return narrowed_plan(selection, python)


def decide(base: Optional[str] = None, environ=None, head: str = 'HEAD') -> Optional[Selection]:
    '''The narrowed selection, or None when the full suite has to run (with the reason on stderr).'''
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
    print(f'run-tests: narrowed to {", ".join(selection.packages) or "the central tests only"} '
          f'(coverage gated on: {", ".join(selection.gated) or "nothing"}); {selection.reason}',
          file=sys.stderr)
    return selection


def main(argv=None) -> int:
    '''Decide, then run the plan, stopping at the first failing command.'''
    parser = base_head_parser(__doc__, base_required=False,
                              base_help='diff against this local ref instead of GITHUB_BASE_REF')
    parser.add_argument('--dry-run', action='store_true', help='print the commands instead of running them')
    args = parser.parse_args(argv)
    commands = plan(decide(args.base, head=args.head))
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
