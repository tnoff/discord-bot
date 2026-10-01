'''
Enforce that a PR bumps the VERSION of every package it touches.

Criterion 8 step 6 gave each of the seven packages (`discord_core` plus the
six pods) its own real `VERSION` file -- see `_roots.PACKAGE_ROOTS` -- in
place of the step-5-era symlink to the shared root `VERSION`. Nothing
automates the bump itself yet: like the shared root `VERSION` before it, each
package's own file is still hand-edited as part of the PR that changes that
package, the same way `git log` shows the root one always has been. This
module is the enforcement half of that convention -- it does not write a
bump, it only checks one was not forgotten.

Run by ci.yml's "Detect package version bumps" job, which has no installed
package and no dependencies -- so, like `_affected_images.py`, everything
here is standard library and nothing imports `_image_deps` (which shells out
to a real interpreter per entrypoint):

    python3 -m tests.cli._package_versions --base "$BASE_SHA"

NO DEPENDENCY-PROPAGATION RULE
-------------------------------
A PR touching only `discord_search/*` must bump `discord_search/VERSION`; one
touching both `discord_core/*` and `discord_search/*` must bump both. A
`discord_core`-only change does NOT force every pod's VERSION to bump too --
every pod installs `discord_core` from the live checkout path rather than a
pinned tarball, so a core-only change has nothing downstream to re-pin yet.
That changes once a later step moves release Dockerfiles onto pinned, tagged
installs; until then a propagation rule would only invent bumps nothing
downstream can observe.

NO EXEMPTIONS
-------------
Touching a package's own nested `tests/` counts as touching the package --
the simplest rule available, and the one `_affected_images.module_for` had to
learn to carve back OUT for a different purpose (test code ships in no
image). This module asks a different question ("did this PR touch the
package at all") and test code answers it the same as production code does.
'''
import argparse
import subprocess  # nosec B404 - fixed argv, no shell, reads git metadata only
import sys
from pathlib import Path

from tests.cli._roots import CORE_ROOT, SERVICE_ROOTS, root_of

REPO_ROOT = Path(__file__).resolve().parents[2]

#: Every package root this convention covers, keyed the same way a tag-name
#: table keys its short names -- not hand-rolled a second time, imported from
#: the one place `_roots.py` already declares them.
PACKAGE_ROOTS = {'core': CORE_ROOT, **SERVICE_ROOTS}


def touched_packages(changed_files: list) -> set:
    '''The distinct package roots (`discord_core`, `discord_search`, ...) any
    of `changed_files` sits under.

    Uses `root_of`'s direct-ownership mapping -- a repo-relative path to the
    one root it sits under -- not the dependency-closure fanout
    `_affected_images` reads from the measured image graph. Those answer
    different questions: that one is "which images must rebuild", this one is
    "whose version number did this PR's own diff change".
    '''
    return {root for path in changed_files if (root := root_of(path)) is not None}


def main(argv=None) -> int:
    '''Fail if any touched package's own VERSION was not also touched.'''
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base', required=True, help='base ref to diff against')
    parser.add_argument('--head', default='HEAD', help='head ref (default HEAD)')
    args = parser.parse_args(argv)

    result = subprocess.run(  # nosec B603 B607 - fixed argv
        ['git', 'diff', '--name-only', f'{args.base}...{args.head}'],
        cwd=REPO_ROOT, capture_output=True, text=True, check=True)
    changed = [line.strip() for line in result.stdout.splitlines() if line.strip()]

    print('changed files:', file=sys.stderr)
    for path in changed:
        print(f'  {path}', file=sys.stderr)

    changed_set = set(changed)
    missing = sorted(root for root in touched_packages(changed)
                      if f'{root}/VERSION' not in changed_set)

    if missing:
        print('\nERROR: these packages changed without bumping their own VERSION:',
              file=sys.stderr)
        for root in missing:
            print(f'  {root} (expected {root}/VERSION in this diff)', file=sys.stderr)
        print(
            '\nEach of the seven packages now tracks its own version independently --'
            '\nsee DEVELOPMENT.md. Bump the VERSION file for every package this PR'
            '\ntouches, including a nested tests/ change under it.',
            file=sys.stderr)
        return 1

    print('\nevery touched package bumped its own VERSION', file=sys.stderr)
    return 0


if __name__ == '__main__':
    sys.exit(main())
