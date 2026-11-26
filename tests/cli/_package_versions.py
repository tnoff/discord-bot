'''
Enforce that a PR bumps the VERSION of every package it touches.

Criterion 8 step 6 gave each of the seven packages (`discord_core` plus the
six pods) its own real `VERSION` file -- see `_roots.PACKAGE_ROOTS`. There is
no repo-wide `VERSION` any more; these are the only version numbers. This
module is the one place that decides which packages a diff touches, and it
serves two callers:

  * the check (default): fail a PR that changed a package without bumping that
    package's own `VERSION`, run by ci.yml's "Detect package version bumps" job;
  * the list (`--list`): print the `VERSION` file of every touched package, one
    per line, run by ci.yml's `bump-version` job as the shared workflow's
    `version_files_command` so Renovate's dev- PRs get exactly the bumps the
    check then demands. Because both read `touched_packages`, the bump and the
    check cannot disagree about what "touched" means;
  * the full list (`--list-all`): every package's `VERSION`, touched or not,
    needing no diff -- release.yml hands it to `assemble-changelog` to fold each
    package's own `changelog.d/` into its own `CHANGELOG.md`.

Neither has an installed package or dependencies to lean on -- so, like
`_affected_images.py`, everything here is standard library and nothing imports
`_image_deps` (which shells out to a real interpreter per entrypoint):

    python3 -m tests.cli._package_versions --base "$BASE_SHA"
    python3 -m tests.cli._package_versions --base "$BASE_SHA" --list
    python3 -m tests.cli._package_versions --list-all

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

A PACKAGE'S OWN tests/ DOES NOT COUNT
-------------------------------------
A path under `<package>/tests/` is not a change to the package. VERSION is what
release.yml tags and what the pods' `core-v...` pins and image tags follow, and
test code ships in no image and no distribution (each pyproject.toml's
`packages.find` excludes it, and `_affected_images.module_for` carves it out for
the same reason). Requiring a bump for it only cut empty patch releases. A PR
that changes a package's source AND its tests still bumps, because of the source.

This used to be the opposite, on the grounds that "touching a package at all" was
the simplest rule. It was simple, and it meant a test-only refactor (the helpers
split in discord-bot#1253) forced four packages through a release. Only the nested
`tests/` directory is exempt: a sibling such as `discord_x/testing_utils.py`, or
a `tests` file that is not under a package root, is unaffected by this.
'''
import sys

from tests.cli._git_diff import base_head_parser, diff_paths, report_changed
from tests.cli._roots import CORE_ROOT, SERVICE_ROOTS, TESTS_DIR, root_of


#: Every package root this convention covers, keyed the same way a tag-name
#: table keys its short names -- not hand-rolled a second time, imported from
#: the one place `_roots.py` already declares them.
PACKAGE_ROOTS = {'core': CORE_ROOT, **SERVICE_ROOTS}


def touched_packages(changed_files: list) -> set:
    '''The distinct package roots (`discord_core`, `discord_search`, ...) any
    of `changed_files` sits under, other than inside the package's own `tests/`.

    Uses `root_of`'s direct-ownership mapping -- a repo-relative path to the
    one root it sits under -- not the dependency-closure fanout
    `_affected_images` reads from the measured image graph. Those answer
    different questions: that one is "which images must rebuild", this one is
    "whose version number did this PR's own diff change".
    '''
    return {root for path in changed_files
            if (root := root_of(path)) is not None and path.split('/')[1] != TESTS_DIR}


def main(argv=None) -> int:
    '''Fail if any touched package's own VERSION was not also touched, or with
    `--list` / `--list-all` print VERSION paths instead.'''
    parser = base_head_parser(__doc__, base_required=False,
                              base_help='base ref to diff against (required except with --list-all)')
    parser.add_argument('--list', action='store_true',
                        help='print each touched package\'s VERSION path to stdout and exit 0')
    parser.add_argument('--list-all', action='store_true',
                        help='print every package\'s VERSION path, touched or not, and exit 0')
    args = parser.parse_args(argv)

    if args.list_all:
        for root in sorted(PACKAGE_ROOTS.values()):
            print(f'{root}/VERSION')
        return 0
    if not args.base:
        parser.error('--base is required unless --list-all is given')

    changed = diff_paths(args.base, args.head)
    report_changed(changed)

    if args.list:
        # stdout carries only the paths; the changed-files diagnostics above went
        # to stderr, so a caller can capture this cleanly.
        for root in sorted(touched_packages(changed)):
            print(f'{root}/VERSION')
        return 0

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
            '\ntouches. (A change confined to its own tests/ does not count.)',
            file=sys.stderr)
        return 1

    print('\nevery touched package bumped its own VERSION', file=sys.stderr)
    return 0


if __name__ == '__main__':
    sys.exit(main())
