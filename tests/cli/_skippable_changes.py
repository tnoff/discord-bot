'''
Decide which of ci.yml's jobs a set of changed files can skip.

Run by ci.yml's `metadata` job, which has no installed package and no
dependencies -- so everything here is standard library, the same shape as
_affected_images and _package_versions:

    python -m tests.cli._skippable_changes --base "origin/$BASE_REF"

It prints two GITHUB_OUTPUT lines on stdout (diagnostics go to stderr):

    only=true             every changed file is in METADATA_ONLY: nothing any job
                          inspects changed, so every job skips.
    tests_skippable=true  every changed file is METADATA_ONLY, documentation no test
                          reads, or a hand edit to one of the four docs a test
                          compares that leaves every GENERATED block untouched:
                          tox skips. It is a superset of `only`.

Only tox is gated on `tests_skippable`. The secret scan, the version and pin
checks and the workflow-contract check stay on for a documentation PR: they take
seconds, and documentation is exactly where somebody pastes an example token.
The image build is already conditional on which images a change reaches.

AN ALLOW-LIST, NOT A DENY-LIST
------------------------------
A path skips tox only if it is named here. Everything else, including a file
nobody has classified yet, runs the whole suite. Documentation is not safe to
skip wholesale because several docs are test inputs: each holds a block that is
regenerated from the code and compared byte for byte, so editing one by hand can
fail a test (docs/monitoring/metrics_reference.md failed that way on #1151 when a
code change shifted its line numbers). test_skippable_changes.py scans the test
sources for such reads, so a new one fails there instead of being skipped here.

HAND EDITS TO A COMPARED DOC
----------------------------
The four docs in DOCS_READ_BY_TESTS are not prose a test ignores; each has one test
that splices a regenerated body between paired `<!-- BEGIN GENERATED(...) -->` and
`<!-- END GENERATED(...) -->` markers and fails if the file differs. The result
therefore depends only on the text inside those markers, so a change that leaves every
block byte-identical (the prose around it, which is most edits to architecture.md)
cannot change it. That is decided by comparing the blocks at the merge base and at
head. A new or deleted doc, a doc with no block, or a changed marker runs the suite.
test_skippable_changes.py checks the premise against the real render of each doc.

NO RENAME DETECTION
-------------------
`git diff --name-only` reports only the new path of a rename, so moving
`discord_core/x.py` to `docs/x.md` would look like a docs-only change and hide the
deleted module. --no-renames reports both paths.

An empty diff yields false for both, which runs everything.
'''
import re
import sys
from typing import Callable, NamedTuple, Optional

from tests.cli._git_diff import base_head_parser, diff_paths, git_output, git_show, report_changed


# Changes that no job inspects, named exactly and anchored. Not a prefix, not a
# glob. catalog-info.yaml used to be here, but tests/test_catalog_apis.py reads it
# (it checks the API wiring against the measured seams), so a catalog-only PR skipped
# the test that would have caught a bad edit. test_no_test_reads_a_file_that_skips_tox
# now covers this set as well as the documentation one.
METADATA_ONLY = frozenset({'renovate.json'})

# Documentation a test reads. These are inputs, not prose.
DOCS_READ_BY_TESTS = frozenset({
    'docs/architecture.md',                    # tests/cli/_seam_topology.py
    'docs/configuration.md',                   # tests/cli/_config_reference.py
    'docs/monitoring/metrics_reference.md',    # tests/cli/_metric_census.py
    'docs/monitoring/trace_linking.md',        # tests/cli/_span_census.py
})
# Whole directories a test reads: docs/openapi/*.yaml is generated and compared.
DOC_DIRS_READ_BY_TESTS = ('docs/openapi/',)

# Repo-root prose that no test reads.
ROOT_DOCS = frozenset({
    'AGENTS.md', 'CHANGELOG.md', 'CONTRIBUTING.md', 'DEVELOPMENT.md', 'LICENSE', 'README.md',
})


# A generated block, markers included. The BEGIN marker's comment can run over more
# than one line, so match across newlines up to the first END marker.
_GENERATED_BLOCK = re.compile(r'<!-- BEGIN GENERATED\(.*?<!-- END GENERATED\([^)]*\) -->', re.DOTALL)


class Skippable(NamedTuple):
    '''What a change set lets CI skip.'''
    everything: bool
    tests: bool


def is_documentation(path: str) -> bool:
    '''True for prose that no test reads, so editing it cannot change a test result.'''
    if path in ROOT_DOCS:
        return True
    if not path.startswith('docs/'):
        return False
    return path not in DOCS_READ_BY_TESTS and not path.startswith(DOC_DIRS_READ_BY_TESTS)


def edits_outside_generated_blocks(path: str, before: Optional[str], after: Optional[str]) -> bool:
    '''True when a compared doc changed but every GENERATED block in it did not.

    False for a doc that is not one of DOCS_READ_BY_TESTS, for one that is new or gone
    (None on either side), and for one with no block at all, since then the test has
    nothing to compare and the premise does not hold.
    '''
    if path not in DOCS_READ_BY_TESTS or before is None or after is None:
        return False
    blocks = _GENERATED_BLOCK.findall(before)
    return bool(blocks) and blocks == _GENERATED_BLOCK.findall(after)


def classify(changed_files, prose_only: Optional[Callable[[str], bool]] = None) -> Skippable:
    '''Map the changed paths onto the jobs they let CI skip.

    prose_only(path) says whether a changed DOCS_READ_BY_TESTS doc was edited outside its
    generated blocks; it defaults to "no", which is the safe answer.
    '''
    prose_only = prose_only or (lambda _path: False)
    changed = [path for path in changed_files if path]
    if not changed:
        return Skippable(everything=False, tests=False)
    everything = all(path in METADATA_ONLY for path in changed)
    tests = all(path in METADATA_ONLY or is_documentation(path)
                or (path in DOCS_READ_BY_TESTS and prose_only(path))
                for path in changed)
    return Skippable(everything=everything, tests=tests)


def main(argv=None) -> int:
    '''Print the `only` and `tests_skippable` outputs for the diff base...head.'''
    args = base_head_parser(__doc__).parse_args(argv)

    changed = diff_paths(args.base, args.head, '--no-renames')
    report_changed(changed)

    merge_base = git_output('merge-base', args.base, args.head).stdout.strip()
    skippable = classify(changed, prose_only=lambda path: edits_outside_generated_blocks(
        path, git_show(merge_base, path), git_show(args.head, path)))
    print(f'metadata-only: {skippable.everything}', file=sys.stderr)
    print(f'tests skippable: {skippable.tests}', file=sys.stderr)
    print(f'only={str(skippable.everything).lower()}')
    print(f'tests_skippable={str(skippable.tests).lower()}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
