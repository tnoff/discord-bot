'''
`.dockerignore` decides what every image's build context holds, and a mistake in it fails in one of
two directions that look nothing alike: ignoring something a Dockerfile COPYs breaks the build
loudly, but leaving a package's tests/ in costs nothing visible -- it just rides into the builder
stage and invalidates that COPY layer on a test-only edit.

So both are checked here, against the real files, with a small evaluator of Docker's ignore rules
(see `is_ignored`). It implements only what this file uses -- per-segment globs, directory prefixes
and `!` re-includes -- and refuses `**`, so a pattern it cannot judge fails loudly instead of being
read wrongly.
'''
import fnmatch
import re

import pytest

from ci._affected_tests import PACKAGES
from ci._git_diff import REPO_ROOT

DOCKERFILES = sorted((REPO_ROOT / 'docker').glob('Dockerfile.*'))


def ignore_rules(text: str) -> list:
    '''(pattern, negated) for each rule in a .dockerignore, in order.'''
    rules = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        negated = line.startswith('!')
        pattern = line.lstrip('!').strip('/')
        if '**' in pattern:
            raise ValueError(f'{line!r}: ** is not supported by this evaluator')
        rules.append((pattern, negated))
    return rules


def _matches(pattern: str, path: str) -> bool:
    '''True when `pattern` matches `path` or one of its parent directories, segment by segment.'''
    pattern_parts, path_parts = pattern.split('/'), path.strip('/').split('/')
    return (len(pattern_parts) <= len(path_parts)
            and all(fnmatch.fnmatchcase(part, wanted) for part, wanted in zip(path_parts, pattern_parts)))


def is_ignored(path: str, rules: list) -> bool:
    '''Whether the rules exclude `path`: the last matching rule wins, and `!` re-includes.'''
    ignored = False
    for pattern, negated in rules:
        if _matches(pattern, path):
            ignored = not negated
    return ignored


@pytest.fixture(name='rules')
def rules_fixture():
    '''The repo's real rules.'''
    return ignore_rules((REPO_ROOT / '.dockerignore').read_text(encoding='utf-8'))


@pytest.mark.parametrize('path, expected', [
    ('tests', True), ('tests/helpers.py', True), ('conftest.py', True),
    ('discord_gateway/tests', True), ('discord_gateway/tests/cogs/test_music.py', True),
    ('discord_gateway', False), ('discord_gateway/cogs/music.py', False),
    # `*` stops at a slash and `tests` must be a whole segment.
    ('discord_gateway/cogs/tests', False), ('discord_gateway/testsuite.py', False),
    ('docker/Dockerfile.db', True), ('docker/entrypoint.sh', False),
    ('docs/music.md', True), ('README.md', False), ('CHANGELOG.md', True),
])
def test_is_ignored_against_the_real_rules(rules, path, expected):  # pylint: disable=redefined-outer-name
    '''The evaluator agrees with what Docker is documented to do with these rules.'''
    assert is_ignored(path, rules) is expected


def test_the_evaluator_refuses_what_it_cannot_judge():
    '''A rule it would misread must not be silently accepted.'''
    with pytest.raises(ValueError):
        ignore_rules('**/tests\n')


def test_every_package_tests_directory_is_ignored(rules):  # pylint: disable=redefined-outer-name
    '''Also true of a package added later, because the rule is a glob over `discord_*`.'''
    for package in PACKAGES:
        assert is_ignored(f'{package}/tests', rules), f'{package}/tests would ship in the build context'
        assert not is_ignored(f'{package}/pyproject.toml', rules), f'{package}/pyproject.toml is needed'
    assert is_ignored('tests', rules)


def _copy_sources(dockerfile) -> list:
    '''The context paths a Dockerfile COPYs (not `--from=` stages), without the destination.'''
    sources = []
    for line in dockerfile.read_text(encoding='utf-8').splitlines():
        match = re.match(r'^COPY\s+((?:--\S+\s+)*)(.+)$', line.strip())
        if match and '--from=' not in match.group(1):
            parts = re.findall(r'"[^"]*"|\S+', match.group(2))
            sources.extend(part.strip('"') for part in parts[:-1])
    return sources


def test_the_copy_parser_reads_a_real_dockerfile():
    '''The guard below is vacuous if the parser finds nothing.'''
    assert DOCKERFILES, 'no docker/Dockerfile.* found'
    sources = {source for dockerfile in DOCKERFILES for source in _copy_sources(dockerfile)}
    assert 'docker/entrypoint.sh' in sources
    assert any(source.endswith('/pyproject.toml') for source in sources)


@pytest.mark.parametrize('dockerfile', DOCKERFILES, ids=lambda path: path.name)
def test_nothing_a_dockerfile_copies_is_ignored(rules, dockerfile):  # pylint: disable=redefined-outer-name
    '''An over-broad rule here would break the build, but only on the image that needs the path.'''
    for source in _copy_sources(dockerfile):
        assert not is_ignored(source, rules), f'{dockerfile.name} COPYs {source}, which .dockerignore excludes'
