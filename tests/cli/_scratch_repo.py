'''A throwaway git repo for the tests of the scripts that read a diff.'''
from tests.cli import _git_diff
from tests.cli._git_diff import git_output


def git(cwd, *args):
    '''Run `git <args>` in cwd, failing the test if it fails.'''
    git_output(*args, cwd=cwd)


def head_sha(cwd):
    '''The SHA at HEAD.'''
    return git_output('rev-parse', 'HEAD', cwd=cwd).stdout.strip()


def commit(cwd, message):
    '''Stage everything and commit it.'''
    git(cwd, 'add', '-A')
    git(cwd, 'commit', '-q', '-m', message)


def init_scratch_repo(root, monkeypatch):
    '''Make root an empty git repo and point the scripts' REPO_ROOT at it.'''
    monkeypatch.setattr(_git_diff, 'REPO_ROOT', root)
    git(root, 'init', '-q', '-b', 'main')
    git(root, 'config', 'user.email', 'test@example.com')
    git(root, 'config', 'user.name', 'Test')
