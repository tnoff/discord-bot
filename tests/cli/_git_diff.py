'''
What git says changed between two refs: the shared front end of the CI scripts keyed on a diff.

`_affected_images`, `_package_versions` and `_skippable_changes` all take `--base` / `--head`,
list the files that differ and say so on stderr. This imports nothing but the standard library,
because `_affected_images` runs in ci.yml's `changes` job, which has no installed package.
'''
import argparse
import subprocess  # nosec B404 - fixed argv, no shell, reads git metadata only
import sys
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parents[2]


def base_head_parser(description: Optional[str], *, base_required: bool = True,
                     base_help: str = 'base ref to diff against') -> argparse.ArgumentParser:
    '''A parser with the `--base` and `--head` refs every diff-keyed script takes.'''
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--base', required=base_required, help=base_help)
    parser.add_argument('--head', default='HEAD', help='head ref (default HEAD)')
    return parser


def git_output(*args: str, cwd: Optional[Path] = None, check: bool = True) -> subprocess.CompletedProcess:
    '''Run `git <args>` in cwd (default REPO_ROOT, read at call time), capturing text output.'''
    return subprocess.run(  # nosec B603 B607 - fixed argv
        ['git', *args], cwd=cwd or REPO_ROOT, capture_output=True, text=True, check=check)


def diff_paths(base: str, head: str, *extra: str) -> list[str]:
    '''Paths that differ between base...head; `extra` are further `git diff` flags.'''
    result = git_output('diff', '--name-only', *extra, f'{base}...{head}')
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def report_changed(changed: list[str]) -> None:
    '''List the changed files on stderr, so stdout stays clean for a caller to capture.'''
    print('changed files:', file=sys.stderr)
    for path in changed:
        print(f'  {path}', file=sys.stderr)


def git_show(ref: str, path: str, cwd: Optional[Path] = None) -> Optional[str]:
    '''path's content at ref, or None if it does not exist there.'''
    result = git_output('show', f'{ref}:{path}', cwd=cwd, check=False)
    return result.stdout if result.returncode == 0 else None
