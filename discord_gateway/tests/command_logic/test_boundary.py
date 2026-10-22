'''
command_logic and loop_logic must run without discord.py (#1030). The gateway cogs depend on them, never
the other way round, so a discord import or an import of ``discord_gateway.cogs`` here
means logic has leaked back into the Discord-facing layer.
'''
import ast
from pathlib import Path

import pytest

from discord_gateway import command_logic, loop_logic

PACKAGE_DIRS = [Path(command_logic.__file__).parent, Path(loop_logic.__file__).parent]
FORBIDDEN = ('discord', 'discord_gateway.cogs')


def _imported_modules(path: Path) -> list[str]:
    modules = []
    for node in ast.walk(ast.parse(path.read_text(encoding='utf-8'))):
        if isinstance(node, ast.Import):
            modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            modules.append(node.module or '')
    return modules


def _is_forbidden(module: str) -> bool:
    return any(module == name or module.startswith(f'{name}.') for name in FORBIDDEN)


@pytest.mark.parametrize('path', sorted(path for package in PACKAGE_DIRS for path in package.glob('*.py')),
                         ids=lambda p: f'{p.parent.name}/{p.name}')
def test_command_logic_has_no_discord_imports(path):
    offenders = [m for m in _imported_modules(path) if _is_forbidden(m)]
    assert not offenders, f'{path.parent.name}/{path.name} imports {offenders}; it must stay discord-free'


def test_is_forbidden_matches_submodules_not_prefixes():
    assert _is_forbidden('discord')
    assert _is_forbidden('discord.ext.commands')
    assert _is_forbidden('discord_gateway.cogs.common')
    # discord_core is not discord.py
    assert not _is_forbidden('discord_core.utils.common')
