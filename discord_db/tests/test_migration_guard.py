'''
CI guard: a new Alembic revision may not break the previous release.

The db pod rolls with the old pod still serving while the new one runs
`alembic upgrade head`, so for a few seconds the old code runs against the new
schema. Additive changes (new table, new nullable column, index) are safe. A
drop, a rename, or an alter_column (type, nullability, rename) on something the
old code still reads or writes is not.

The rule is expand and contract: release N adds the new schema and uses it,
release N+1, once N has fully rolled, removes the old one. A revision that does
the removing is a contract step and says so with a module-level

    CONTRACT_STEP = 'why this is safe once the previous release has rolled'

For the release that ships one, set discord-db's strategy back to Recreate.
Only `upgrade` is checked: a downgrade runs by hand, with the pod stopped.
'''
import ast
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
VERSIONS = REPO_ROOT / 'alembic' / 'versions'

DESTRUCTIVE_CALLS = frozenset({'drop_column', 'drop_table', 'alter_column', 'rename_table'})

# Every revision that existed when the guard was added. They shipped under
# Recreate, so their drops are not contract steps to re-review. A new revision
# never belongs here.
PREDATES_GUARD = frozenset({
    '0a3945d3e28b', '0f696315a882', '491790be3ce3', '4c68f4df56dd',
    '5b627533b326', '86418d6f1ce3', 'a399ba3deb56', 'bf20d91d337c',
    'bf24a336d155', 'c3f1a7d20b45', 'd1c8b4e6f7a2', 'faa6198cbed2',
})


def destructive_calls(source: str) -> list[str]:
    '''Names of destructive operations called anywhere outside `downgrade`.

    The whole module is walked, not just `upgrade`, so a helper that `upgrade`
    calls cannot hide one. Matches `op.x`, `batch_op.x` and any other receiver.
    '''
    found = []

    class _Visitor(ast.NodeVisitor):
        def visit_FunctionDef(self, node):  # pylint: disable=invalid-name
            if node.name != 'downgrade':
                self.generic_visit(node)

        def visit_Call(self, node):  # pylint: disable=invalid-name
            if isinstance(node.func, ast.Attribute) and node.func.attr in DESTRUCTIVE_CALLS:
                found.append(node.func.attr)
            self.generic_visit(node)

    _Visitor().visit(ast.parse(source))
    return found


def contract_step_reason(source: str) -> str:
    '''The module-level CONTRACT_STEP string, or '' if absent, empty or not a string.'''
    for node in ast.parse(source).body:
        if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == 'CONTRACT_STEP' for t in node.targets):
            value = node.value
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                return value.value.strip()
    return ''


def revision_id(source: str) -> str:
    '''The module-level `revision` identifier.'''
    for node in ast.parse(source).body:
        if isinstance(node, ast.AnnAssign) and getattr(node.target, 'id', None) == 'revision':
            return node.value.value
    raise AssertionError('revision file has no module-level `revision`')


def test_new_revisions_are_additive_or_marked_as_contract_steps():
    '''Fails a new revision that drops or alters schema without CONTRACT_STEP.'''
    offenders = {}
    for path in sorted(VERSIONS.glob('*.py')):
        source = path.read_text(encoding='utf-8')
        if revision_id(source) in PREDATES_GUARD:
            continue
        calls = destructive_calls(source)
        if calls and not contract_step_reason(source):
            offenders[path.name] = sorted(set(calls))
    assert not offenders, (
        f'{offenders} drop or alter schema the previous release may still use. '
        'Split it into expand and contract (see discord_db/tests/test_migration_guard.py), '
        'or, if this is the contract step, add CONTRACT_STEP = "<why it is safe>".'
    )


def test_predates_guard_lists_only_real_revisions():
    '''A typo here would silently exempt nothing, or leave a stale id behind.'''
    present = {revision_id(p.read_text(encoding='utf-8')) for p in VERSIONS.glob('*.py')}
    assert PREDATES_GUARD <= present


@pytest.mark.parametrize('body, expected', [
    ('op.add_column("t", sa.Column("c", sa.Integer(), nullable=True))', []),
    ('op.create_table("t")\n    op.create_index("i", "t", ["c"])', []),
    ('op.drop_column("t", "c")', ['drop_column']),
    ('op.drop_table("t")', ['drop_table']),
    ('op.alter_column("t", "c", nullable=False)', ['alter_column']),
    ('op.rename_table("a", "b")', ['rename_table']),
    ('with op.batch_alter_table("t") as b:\n        b.alter_column("c", new_column_name="d")',
     ['alter_column']),
])
def test_destructive_calls_finds_upgrade_operations(body, expected):
    source = f'def upgrade():\n    {body}\n'
    assert destructive_calls(source) == expected


def test_destructive_calls_ignores_downgrade():
    '''The downgrade of an additive revision drops what upgrade added.'''
    source = 'def upgrade():\n    op.create_table("t")\n\ndef downgrade():\n    op.drop_table("t")\n'
    assert not destructive_calls(source)


def test_destructive_calls_sees_through_a_helper():
    source = 'def _shrink():\n    op.drop_column("t", "c")\n\ndef upgrade():\n    _shrink()\n'
    assert destructive_calls(source) == ['drop_column']


@pytest.mark.parametrize('source, expected', [
    ('CONTRACT_STEP = "old column unused since db 3.2.0"\n', 'old column unused since db 3.2.0'),
    ('CONTRACT_STEP = ""\n', ''),
    ('CONTRACT_STEP = True\n', ''),
    ('CONTRACT_STEP = f"{x}"\n', ''),
    ('OTHER = "x"\n', ''),
    ('x = 1\n', ''),
])
def test_contract_step_reason(source, expected):
    '''Only a non-empty string literal counts, so a bare `= True` is not a reason.'''
    assert contract_step_reason(source) == expected


def test_revision_id_requires_a_revision():
    assert revision_id("revision: str = 'abc'\n") == 'abc'
    with pytest.raises(AssertionError):
        revision_id('x = 1\n')
