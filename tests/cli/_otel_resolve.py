'''
Shared static-resolution engine for the span/metric census generators.

`tests/cli/_span_census.py` and `tests/cli/_metric_census.py` both need the
same thing: given the AST expression passed as an argument to a call site
(`otel_span_wrapper(NAME, ...)`, `create_observable_gauge(_, NAME, _, DESC,
unit=UNIT)`), resolve it to a literal string if the source makes that
possible without running the process, and say exactly which construct
defeated resolution when it does not. This module is the resolver; it knows
nothing about spans or metrics specifically.

**Why static (AST), not live introspection.** `_seam_topology.py` measures by
importing each entrypoint and reading `SEAM`/`ROUTES_CALLED` off the live
class object -- that works because those are class-level declarations.
A span/metric name is usually built inside a function BODY (an argument
expression to a call), which live introspection cannot see without actually
invoking the function. AST parsing of the source is the only static option
left, so this module parses rather than imports.

Five resolution rules, tried against an argument expression:

1. A plain string literal (`ast.Constant`).
2. A bare name bound by a module-level assignment in the SAME file
   (`SEARCH_SPAN_NAME = 'music.search_youtube_music'`).
3. `EnumClass.MEMBER.value`, where `EnumClass` is defined anywhere in the
   scanned tree as `class EnumClass(Enum): MEMBER = '...'`. Resolved
   repo-wide because nothing requires the enum to live in the same file as
   every caller (`MetricNaming` lives in `discord_core`, every pod uses it).
4. `self.X` / `cls.X`, where some class anywhere in the tree subclasses the
   enclosing class and itself resolves `X` (by these same rules,
   recursively) -- **subclass fan-out**, attributed to the SUBCLASS's own
   file rather than the call site's file, because that is where the real
   pod boundary is (`HttpDownloadClient`/`HttpYoutubeMusicSearchClient`
   both override `SPAN_PREFIX` on their shared base
   `HttpQueueWorkerClient`). Tried before rule 5, even when the enclosing
   class has its own value for `X` -- a base class's own default is a
   "please override me" placeholder in every case this repo has today
   (checked against the tree: none of these base classes are ever
   instantiated directly, so their own default is never what actually gets
   emitted).
5. `self.X` / `cls.X`, where `X` is assigned a literal directly in the
   enclosing class's own body and no subclass overrides it.

An f-string (`ast.JoinedStr`) resolves by resolving each formatted value by
these same rules and concatenating with the literal text between them; a
fan-out in any one component fans out the whole string, paired correctly
(each subclass contributes one complete resolved string, not a cross
product against other components).

Anything else -- a function call, a parameter reference, a loop variable, an
f-string component that doesn't resolve -- is UNRESOLVED. Unresolved is
returned as `None` from `resolve()`, which both callers render into an
explicit "not statically determinable" bucket rather than dropping silently.
'''
import ast
import itertools
from dataclasses import dataclass, field
from pathlib import Path

from tests.cli._roots import source_files

REPO_ROOT = Path(__file__).resolve().parents[2]

# Guard against a resolution cycle (two classes subclassing each other, or a
# constant referencing itself) turning into infinite recursion. No real chain
# in this repo is anywhere near this deep; it exists so a future cycle fails
# loudly as "unresolved" instead of hanging the test suite.
_MAX_DEPTH = 25


@dataclass
class ClassInfo:
    '''One `class Foo(Bar, Baz): ...` seen while scanning.'''
    file: Path
    bases: list = field(default_factory=list)
    #: attr name -> its assigned value AST node, or None if only annotated
    #: (`X: ClassVar[str]`) with no default.
    attrs: dict = field(default_factory=dict)


@dataclass
class Resolved:
    '''One concrete, ground-truth resolution of an expression.'''
    value: str
    #: The file responsible for this specific value -- the call site's own
    #: file, unless a subclass fan-out moved attribution to the subclass
    #: that supplies the override.
    file: Path


class Corpus:
    '''Parses every shipped `.py` file once and indexes the declarations that
    `resolve()` needs: module-level constants (per file), enum members
    (global -- an enum name is assumed unique repo-wide), and classes (global,
    keyed by bare name, with their bases and their own literal/annotated
    attributes).
    '''

    def __init__(self, repo_root: Path = REPO_ROOT):
        self.repo_root = repo_root
        self.files: list[Path] = source_files(repo_root)
        self.trees: dict[Path, ast.Module] = {}
        self.module_consts: dict[Path, dict] = {}
        self.enum_members: dict[str, dict] = {}
        self.classes: dict[str, ClassInfo] = {}
        #: base class bare name -> list of direct-subclass bare names.
        self.subclasses_of: dict[str, list] = {}
        #: classes sharing a bare name across files -- resolution through
        #: such a name is refused rather than guessed at.
        self._ambiguous_classes: set = set()
        self._scan()

    def _scan(self):
        for path in self.files:
            try:
                tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
            except SyntaxError as exc:
                raise AssertionError(f'{path} failed to parse: {exc}') from exc
            self.trees[path] = tree
            consts = {}
            for node in tree.body:
                self._index_module_assign(node, consts)
                if isinstance(node, ast.ClassDef):
                    self._index_class(node, path)
            self.module_consts[path] = consts
        for name, info in self.classes.items():
            for base in info.bases:
                self.subclasses_of.setdefault(base, []).append(name)

    @staticmethod
    def _index_module_assign(node, consts: dict):
        '''Record `NAME = <expr>` at module level. Tuple/multi-target unpacking
        is deliberately not supported -- nothing in this tree defines a span
        or metric name that way, and guessing which target got which value is
        exactly the kind of silent wrong answer this module exists to avoid.
        '''
        if isinstance(node, ast.Assign) and len(node.targets) == 1 \
                and isinstance(node.targets[0], ast.Name):
            consts[node.targets[0].id] = node.value
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) \
                and node.value is not None:
            consts[node.target.id] = node.value

    def _index_class(self, node: ast.ClassDef, path: Path):
        bases = [b.id for b in node.bases if isinstance(b, ast.Name)]
        is_enum = any(
            (isinstance(b, ast.Name) and b.id == 'Enum')
            or (isinstance(b, ast.Attribute) and b.attr == 'Enum')
            for b in node.bases
        )
        attrs = {}
        for item in node.body:
            if isinstance(item, ast.Assign) and len(item.targets) == 1 \
                    and isinstance(item.targets[0], ast.Name):
                attrs[item.targets[0].id] = item.value
            elif isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
                attrs[item.target.id] = item.value  # None when annotation-only
        if is_enum:
            members = {}
            for item in node.body:
                if isinstance(item, ast.Assign) and len(item.targets) == 1 \
                        and isinstance(item.targets[0], ast.Name) \
                        and isinstance(item.value, ast.Constant) \
                        and isinstance(item.value.value, str):
                    members[item.targets[0].id] = item.value.value
            # An enum redefining a name already seen elsewhere is a repo bug
            # this generator should surface, not silently pick one of.
            assert node.name not in self.enum_members, (
                f'Enum class name {node.name!r} is not unique repo-wide '
                f'({path} redefines it) -- resolver assumes uniqueness.'
            )
            self.enum_members[node.name] = members
        if node.name in self.classes:
            self._ambiguous_classes.add(node.name)
        else:
            self.classes[node.name] = ClassInfo(file=path, bases=bases, attrs=attrs)

    # -- resolution -----------------------------------------------------

    def resolve(self, expr: ast.expr, *, file: Path, cls: str | None,
               _depth: int = 0) -> list[Resolved] | None:
        '''Resolve one argument expression. Returns a list of `Resolved`
        (more than one only when a subclass fan-out happened), or `None` if
        no rule applies.
        '''
        if _depth > _MAX_DEPTH:
            return None

        if isinstance(expr, ast.Constant) and isinstance(expr.value, str):
            return [Resolved(expr.value, file)]

        if isinstance(expr, ast.JoinedStr):
            return self._resolve_joined_str(expr, file=file, cls=cls, _depth=_depth)

        if isinstance(expr, ast.Name):
            const = self.module_consts.get(file, {}).get(expr.id)
            return (self.resolve(const, file=file, cls=cls, _depth=_depth + 1)
                    if const is not None else None)

        if isinstance(expr, ast.Attribute):
            return self._resolve_attribute(expr, file=file, cls=cls, _depth=_depth)

        return None

    def _resolve_attribute(self, expr: ast.Attribute, *, file, cls, _depth):
        value, attr = expr.value, expr.attr

        # Enum.MEMBER.value
        if attr == 'value' and isinstance(value, ast.Attribute) \
                and isinstance(value.value, ast.Name):
            enum_name, member = value.value.id, value.attr
            members = self.enum_members.get(enum_name)
            if members is not None and member in members:
                return [Resolved(members[member], file)]
            return None

        # self.X / cls.X
        if isinstance(value, ast.Name) and value.id in ('self', 'cls') and cls is not None:
            return self._resolve_class_attr(cls, attr, _depth=_depth)

        return None

    def _resolve_class_attr(self, cls: str, attr: str, *, _depth) -> list[Resolved] | None:
        if cls in self._ambiguous_classes:
            return None
        info = self.classes.get(cls)
        if info is None:
            return None
        # Subclass overrides win even when this class has its own literal
        # value: a base class that declares a "please override me" default
        # (an empty string, a generic placeholder) is never instantiated
        # directly in this repo -- only concrete subclasses are -- so the
        # base's own value is never what actually gets emitted. Checked
        # against the tree, not assumed: see the module docstring.
        resolved: list[Resolved] = []
        for sub in self.subclasses_of.get(cls, []):
            sub_result = self._resolve_class_attr(sub, attr, _depth=_depth + 1)
            if sub_result:
                resolved.extend(sub_result)
        if resolved:
            return resolved
        own_value = info.attrs.get(attr)
        if attr in info.attrs and own_value is not None:
            return self.resolve(own_value, file=info.file, cls=cls, _depth=_depth + 1)
        return None

    def _resolve_joined_str(self, expr: ast.JoinedStr, *, file, cls, _depth):
        # Each component resolves independently; a fan-out in one component
        # must multiply the OTHERS as literal text, not cross-join against
        # an unrelated fan-out elsewhere (none of the call sites in this repo
        # have two dynamic components in one f-string, so a simple sequential
        # fold covers every real case -- a second fan-out would raise here via
        # the itertools.product below rather than silently mis-pairing).
        per_component: list[list[str]] = []
        attributions: list[Path | None] = []
        for part in expr.values:
            if isinstance(part, ast.Constant):
                per_component.append([part.value])
                attributions.append(None)
            elif isinstance(part, ast.FormattedValue):
                sub = self.resolve(part.value, file=file, cls=cls, _depth=_depth + 1)
                if not sub:
                    return None
                per_component.append([r.value for r in sub])
                # All Resolved in `sub` share whichever file(s) fan-out
                # produced; take them as the candidate attribution set.
                attributions.append([r.file for r in sub])
            else:
                return None
        combos = list(itertools.product(*per_component))
        # Build attribution per combo: the first component with a real
        # per-value file list drives it (there is at most one fan-out
        # component in every call site this repo has today).
        fanout_files = next((a for a in attributions if a is not None), None)
        results = []
        for i, combo in enumerate(combos):
            text = ''.join(combo)
            file_for_combo = fanout_files[i] if fanout_files else file
            results.append(Resolved(text, file_for_combo))
        return results


def pod_of(path: Path, repo_root: Path = REPO_ROOT) -> str:
    '''The top-level package a file ships under -- `discord_gateway`,
    `discord_core`, etc. Criterion 8 is the only spelling left on disk, so
    this is just the first path component; no need for `_roots.home_of`'s
    dual-spelling handling here.
    '''
    return path.relative_to(repo_root).parts[0]


def unparse(expr: ast.expr) -> str:
    '''Best-effort source text for an unresolved expression, for display in
    the "not statically determinable" bucket.
    '''
    try:
        return ast.unparse(expr)
    except Exception:  # pylint: disable=broad-except
        return '<unparsable>'
