'''
Measures every `otel_span_wrapper(...)` / `async_otel_span_wrapper(...)` call
site in the tree via static AST resolution (`_otel_resolve.py`), and renders
the measured span content that replaces hand-maintained copies in
docs/monitoring/trace_linking.md.

**Why this exists.** The dispatcher's "Spans emitted" table in
trace_linking.md went stale twice in one session: first three span names
carried a leftover `_redis` suffix that no longer exists in the code, then a
second pass found four more real spans the table never listed at all. Both
were hand-transcription errors a measurement cannot make -- it reads the
actual `async_otel_span_wrapper(...)` call, not a memory of what the call
used to look like.

**What "exempt" means here.** A handful of span names are built from a
runtime value this repo has no way to know without executing the function --
a route name passed as a parameter, a Discord command's own qualified name
resolved from `ctx` inside `@command_wrapper`. These are reported, not
dropped: `measure()` always returns the exempt list alongside the resolved
one, and `render_trace_linking_doc()` renders both into the doc.
'''
import ast
from dataclasses import dataclass
from pathlib import Path

from tests.cli._otel_resolve import Corpus, pod_of, unparse
from tests.cli._roots import SERVICE_ROOTS, CORE_ROOT

REPO_ROOT = Path(__file__).resolve().parents[2]
TRACE_LINKING_DOC = REPO_ROOT / 'docs' / 'monitoring' / 'trace_linking.md'

_WRAPPER_NAMES = ('otel_span_wrapper', 'async_otel_span_wrapper')
_DEFAULT_KIND = 'INTERNAL'  # the wrapper functions' own default

#: Package root -> the short pod label the hand-written docs already use.
_POD_LABELS = {root: name for name, root in SERVICE_ROOTS.items()}
_POD_LABELS[CORE_ROOT] = 'core (shared)'

_DISPATCHER_PREFIXES = ('dispatch.', 'dispatch_client.', 'message_dispatcher.')

_BEGIN_DISPATCHER = (
    '<!-- BEGIN GENERATED(dispatcher-span-table) by tests/cli/test_span_census.py. '
    'Do not edit this block by hand.\n     '
    'Regenerate with: UPDATE_SPAN_CENSUS=1 pytest tests/cli/test_span_census.py -->'
)
_END_DISPATCHER = '<!-- END GENERATED(dispatcher-span-table) -->'

_BEGIN_EXEMPT = (
    '<!-- BEGIN GENERATED(exempt-span-appendix) by tests/cli/test_span_census.py. '
    'Do not edit this block by hand.\n     '
    'Regenerate with: UPDATE_SPAN_CENSUS=1 pytest tests/cli/test_span_census.py -->'
)
_END_EXEMPT = '<!-- END GENERATED(exempt-span-appendix) -->'


@dataclass(frozen=True)
class SpanEntry:
    '''One statically-resolved span name, fully attributed.'''
    name: str
    kind: str
    pod: str
    source: str  # 'relative/path.py:123'


@dataclass(frozen=True)
class ExemptSpan:
    '''One call site this census could not resolve, with why.'''
    source: str
    reason: str
    expression: str


class _SpanVisitor(ast.NodeVisitor):
    '''Finds every span-wrapper call, tracking the enclosing class (if any)
    so `self.X`/`cls.X` references can be resolved against it.
    '''

    def __init__(self):
        self.class_stack: list[str] = []
        self.hits: list[tuple] = []  # (ast.Call, enclosing class name or None)

    # ast.NodeVisitor dispatches by this exact method name.
    def visit_ClassDef(self, node):  # pylint: disable=invalid-name
        self.class_stack.append(node.name)
        self.generic_visit(node)
        self.class_stack.pop()

    def visit_Call(self, node):  # pylint: disable=invalid-name
        if isinstance(node.func, ast.Name) and node.func.id in _WRAPPER_NAMES:
            self.hits.append((node, self.class_stack[-1] if self.class_stack else None))
        self.generic_visit(node)


def _kind_of(call: ast.Call) -> str | None:
    '''`kind=SpanKind.X` or `kind=trace.SpanKind.X` -> `'X'`; omitted -> the
    wrapper's own INTERNAL default; any other shape -> unresolvable (None).
    '''
    kw = next((k.value for k in call.keywords if k.arg == 'kind'), None)
    if kw is None:
        return _DEFAULT_KIND
    if isinstance(kw, ast.Attribute):
        return kw.attr
    return None


def _name_expr(call: ast.Call):
    if call.args:
        return call.args[0]
    return next((k.value for k in call.keywords if k.arg == 'span_name'), None)


def measure(corpus: Corpus | None = None) -> tuple[list, list]:
    '''Every span-wrapper call site in the tree, resolved where possible.

    Returns `(resolved, exempt)` -- `resolved` is a `SpanEntry` per
    statically-determined span name (more than one per call site when a
    subclass fan-out applies), `exempt` is an `ExemptSpan` per call site
    that could not be resolved, in both cases covering the WHOLE tree, not
    just the dispatcher.
    '''
    corpus = corpus or Corpus()
    resolved: list[SpanEntry] = []
    exempt: list[ExemptSpan] = []
    for path, tree in corpus.trees.items():
        visitor = _SpanVisitor()
        visitor.visit(tree)
        for call, cls in visitor.hits:
            source = f'{path.relative_to(corpus.repo_root)}:{call.lineno}'
            name_expr = _name_expr(call)
            if name_expr is None:
                exempt.append(ExemptSpan(source, 'no resolvable name argument',
                                         unparse(call)))
                continue
            kind = _kind_of(call)
            if kind is None:
                exempt.append(ExemptSpan(source, 'unresolvable kind= argument',
                                         unparse(call)))
                continue
            results = corpus.resolve(name_expr, file=path, cls=cls)
            if not results:
                exempt.append(ExemptSpan(source, 'span name not statically determinable',
                                         unparse(name_expr)))
                continue
            for r in results:
                # `r.file` is the FAN-OUT attribution (the subclass that
                # supplies the override) and drives `pod` alone -- `source`
                # always points at the call site itself (`path:call.lineno`),
                # which is constant across every fanned-out row from this one
                # call and is where a reader would actually look.
                pod_root = pod_of(r.file, corpus.repo_root)
                resolved.append(SpanEntry(
                    name=r.value, kind=kind,
                    pod=_POD_LABELS.get(pod_root, pod_root),
                    source=source,
                ))
    return resolved, exempt


def _dispatcher_table(resolved: list) -> str:
    rows = sorted(
        {(e.name, e.kind, e.pod, e.source) for e in resolved
         if e.name.startswith(_DISPATCHER_PREFIXES)}
    )
    lines = ['| Span name | Kind | Pod | Source |', '|---|---|---|---|']
    for name, kind, pod, source in rows:
        lines.append(f'| `{name}` | {kind} | {pod} | `{source}` |')
    return '\n'.join(lines)


def _exempt_appendix(exempt: list) -> str:
    rows = sorted({(e.source, e.reason, e.expression) for e in exempt})
    lines = [
        'Every `otel_span_wrapper`/`async_otel_span_wrapper` call site this',
        "census can't reduce to a literal string, repo-wide -- not just the",
        'dispatcher. Each one is a genuine runtime value (a route name passed',
        'as a parameter, a Discord command name resolved from `ctx`), not a',
        'gap in the resolver: see `tests/cli/_otel_resolve.py` for exactly',
        'which constructs it does and does not follow.',
        '',
        '| Source | Why | Expression |',
        '|---|---|---|',
    ]
    for source, reason, expression in rows:
        lines.append(f'| `{source}` | {reason} | `{expression}` |')
    return '\n'.join(lines)


def render_trace_linking_doc(original: str, resolved: list, exempt: list) -> str:
    '''Replace the two generated blocks in `original`, leaving everything
    else -- the hand-written narrative -- untouched.
    '''

    def _replace(text: str, begin: str, end: str, body: str) -> str:
        if begin not in text or end not in text:
            raise AssertionError(
                f'Expected markers not found in trace_linking.md: {begin!r} / {end!r}. '
                'They were removed by hand -- restore them before regenerating.'
            )
        start = text.index(begin) + len(begin)
        stop = text.index(end, start)
        return text[:start] + '\n\n' + body + '\n\n' + text[stop:]

    text = _replace(original, _BEGIN_DISPATCHER, _END_DISPATCHER, _dispatcher_table(resolved))
    text = _replace(text, _BEGIN_EXEMPT, _END_EXEMPT, _exempt_appendix(exempt))
    return text
