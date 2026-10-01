'''
Measures every `create_observable_gauge(...)` call site in the tree via
static AST resolution (`_otel_resolve.py`), and renders the measured metric
census that backs docs/monitoring/metrics_reference.md's per-metric claims
with a live cross-check instead of a one-off grep.

**Why this exists.** `message_dispatcher_queue_depth`'s "Planned — not
currently emitted" status and the `heartbeat` metric's `background_job`
table were both corrected by hand in an earlier pass, against a manual grep.
A grep is itself a one-off check: nothing re-runs it the next time someone
adds a gauge. This generator reads the same signal `test_metric_census.py`
re-checks on every run, so the claim can't quietly drift out from under the
doc again.

Call shape: `create_observable_gauge(meter_provider, name, callback,
description, unit='1')` -- positional `name` (index 1) and `description`
(index 3), keyword `unit` (default `'1'`, the wrapper's own default).
'''
import ast
from dataclasses import dataclass
from pathlib import Path

from tests.cli._otel_resolve import Corpus, pod_of, unparse
from tests.cli._roots import SERVICE_ROOTS, CORE_ROOT

REPO_ROOT = Path(__file__).resolve().parents[2]
METRICS_REFERENCE_DOC = REPO_ROOT / 'docs' / 'monitoring' / 'metrics_reference.md'

_CALL_NAME = 'create_observable_gauge'
_DEFAULT_UNIT = '1'

_POD_LABELS = {root: name for name, root in SERVICE_ROOTS.items()}
_POD_LABELS[CORE_ROOT] = 'core (shared)'

_BEGIN_CENSUS = (
    '<!-- BEGIN GENERATED(metric-census) by tests/cli/test_metric_census.py. '
    'Do not edit this block by hand.\n     '
    'Regenerate with: UPDATE_METRIC_CENSUS=1 pytest tests/cli/test_metric_census.py -->'
)
_END_CENSUS = '<!-- END GENERATED(metric-census) -->'

_BEGIN_EXEMPT = (
    '<!-- BEGIN GENERATED(metric-exempt-appendix) by tests/cli/test_metric_census.py. '
    'Do not edit this block by hand.\n     '
    'Regenerate with: UPDATE_METRIC_CENSUS=1 pytest tests/cli/test_metric_census.py -->'
)
_END_EXEMPT = '<!-- END GENERATED(metric-exempt-appendix) -->'


@dataclass(frozen=True)
class MetricEntry:
    name: str
    description: str
    unit: str
    pod: str
    source: str


@dataclass(frozen=True)
class ExemptMetric:
    source: str
    reason: str
    expression: str


class _GaugeVisitor(ast.NodeVisitor):
    def __init__(self):
        self.class_stack: list[str] = []
        self.hits: list[tuple] = []

    # ast.NodeVisitor dispatches by this exact method name.
    def visit_ClassDef(self, node):  # pylint: disable=invalid-name
        self.class_stack.append(node.name)
        self.generic_visit(node)
        self.class_stack.pop()

    def visit_Call(self, node):  # pylint: disable=invalid-name
        if isinstance(node.func, ast.Name) and node.func.id == _CALL_NAME:
            self.hits.append((node, self.class_stack[-1] if self.class_stack else None))
        self.generic_visit(node)


def _positional_or_kw(call: ast.Call, index: int, kw_name: str):
    if len(call.args) > index:
        return call.args[index]
    return next((k.value for k in call.keywords if k.arg == kw_name), None)


def measure(corpus: Corpus | None = None) -> tuple[list, list]:
    '''Every `create_observable_gauge` call site, resolved where possible.

    Pairs each resolved `name` with its resolved `description` positionally
    when a subclass fan-out produced more than one of either (both
    `QueueMetricsBase` gauges and `QueueWorkerHttpServer`'s heartbeat fan out
    this way) -- the two lists always fan out over the SAME subclass set in
    the same order, since both come from attributes on the same subclass.
    '''
    corpus = corpus or Corpus()
    resolved: list[MetricEntry] = []
    exempt: list[ExemptMetric] = []
    for path, tree in corpus.trees.items():
        visitor = _GaugeVisitor()
        visitor.visit(tree)
        for call, cls in visitor.hits:
            source = f'{path.relative_to(corpus.repo_root)}:{call.lineno}'
            name_expr = _positional_or_kw(call, 1, 'name')
            desc_expr = _positional_or_kw(call, 3, 'description')
            unit_expr = next((k.value for k in call.keywords if k.arg == 'unit'), None)

            if name_expr is None:
                exempt.append(ExemptMetric(source, 'no resolvable name argument', unparse(call)))
                continue
            names = corpus.resolve(name_expr, file=path, cls=cls)
            if not names:
                exempt.append(ExemptMetric(source, 'metric name not statically determinable',
                                           unparse(name_expr)))
                continue

            if desc_expr is None:
                exempt.append(ExemptMetric(source, 'no resolvable description argument',
                                           unparse(call)))
                continue
            descs = corpus.resolve(desc_expr, file=path, cls=cls)
            if not descs:
                exempt.append(ExemptMetric(source, 'description not statically determinable',
                                           unparse(desc_expr)))
                continue

            if unit_expr is None:
                units = [type('_U', (), {'value': _DEFAULT_UNIT, 'file': path})()]
            else:
                units = corpus.resolve(unit_expr, file=path, cls=cls)
                if not units:
                    exempt.append(ExemptMetric(source, 'unit not statically determinable',
                                               unparse(unit_expr)))
                    continue

            # Pair by position when fan-out produced parallel lists (same
            # subclass set, same order, since both attrs live on one
            # subclass); otherwise broadcast a single value against the rest.
            n = max(len(names), len(descs), len(units))

            def _at(seq, i, _n=n):
                return seq[i] if len(seq) == _n else seq[0]

            if len(names) not in (1, n) or len(descs) not in (1, n) or len(units) not in (1, n):
                exempt.append(ExemptMetric(
                    source, 'fan-out size mismatch between name/description/unit',
                    unparse(call),
                ))
                continue
            # Pod attribution must come from whichever argument actually fanned
            # out (the subclass that supplied the override), never from a
            # broadcast argument's file -- a broadcast `name` shared across
            # subclasses still carries the FIRST subclass resolved against it,
            # which is the wrong pod for every row after the first.
            fanout_seq = next((seq for seq in (names, descs, units) if len(seq) == n), None)
            for i in range(n):
                name_r, desc_r, unit_r = _at(names, i), _at(descs, i), _at(units, i)
                # `fanout_seq[i].file` is the subclass that supplies the
                # override and drives `pod` alone -- `source` always points
                # at the call site itself, which is constant across every
                # fanned-out row from this one call.
                attributed_file = fanout_seq[i].file if fanout_seq else name_r.file
                pod_root = pod_of(attributed_file, corpus.repo_root)
                resolved.append(MetricEntry(
                    name=name_r.value, description=desc_r.value, unit=unit_r.value,
                    pod=_POD_LABELS.get(pod_root, pod_root),
                    source=source,
                ))
    return resolved, exempt


def _census_table(resolved: list) -> str:
    rows = sorted({(e.name, e.description, e.unit, e.pod, e.source) for e in resolved})
    lines = ['| Name | Description | Unit | Pod | Source |', '|---|---|---|---|---|']
    for name, description, unit, pod, source in rows:
        lines.append(f'| `{name}` | {description} | `{unit}` | {pod} | `{source}` |')
    return '\n'.join(lines)


def _exempt_appendix(exempt: list) -> str:
    rows = sorted({(e.source, e.reason, e.expression) for e in exempt})
    lines = [
        "Every `create_observable_gauge(...)` call site this census can't",
        'reduce every argument of to a literal, repo-wide. Each one is a',
        'genuine runtime value (a pod label interpolated into a description,',
        'a for-loop binding `description` across four literal strings), not a',
        'gap in the resolver: see `tests/cli/_otel_resolve.py`.',
        '',
        '| Source | Why | Expression |',
        '|---|---|---|',
    ]
    for source, reason, expression in rows:
        lines.append(f'| `{source}` | {reason} | `{expression}` |')
    return '\n'.join(lines)


def render_metrics_reference_doc(original: str, resolved: list, exempt: list) -> str:
    def _replace(text: str, begin: str, end: str, body: str) -> str:
        if begin not in text or end not in text:
            raise AssertionError(
                f'Expected markers not found in metrics_reference.md: {begin!r} / {end!r}. '
                'They were removed by hand -- restore them before regenerating.'
            )
        start = text.index(begin) + len(begin)
        stop = text.index(end, start)
        return text[:start] + '\n\n' + body + '\n\n' + text[stop:]

    text = _replace(original, _BEGIN_CENSUS, _END_CENSUS, _census_table(resolved))
    text = _replace(text, _BEGIN_EXEMPT, _END_EXEMPT, _exempt_appendix(exempt))
    return text
