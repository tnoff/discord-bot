'''
Where a metric name lives, and that it lives in an enum at all.

A metric only one image emits does not belong in the shared enum.

This is the rule the tier split exists to hold, and without a check it does not
hold: every one of the 21 names that had to be moved arrived one PR at a time,
each individually reasonable, in the only enum anyone knew about. Nothing failed,
because nothing was looking -- a tier-local metric in a fanout-6 module is a
rebuild cost, not an error.

The check reads the MEASURED closure in docs/image-closure.json rather than a
static walk, for the same reason the CI build filter does: that file is what
CI itself keys on, so a disagreement here is a disagreement with the build.
'''
import ast
import json
import subprocess  # nosec B404 - fixed argv, no shell, greps the repo
from pathlib import Path

import pytest

from tests.cli._image_deps import CLOSURE_DOC, REPO_ROOT

SHARED_ENUM = REPO_ROOT / 'discord_bot' / 'core' / 'utils' / 'otel.py'


def _members(path: Path, class_name: str) -> list:
    tree = ast.parse(path.read_text(encoding='utf-8'))
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            return [n.targets[0].id for n in node.body
                    if isinstance(n, ast.Assign) and isinstance(n.targets[0], ast.Name)]
    return []


def _module_for(path: str) -> str | None:
    if not path.startswith('discord_bot/') or not path.endswith('.py'):
        return None
    module = path[: -len('.py')].replace('/', '.')
    return module[: -len('.__init__')] if module.endswith('.__init__') else module


def _images_emitting(class_name: str, member: str, claims: dict) -> set:
    '''Images whose closure contains a module referencing `class_name.member`.'''
    found = subprocess.run(  # nosec B603 B607 - fixed argv
        ['grep', '-rl', f'{class_name}\\.{member}\\b', '--include=*.py', 'discord_bot/'],
        cwd=REPO_ROOT, capture_output=True, text=True, check=False).stdout.split()
    images = set()
    for path in found:
        module = _module_for(path)
        if module is None:
            continue
        images |= {image for image, modules in claims.items() if module in modules}
    return images


@pytest.fixture(name='claims', scope='module')
def _claims():
    closure = json.loads(CLOSURE_DOC.read_text(encoding='utf-8'))
    return {image['image']: set(image['modules']) for image in closure['images']}


def test_no_shared_metric_name_is_emitted_by_one_image(claims):
    '''
    Every name in MetricNaming is emitted by two or more images.

    A single-image name here makes a tier-local change edit a module all six
    import. Move it to that tier's module -- utils/bot_metrics.py,
    workers/broker_metrics.py -- and this passes again.

    An unused name fails too, and deliberately: it is either dead, or the enum
    and its emitter have drifted apart. Both want a human, and neither shows up
    anywhere else.
    '''
    offenders = {}
    for member in _members(SHARED_ENUM, 'MetricNaming'):
        images = _images_emitting('MetricNaming', member, claims)
        if len(images) < 2:
            offenders[member] = sorted(images) or ['UNUSED']
    assert not offenders, (
        f'metric names in the shared enum that fewer than two images emit: {offenders}. '
        f'A tier-local name here rebuilds all six images to ship a string one uses.'
    )


@pytest.mark.parametrize('module, class_name, image', [
    ('discord_bot/utils/bot_metrics.py', 'BotMetricNaming', 'bot'),
    ('discord_bot/workers/broker_metrics.py', 'BrokerMetricNaming', 'broker'),
])
def test_tier_enums_hold_only_their_own_tiers_names(module, class_name, image, claims):
    '''
    The converse: a tier module must not accumulate names other images emit.

    Without this the split rots the other way -- a name added to bot_metrics.py
    and then used from a shared module would be invisible here while quietly
    pulling the bot's enum into another image's closure.
    '''
    strays = {}
    for member in _members(REPO_ROOT / module, class_name):
        images = _images_emitting(class_name, member, claims)
        if images - {image}:
            strays[member] = sorted(images)
    assert not strays, (
        f'{class_name} holds names emitted outside {image}: {strays}. '
        f'Move them to utils/otel.py, which is where shared names belong.'
    )


def test_the_tier_modules_are_reached_by_their_own_image_alone(claims):
    '''
    The split only pays if the new modules are single-image.

    A tier enum that some shared module imports is fanout-6 again, and the
    rebuild it was meant to avoid comes straight back with the file renamed.
    '''
    for module, image in (('discord_bot.services.bot.utils.bot_metrics', 'bot'),
                          ('discord_bot.services.broker.workers.broker_metrics', 'broker')):
        reached = sorted(i for i, modules in claims.items() if module in modules)
        assert reached == [image], f'{module} is reached by {reached}, not just {image}'


_INSTRUMENT_FACTORIES = frozenset({
    'create_counter',
    'create_up_down_counter',
    'create_histogram',
    'create_gauge',
    'create_observable_counter',
    'create_observable_gauge',
    'create_observable_up_down_counter',
})


def _calls(node, params=frozenset()):
    '''Every Call in the tree, paired with the parameter names in scope for it.'''
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        params = frozenset(arg.arg for arg in node.args.args + node.args.kwonlyargs)
    if isinstance(node, ast.Call):
        yield node, params
    for child in ast.iter_child_nodes(node):
        yield from _calls(child, params)


def _instrument_name_arg(call):
    '''
    The expression naming the instrument a call creates, or None if it creates none.

    Two shapes exist. `METER_PROVIDER.create_counter(name=...)` names the
    instrument on the meter itself, and `create_observable_gauge(METER_PROVIDER,
    <name>, ...)` -- the helper in utils/otel.py -- takes the meter first and the
    name second. Reading only the first would miss every gauge in the project.
    '''
    func = call.func
    if isinstance(func, ast.Attribute) and func.attr in _INSTRUMENT_FACTORIES:
        positional = call.args[0] if call.args else None
    elif isinstance(func, ast.Name) and func.id in _INSTRUMENT_FACTORIES:
        positional = call.args[1] if len(call.args) > 1 else None
    else:
        return None
    for keyword in call.keywords:
        if keyword.arg == 'name':
            return keyword.value
    return positional


def _names_an_enum_member(expr) -> bool:
    '''True for `SomethingNaming.MEMBER.value`, and nothing else.'''
    return (isinstance(expr, ast.Attribute) and expr.attr == 'value'
            and isinstance(expr.value, ast.Attribute)
            and isinstance(expr.value.value, ast.Name)
            and expr.value.value.id.endswith('Naming'))


def _deferred_attribute(expr) -> str | None:
    '''
    The attribute name in `self.X` / `cls.X`, which defers the choice to a subclass.

    QueueMetricsBase names its three gauges this way so the downloader and the
    search pod can share one body. The name is still chosen from an enum -- just
    one class further down -- so the rule follows the indirection rather than
    forbidding it.
    '''
    if (isinstance(expr, ast.Attribute) and isinstance(expr.value, ast.Name)
            and expr.value.id in {'self', 'cls'}):
        return expr.attr
    return None


def _class_attribute_assignments(tree, wanted: set) -> list:
    '''Every class-body assignment to one of `wanted`, as (attribute, value).'''
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        for stmt in node.body:
            if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                targets, value = [stmt.target], stmt.value
            elif isinstance(stmt, ast.Assign):
                targets, value = [t for t in stmt.targets if isinstance(t, ast.Name)], stmt.value
            else:
                continue
            if value is None:      # a bare `X: ClassVar[str]` declares, it does not name
                continue
            found.extend((t.id, value) for t in targets if t.id in wanted)
    return found


def test_every_instrument_is_named_from_an_enum():
    '''
    An instrument named by a string literal is invisible to every other check here.

    This is the gap that hid `discord_bot.dispatch.request.count` through three
    renames. Every scheme test next door reads the enums -- dots, pod prefixes,
    `_count` on a gauge -- so a name that never reached an enum was never
    examined, and it broke three of those rules while the suite stayed green. It
    was emitted by the bot AND the broker, so the pod prefix it carried was
    wrong for the majority of its series.

    Enum membership is the hinge the rest of the metric tests hang on: assert it
    here, and every other assertion applies to every metric the project emits.

    Two things are not literals and are allowed. Forwarding: utils/otel.py's
    create_observable_gauge passes on the `name` its caller chose, and the caller
    is where the rule bites. Deferral: QueueMetricsBase names its gauges
    `self.QUEUE_DEPTH_METRIC` so two pods can share one body -- so the check
    follows that attribute to the subclasses and requires an enum THERE, which
    is what stops the indirection becoming the next hiding place.
    '''
    offenders, deferred = {}, set()
    trees = {path: ast.parse(path.read_text(encoding='utf-8'))
             for path in sorted((REPO_ROOT / 'discord_bot').rglob('*.py'))}

    for path, tree in trees.items():
        for call, params in _calls(tree):
            expr = _instrument_name_arg(call)
            if expr is None or _names_an_enum_member(expr):
                continue
            if isinstance(expr, ast.Name) and expr.id in params:
                continue
            attribute = _deferred_attribute(expr)
            if attribute is not None:
                deferred.add(attribute)
                continue
            offenders[f'{path.relative_to(REPO_ROOT)}:{call.lineno}'] = ast.unparse(expr)

    supplied = set()
    for path, tree in trees.items():
        for attribute, value in _class_attribute_assignments(tree, deferred):
            supplied.add(attribute)
            if not _names_an_enum_member(value):
                offenders[f'{path.relative_to(REPO_ROOT)}:{value.lineno}'] = ast.unparse(value)

    assert not offenders, (
        f'instruments named outside the naming enums: {offenders}. '
        f'Add the name to MetricNaming (two or more images) or the tier module '
        f'for the one image that emits it, and reference it as `.value`.'
    )
    assert deferred == supplied, (
        f'instrument names deferred to a subclass that no subclass supplies: '
        f'{sorted(deferred - supplied)}. An unsupplied name is an AttributeError '
        f'at import, and nothing else here would catch it.'
    )
