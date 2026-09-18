'''
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

SHARED_ENUM = REPO_ROOT / 'discord_bot' / 'utils' / 'otel.py'


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
    for module, image in (('discord_bot.utils.bot_metrics', 'bot'),
                          ('discord_bot.workers.broker_metrics', 'broker')):
        reached = sorted(i for i, modules in claims.items() if module in modules)
        assert reached == [image], f'{module} is reached by {reached}, not just {image}'
