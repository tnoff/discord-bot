'''
The per-pod "Configure with:" reference spliced into docs/configuration.md,
and the assertions that keep it honest.

Generated straight from each pod's own `cli/<entrypoint>.py` module docstring
via `ast`, rather than a second hand-maintained copy -- this project's
documented reason every other `tests/cli/_*.py` generator exists (see
_seam_topology.py, _span_census.py).
'''
import os

import pytest

import tests.cli._config_reference as config_reference
from tests.cli._config_reference import (
    CONFIGURATION_DOC, POD_ORDER, configure_with_block, entrypoint_module,
    render_configuration_doc,
)
from tests.cli._image_deps import IMAGE_NAMES
from tests.cli._roots import SERVICE_ROOTS


def test_pod_order_covers_exactly_the_six_pods():
    '''POD_ORDER, derived from IMAGE_NAMES, must name every SERVICE_ROOTS pod once.

    Pinned so a pod added to one table without the other fails here instead of
    the generated doc quietly carrying five sections forever.
    '''
    assert set(POD_ORDER) == set(SERVICE_ROOTS)
    assert len(POD_ORDER) == len(set(POD_ORDER))


def test_every_pod_resolves_to_an_entrypoint_module():
    '''Every SERVICE_ROOTS package declares exactly the one console script this
    generator expects to find under it.
    '''
    for pod, root in SERVICE_ROOTS.items():
        module = entrypoint_module(root)
        assert module in IMAGE_NAMES, (
            f'{pod}: entrypoint_module resolved {module!r}, which IMAGE_NAMES '
            'does not recognise as one of the six image entrypoints.'
        )


def test_every_entrypoint_has_a_configure_with_block():
    '''Every one of the six entrypoints' docstrings carries the marker.

    Guards the failure mode the task that added this generator called out by
    name: a pod whose docstring loses the block should fail loudly here, not
    silently render an empty section in docs/configuration.md.
    '''
    for module in IMAGE_NAMES:
        block = configure_with_block(module)
        assert block.startswith('Configure with:')
        assert len(block.splitlines()) > 1


def test_configure_with_block_missing_marker_raises(tmp_path, monkeypatch):
    '''A module with no "Configure with:" marker in its docstring fails loudly.'''
    monkeypatch.setattr(config_reference, 'REPO_ROOT', tmp_path)
    package = tmp_path / 'no_marker_pod' / 'cli'
    package.mkdir(parents=True)
    (tmp_path / 'no_marker_pod' / '__init__.py').write_text('', encoding='utf-8')
    (package / '__init__.py').write_text('', encoding='utf-8')
    (package / 'entry.py').write_text("'''No marker here.'''\n", encoding='utf-8')

    with pytest.raises(AssertionError, match='Configure with'):
        config_reference.configure_with_block('no_marker_pod.cli.entry')


def test_entrypoint_module_missing_script_raises():
    '''A root with no declared console script fails loudly rather than
    silently resolving to nothing.
    '''
    with pytest.raises(AssertionError, match='No console script'):
        entrypoint_module('not_a_real_package')


def test_config_reference_doc_is_current():
    '''The generated per-pod block in docs/configuration.md matches a live
    read of every entrypoint's own docstring.

    Regenerate with: UPDATE_CONFIG_REFERENCE=1 pytest tests/cli/test_config_reference.py
    '''
    original = CONFIGURATION_DOC.read_text(encoding='utf-8')
    rendered = render_configuration_doc(original)
    if os.environ.get('UPDATE_CONFIG_REFERENCE'):
        CONFIGURATION_DOC.write_text(rendered, encoding='utf-8')
        original = rendered
    assert original == rendered, (
        'docs/configuration.md\'s per-pod config reference is out of date. Regenerate with:\n'
        '    UPDATE_CONFIG_REFERENCE=1 pytest tests/cli/test_config_reference.py'
    )
