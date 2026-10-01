'''
Per-pod configuration reference, rendered from each pod's own CLI entrypoint
docstring and spliced into docs/configuration.md's ``## Per-pod configuration
reference`` section.

Four of the six pods' entrypoints (``discord_db/cli/database.py``,
``discord_broker/cli/broker.py``, ``discord_downloader/cli/downloader.py``,
``discord_search/cli/search.py``) already carry an accurate "Configure with:"
docstring block naming every config key that pod's ``run()`` reads, required
vs. optional, with real defaults. The other two (``discord_gateway/cli/bot.py``,
``discord_dispatcher/cli/dispatcher.py``) grew the same block once this
generator existed to read it. None of that prose is worth retyping into a doc
by hand a second time -- this module reads it straight off the module AST and
splices it in verbatim, so the doc cannot drift from the docstring the way a
hand-copied one has drifted before (see project_docs_ci_scope_decisions and
every other generator in this package).

**No hardcoded pod -> entrypoint-module map.** ``tests.cli._roots.SERVICE_ROOTS``
gives pod name -> package root (``'bot': 'discord_gateway'``); combined with
``tests.cli._image_deps.declared_scripts()``, which reads every package's own
``pyproject.toml`` live, the entrypoint module for a root is whichever console
script's target lives under it. A root with no declared script, or a docstring
that lost its "Configure with:" marker, fails the generator loudly rather than
silently emitting an empty section for that pod.

**Static, not measured by import** -- unlike ``_image_deps``/``_seam_topology``,
which run each entrypoint in a subprocess to see what it actually reaches. A
docstring is text sitting in the file; `ast.parse` + `ast.get_docstring` reads
it the same way `_otel_resolve.py` and `_span_census.py` statically resolve
source rather than importing it, and it is the whole module docstring in this
case, not an expression inside a function.
'''
import ast
from pathlib import Path

from tests.cli._image_deps import IMAGE_NAMES, REPO_ROOT, declared_scripts
from tests.cli._roots import SERVICE_ROOTS

CONFIGURATION_DOC = REPO_ROOT / 'docs' / 'configuration.md'

_BEGIN = (
    '<!-- BEGIN GENERATED(config-reference) by tests/cli/test_config_reference.py. '
    'Do not edit this block by hand.\n     '
    'Regenerate with: UPDATE_CONFIG_REFERENCE=1 pytest tests/cli/test_config_reference.py -->'
)
_END = '<!-- END GENERATED(config-reference) -->'

_MARKER = 'Configure with:'

#: Pod short names in the order IMAGE_NAMES already declares them -- the same
#: order the CI matrix and docs/architecture.md's edge table walk the six pods
#: in. Derived rather than restated: a reordering of IMAGE_NAMES carries this
#: along with it instead of leaving two orderings to keep in step by hand.
POD_ORDER = [name.replace('discord-', '') for name in IMAGE_NAMES.values()]


def entrypoint_module(root: str) -> str:
    '''The dotted module path whose console script lives under `root`.

    Reads every package's own pyproject.toml via `declared_scripts()` rather
    than assuming `root.cli.<root-without-prefix>` -- the one console script
    each package declares is the actual, checked-in contract; a module that
    merely sits in `cli/` but is not wired to a script would be the wrong thing
    to document here.
    '''
    for target in declared_scripts().values():
        module, _, _func = target.partition(':')
        if module.split('.')[0] == root:
            return module
    raise AssertionError(
        f'No console script in any pyproject.toml targets a module under {root!r}. '
        'Every SERVICE_ROOTS entry must have one -- add [project.scripts] to '
        f'{root}/pyproject.toml.'
    )


def configure_with_block(module: str) -> str:
    '''The verbatim "Configure with:" block from `module`'s module docstring.

    `clean=False` so the block's own internal indentation and alignment --
    the whole reason it is legible -- survives the trip through `ast`
    unchanged, rather than being dedented by `inspect.cleandoc`.
    '''
    path = REPO_ROOT / Path(*module.split('.')).with_suffix('.py')
    tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
    doc = ast.get_docstring(tree, clean=False)
    if doc is None or _MARKER not in doc:
        raise AssertionError(
            f'{path} has no "Configure with:" block in its module docstring. '
            'Add one (see discord_db/cli/database.py for the shape) before '
            'regenerating docs/configuration.md.'
        )
    return doc[doc.index(_MARKER):].rstrip('\n')


def render_block() -> str:
    '''Render the generated per-pod config reference content from a live read
    of each pod's own entrypoint docstring.

    No top-level heading and no GENERATED markers of its own -- this is the
    BODY spliced between the paired markers already sitting in
    docs/configuration.md's ``## Per-pod configuration reference`` section, by
    `render_configuration_doc`, the same shape as `_seam_topology.render_block`.
    '''
    lines = [
        'Generated from each pod\'s own `cli/<entrypoint>.py` module docstring --',
        'nothing here is retyped by hand, so it cannot drift from the code the way a',
        'hand-copied reference has before. Per-cog config (`music.*`, `markov.*`,',
        '`role.*`, `urban.*`, `delete_messages.*`) is named in the bot pod\'s block only',
        'as a pointer; each has its own page (docs/music.md, docs/markov.md, etc.).',
    ]
    for pod in POD_ORDER:
        root = SERVICE_ROOTS[pod]
        module = entrypoint_module(root)
        image = IMAGE_NAMES[module]
        block = configure_with_block(module)
        lines += ['', f'### {image}', '', '```', block, '```']
    return '\n'.join(lines)


def render_configuration_doc(original: str) -> str:
    '''Splice the generated config-reference block into `original` (the full
    current content of docs/configuration.md), following the same
    paired-marker pattern as `_seam_topology.render_architecture_doc`.
    Everything outside the markers -- the hand-written sections around it --
    passes through untouched.
    '''

    def _replace(text: str, begin: str, end: str, body: str) -> str:
        if begin not in text or end not in text:
            raise AssertionError(
                f'Expected markers not found in configuration.md: {begin!r} / {end!r}. '
                'They were removed by hand -- restore them before regenerating.'
            )
        start = text.index(begin) + len(begin)
        stop = text.index(end, start)
        return text[:start] + '\n\n' + body + '\n\n' + text[stop:]

    return _replace(original, _BEGIN, _END, render_block())
