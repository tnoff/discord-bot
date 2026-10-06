'''
The CI build filter: which images a set of changed files actually affects.

Every test here drives the pure `affected()` rather than the git-backed main(),
so the cases that matter -- a deletion, a test-only dependency bump, a file no
image claims -- are stated directly instead of being staged as commits.

The closure fixtures are deliberately tiny and hand-written. Using the real
tests/cli/image-closure.json would make these tests restate whatever the tree
happens to look like today, so a regression would change the fixture and the
expectation together and assert nothing.
'''
import json

import pytest

from tests.cli._affected_images import (
    affected, images_for_pyproject, module_for,
)
from tests.cli._image_deps import CLOSURE_DOC, REPO_ROOT
from tests.cli._roots import LEGACY_ROOT, ROOTS


CLOSURE = {'images': [
    {'image': 'bot', 'entrypoint': 'discord_bot.cli.bot', 'dockerfile': 'docker/Dockerfile.gateway',
     'modules': ['discord_bot', 'discord_bot.cogs.music', 'discord_bot.shared']},
    {'image': 'db', 'entrypoint': 'discord_bot.cli.database', 'dockerfile': 'docker/Dockerfile.db',
     'modules': ['discord_bot', 'discord_bot.shared', 'discord_bot.store']},
]}


def _affected(files, base_closure=None, deleted=()):
    return affected(files, deleted, CLOSURE, base_closure)


@pytest.mark.parametrize('path, expected', [
    ('discord_bot/cogs/music.py', 'discord_bot.cogs.music'),
    ('discord_bot/__init__.py', 'discord_bot'),
    ('discord_bot/cogs/__init__.py', 'discord_bot.cogs'),
    ('tests/helpers.py', None),
    ('discord_bot/data.txt', None),
    ('README.md', None),
    # The post-split layout (criterion 7). These are not hypothetical spellings:
    # the folder classes are enforced by test_every_module_lives_where_its_closure_says
    # in test_import_boundaries.py (no longer documented in a generated prose doc,
    # just checked), and keeping `discord_bot` as the import root is what lets one
    # rule serve both layouts while the tree is half-moved.
    ('discord_bot/core/utils/otel.py', 'discord_bot.core.utils.otel'),
    ('discord_bot/core/__init__.py', 'discord_bot.core'),
    ('discord_bot/seams/database/routes.py', 'discord_bot.seams.database.routes'),
    ('discord_bot/services/bot/cogs/music.py', 'discord_bot.services.bot.cogs.music'),
    ('discord_bot/services/downloader/__init__.py', 'discord_bot.services.downloader'),
    # The criterion 8 roots, which nothing on disk uses yet. They are here
    # BEFORE the move for the reason step 1 exists: a path this does not
    # recognise contributes no images, so the first moved file would build
    # nothing and say nothing. Both spellings answer for the whole migration.
    ('discord_core/utils/otel.py', 'discord_core.utils.otel'),
    ('discord_core/__init__.py', 'discord_core'),
    ('discord_gateway/cogs/music.py', 'discord_gateway.cogs.music'),
    ('discord_db/database.py', 'discord_db.database'),
    ('discord_downloader/__init__.py', 'discord_downloader'),
    # An UNDECLARED root is not first-party. Recognising any `*/**.py` would
    # make scripts/foo.py an orphan and fail CI on a file that builds nothing.
    ('discord_notathing/x.py', None),
    ('discord_bot.py', None),
    # All five seams folded into `discord_core` (criterion 8's seam-fold
    # reversal) and, unlike LEGACY_ROOT, were fully retired from ROOTS
    # rather than kept for dual-spelling -- a path under any of them is now
    # exactly as undeclared as discord_notathing above.
    ('discord_seam_queue_worker/workers/redis_guild_queue.py', None),
    ('discord_seam_dispatch/clients/http_dispatch_client.py', None),
    # Criterion 8 step 5 nested each package's own test tree inside its own
    # directory, so a path under it now passes the root_of() check the same
    # as production code -- but no entrypoint's closure will ever claim it.
    # This is the exact shape that made PR #1005's "Detect image-input
    # changes" job fail on all 166 of them: root_of() alone cannot tell test
    # code apart from shipped code once both live under the same root.
    ('discord_core/tests/utils/test_otel.py', None),
    ('discord_gateway/tests/cogs/test_music.py', None),
    ('discord_db/tests/__init__.py', None),
])
def test_module_for(path, expected):
    '''Paths map to module names, and non-modules map to nothing.'''
    assert module_for(path) == expected


def test_a_move_without_a_regenerated_closure_fails_loudly():
    """
    Moving a module without regenerating the closure is an orphan, not silence.

    This is the hazard criterion 7 step 2 exists to prevent, and it is worth a
    test rather than an argument. A filter that did not recognise the new path
    would contribute no images for it and the move would build NOTHING -- the
    silent under-build criterion 3 exists to stop, arriving through the back
    door. Because the layout keeps `discord_bot` as the import root, the new
    path maps to a module name like any other; it is simply a name the stale
    closure does not claim, so it lands in the orphan list and CI stops.

    The old path is attributed by the base closure, which is what tells a move
    apart from a deletion.
    """
    images, orphans = _affected(
        ['discord_bot/cogs/music.py', 'discord_bot/services/bot/cogs/music.py'],
        base_closure=CLOSURE, deleted=['discord_bot/cogs/music.py'])
    assert orphans == ['discord_bot/services/bot/cogs/music.py']
    assert images == {'bot'}


def test_a_move_with_a_regenerated_closure_builds_only_the_owning_image():
    """The same move, done properly, is an ordinary one-image change."""
    moved = {'images': [
        {**image,
         'modules': ['discord_bot.services.bot.cogs.music' if m == 'discord_bot.cogs.music' else m
                     for m in image['modules']]}
        for image in CLOSURE['images']
    ]}
    images, orphans = affected(
        ['discord_bot/cogs/music.py', 'discord_bot/services/bot/cogs/music.py'],
        ['discord_bot/cogs/music.py'], moved, CLOSURE)
    assert not orphans
    assert images == {'bot'}


def test_a_bot_only_module_builds_only_the_bot():
    '''The whole point: a change one image can reach does not build the others.'''
    images, orphans = _affected(['discord_bot/cogs/music.py'])
    assert images == {'bot'}
    assert not orphans


def test_a_shared_module_builds_every_image_that_claims_it():
    '''Fanout is read from the closure, not guessed from the path.'''
    images, _ = _affected(['discord_bot/shared.py'])
    assert images == {'bot', 'db'}


def test_documentation_builds_nothing():
    '''A file no image consumes yields no images rather than defaulting to all.'''
    images, orphans = _affected(['README.md', 'docs/architecture.md'])
    assert images == set()
    assert not orphans


def test_unclaimed_module_is_an_orphan_not_an_empty_answer():
    '''
    A module in the tree that no image reaches fails the build.

    This is the 2026-09-04 shape: "no image needs this" and "the graph has not
    caught up" look identical from here, and only one is safe to skip.
    '''
    images, orphans = _affected(['discord_bot/nowhere.py'])
    assert orphans == ['discord_bot/nowhere.py']
    assert images == set()


def test_a_deleted_module_is_attributed_to_its_old_owners():
    '''
    Deleting a module is not an orphan, and it does not silently build nothing.

    The file is gone from the head closure, which looks exactly like a module no
    image claims. The base closure is what tells them apart.
    '''
    base = {'images': [
        {'image': 'bot', 'entrypoint': 'discord_bot.cli.bot', 'dockerfile': 'docker/Dockerfile.gateway',
         'modules': ['discord_bot', 'discord_bot.gone']},
        {'image': 'db', 'entrypoint': 'discord_bot.cli.database', 'dockerfile': 'docker/Dockerfile.db',
         'modules': ['discord_bot']},
    ]}
    images, orphans = _affected(['discord_bot/gone.py'], base_closure=base,
                                deleted=['discord_bot/gone.py'])
    assert not orphans, 'a deletion must not be reported as an unreachable module'
    assert images == {'bot'}


def test_dockerfile_change_builds_only_its_own_image():
    '''Each Dockerfile is an input to exactly one image.'''
    images, _ = _affected(['docker/Dockerfile.db'])
    assert images == {'db'}


def test_entrypoint_is_genuinely_all_six():
    '''Every image COPYs it, so all-six is correct rather than lazy.'''
    assert _affected(['docker/entrypoint.sh'])[0] == {'bot', 'db'}


def test_retired_root_version_reaches_no_image():
    '''
    The repo-wide VERSION is gone and no Dockerfile COPYs it, so deleting it (or
    a stray one reappearing) builds nothing and is not an orphan either.
    '''
    images, orphans = _affected(['VERSION'], deleted=['VERSION'])
    assert images == set()
    assert not orphans


def test_alembic_belongs_to_the_db_image_alone():
    '''Only Dockerfile.db COPYs the migrations.'''
    assert _affected(['alembic/env.py'])[0] == {'db'}
    assert _affected(['alembic.ini'])[0] == {'db'}


def test_docker_files_no_image_copies_build_nothing():
    '''
    The old filter matched `docker/*` and rebuilt six for these.

    No Dockerfile COPYs the sample configs or the compose file, so a change to
    one of them cannot alter any image.
    '''
    images, _ = _affected(['docker/discord.bot.cnf.example', 'docker/docker-compose.multiprocess.yml'])
    assert images == set()


# The extras/TOML-diffing tests this file used to carry here -- a [test] bump
# building nothing, a [db]/[storage] dependency bump reaching only the extras
# that pulled it, an unparseable base falling back to every image, and
# `extra_closure`'s self-reference resolution -- are gone along with the
# machinery they tested. Criterion 8 step 5 retired the per-image extras on
# one pyproject.toml entirely; `images_for_pyproject` no longer reads any
# TOML content (there is nothing left to diff, and so nothing that can fail
# to parse), only the CHANGED PATH, so the replacement tests below check paths
# rather than dependency-string edits.
def test_root_pyproject_bump_builds_nothing():
    '''
    The root pyproject.toml holds only the shared `test` extra and tool
    config as of criterion 8 step 5 -- it installs no production package of
    its own, so a change to it reaches no deployed image. This is the
    [test]-bump-builds-nothing case, now true of the WHOLE file rather than
    one extra inside it.
    '''
    images, _ = _affected(['pyproject.toml'])
    assert images == set()


def test_a_packages_own_pyproject_bump_builds_only_its_image():
    '''Each package's own pyproject.toml reaches exactly the images whose
    measured closure claims a module under that root -- the same rule a .py
    change under it already gets, not a separate mechanism.'''
    closure = {'images': [
        {'image': 'bot', 'entrypoint': 'discord_gateway.cli.bot', 'dockerfile': 'docker/Dockerfile.gateway',
         'modules': ['discord_core.shared', 'discord_gateway.cogs.music']},
        {'image': 'db', 'entrypoint': 'discord_db.cli.database', 'dockerfile': 'docker/Dockerfile.db',
         'modules': ['discord_core.shared', 'discord_db.store']},
    ]}
    images, _ = affected(['discord_gateway/pyproject.toml'], [], closure, None)
    assert images == {'bot'}


def test_discord_cores_own_pyproject_bump_reaches_every_image_that_installs_it():
    '''discord_core's dependencies reach every pod that installs discord_core --
    the same measured-cost shape boto3 already accepted in the seam-fold
    reversal, not a special case for this function.'''
    closure = {'images': [
        {'image': 'bot', 'entrypoint': 'discord_gateway.cli.bot', 'dockerfile': 'docker/Dockerfile.gateway',
         'modules': ['discord_core.shared', 'discord_gateway.cogs.music']},
        {'image': 'db', 'entrypoint': 'discord_db.cli.database', 'dockerfile': 'docker/Dockerfile.db',
         'modules': ['discord_core.shared', 'discord_db.store']},
    ]}
    images, _ = affected(['discord_core/pyproject.toml'], [], closure, None)
    assert images == {'bot', 'db'}


def test_images_for_pyproject_of_an_undeclared_root_is_empty():
    '''No TOML to fail to parse any more -- an unrecognised root just answers
    with no images, the same as any other path no image claims.'''
    assert images_for_pyproject('discord_notathing/pyproject.toml', {'bot': set()}) == set()


def test_generated_closure_covers_every_image_the_matrix_needs():
    '''
    The committed artifact is well-formed and complete.

    Weak on purpose: test_image_closure_is_current already checks it matches a
    live measurement. This checks the shape ci.yml depends on -- every image has
    a dockerfile and a non-empty module list -- so a malformed regeneration is
    caught here rather than as a confusing YAML error in the changes job.
    '''
    closure = json.loads(CLOSURE_DOC.read_text(encoding='utf-8'))
    assert closure['images'], 'no images in the generated closure'
    for image in closure['images']:
        assert image['image'], image
        assert image['dockerfile'], image
        assert image['modules'], f'{image["image"]} claims no modules'
        # Not `LEGACY_ROOT in image['modules']`: that held while every pod's code
        # still lived under it, and criterion 8 step 4 is what makes it stop
        # holding for the first image (dispatcher moved out entirely). The
        # invariant that survives the hoist is that every claimed module sits
        # under SOME declared root.
        assert all(module.split('.')[0] in ROOTS for module in image['modules']), (
            f'{image["image"]} claims a module under an undeclared root: '
            f'{[m for m in image["modules"] if m.split(".")[0] not in ROOTS]}'
        )


def test_release_yml_gates_each_push_on_its_own_image():
    """`release.yml` must ask `_affected_images` and gate every push per image.

    It used to be one boolean over a `case` glob gating all six pushes, so a
    change confined to discord_db (#1017) pushed and bumped all six images.
    The pairing is read from the workflow rather than restated here, so adding
    an image to the closure without a gated `push-<image>` job fails.
    """
    workflow = (REPO_ROOT / '.github/workflows/release.yml').read_text(encoding='utf-8')
    assert 'tests.cli._affected_images' in workflow, 'release.yml no longer uses the shared filter'
    assert "outputs.image ==" not in workflow, 'a single all-six boolean gate is back'
    closure = json.loads(CLOSURE_DOC.read_text(encoding='utf-8'))
    for image in (i['image'] for i in closure['images']):
        gate = f"contains(fromJSON(needs.changes.outputs.images), '{image}')"
        assert workflow.count(gate) == 1, f'push job for {image} is not gated on exactly its own image'


def test_the_declared_roots_are_exactly_the_packages_on_disk():
    """Every import root in the tree is declared, and every declared one is real
    or still to come.

    This is what keeps `module_for` honest. It recognises a fixed set rather
    than any top-level directory, so a root that appears on disk WITHOUT being
    declared is not first-party to the filter -- its files contribute no images
    and build nothing. Equality in the on-disk direction is the check that
    cannot be satisfied by the migration quietly not happening.

    The other direction was deliberately NOT equality while the migration ran:
    most of the thirteen roots did not exist yet, which was the point of
    declaring them a step early. Criterion 8 step 4 finished the last pod move
    (the gateway, formerly `discord_bot/services/bot/`) and `LEGACY_ROOT`
    vanished from disk with it -- this test used to assert it was still there,
    specifically so this exact moment would be caught rather than pass
    quietly. It was caught. `LEGACY_ROOT` stays declared in `ROOTS` for now: it
    costs nothing (`package_dirs()` already skips a root with no
    `__init__.py`), and retiring the dual-spelling machinery in `canonical()`,
    `home_of()` and `folder_of()` is a real but separate decision -- nothing in
    the current tree needs the legacy spelling recognised any more, but
    removing the recognition itself is not required by any move, only by a
    choice to stop supporting it.
    """
    on_disk = {entry.name for entry in REPO_ROOT.iterdir()
               if entry.is_dir() and (entry / '__init__.py').is_file()
               and not entry.name.startswith(('.', 'test'))}
    undeclared = on_disk - ROOTS
    assert not undeclared, (
        f'these importable top-level packages are not declared in _roots.py: '
        f'{sorted(undeclared)}\n'
        'module_for() does not recognise them, so a change to a file inside one '
        'builds no images and reports nothing. Add them to ROOTS.'
    )
    assert LEGACY_ROOT not in on_disk, (
        'the legacy root is back on disk -- something reintroduced '
        'discord_bot/, which criterion 8 step 4 retired'
    )


def test_the_tox_gates_cover_every_root_on_disk():
    """pylint, bandit and coverage have to name every import root that exists.

    NOT on the criterion 8 step 1 list, and it belongs there. `tox.ini` runs
    `pylint discord_bot/`, `bandit -r discord_bot/` and `pytest
    --cov=discord_bot`, all three keyed on the root by name. The moment a
    package is hoisted out of `discord_bot/`, all three go on passing while
    covering strictly less -- and the coverage one is the worst of them, because
    `--cov-fail-under=99` stays green against a shrinking denominator. A gate
    that quietly stops measuring the thing it guards is the failure this project
    has logged five times under other names.

    Asserted against roots that EXIST rather than all declared ones, so this
    stays green through step 1 -- where nothing has moved -- and fails on the
    first PR that hoists a package without widening the gates.
    """
    tox = (REPO_ROOT / 'tox.ini').read_text(encoding='utf-8')
    on_disk = sorted(root for root in ROOTS if (REPO_ROOT / root / '__init__.py').is_file())
    assert on_disk, 'no declared root exists on disk -- this test stopped checking anything'

    gates = {
        'pylint': [line for line in tox.splitlines() if 'pylint' in line and '.pylintrc.test' not in line],
        'bandit': [line for line in tox.splitlines() if 'bandit -r' in line],
    }
    for name, lines in gates.items():
        assert lines, f'no {name} invocation found in tox.ini -- the gate or this test moved'
        text = ' '.join(lines)
        missing = [root for root in on_disk if root not in text]
        assert not missing, (
            f'tox.ini\'s {name} gate does not cover {missing}.\n'
            f'  {text.strip()}\n'
            'It would keep passing while measuring less than the whole tree.'
        )

    # Coverage is covered by CONSTRUCTION rather than by enumeration, which is
    # strictly better: pyproject's source root is the repo, so a new package is
    # measured the moment it exists and no list can fall behind.
    #
    # Enumerating `--cov=<root>` also had a second cost that only shows at more
    # than one root. Coverage writes one <source> per root and strips the
    # matching root off each filename, so `discord_core/utils/otel.py` is
    # recorded as `utils/otel.py`; diff-cover then reconstructs the path by
    # testing each <source>, and a name under several roots resolves to the
    # first that exists. Measured at the media_search move: four names collided
    # and coverage.xml held one entry for each instead of three.
    assert '--cov=' not in tox, (
        'tox.ini enumerates --cov=<root> again. That makes coverage.xml ambiguous '
        'for any filename present under more than one root, and diff-cover then '
        'reports the wrong file. Use bare --cov with pyproject\'s source root.'
    )
    assert '--cov ' in tox or tox.rstrip().endswith('--cov'), 'no --cov invocation in tox.ini'
    pyproject = (REPO_ROOT / 'pyproject.toml').read_text(encoding='utf-8')
    assert '[tool.coverage.run]' in pyproject and 'source = ["."]' in pyproject, (
        'pyproject no longer sets a single repo-rooted coverage source, so '
        'tox.ini\'s bare --cov measures nothing in particular.'
    )
