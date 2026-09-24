'''
The CI build filter: which images a set of changed files actually affects.

Every test here drives the pure `affected()` rather than the git-backed main(),
so the cases that matter -- a deletion, a test-only dependency bump, a file no
image claims -- are stated directly instead of being staged as commits.

The closure fixtures are deliberately tiny and hand-written. Using the real
docs/image-closure.json would make these tests restate whatever the tree happens
to look like today, so a regression would change the fixture and the expectation
together and assert nothing.
'''
import json
from fnmatch import fnmatch

import pytest

from tests.cli._affected_images import (
    affected, extra_closure, images_for_pyproject, module_for,
)
from tests.cli._image_deps import CLOSURE_DOC, REPO_ROOT
from tests.cli._roots import LEGACY_ROOT, ROOTS


CLOSURE = {'images': [
    {'image': 'bot', 'entrypoint': 'discord_bot.cli.bot', 'dockerfile': 'docker/Dockerfile',
     'extra': 'bot', 'modules': ['discord_bot', 'discord_bot.cogs.music', 'discord_bot.shared']},
    {'image': 'db', 'entrypoint': 'discord_bot.cli.database', 'dockerfile': 'docker/Dockerfile.db',
     'extra': 'db', 'modules': ['discord_bot', 'discord_bot.shared', 'discord_bot.store']},
]}

PYPROJECT = '''
[project]
name = "discord_bot"

[project.optional-dependencies]
storage = ["boto3==1.0.0"]
test = ["pylint==4.0.7"]
bot = ["discord_bot[storage]"]
db = ["sqlalchemy==2.0.0"]
'''


def _affected(files, head=PYPROJECT, base=PYPROJECT, base_closure=None, deleted=()):
    return affected(files, deleted, CLOSURE, base_closure, head, base)


@pytest.mark.parametrize('path, expected', [
    ('discord_bot/cogs/music.py', 'discord_bot.cogs.music'),
    ('discord_bot/__init__.py', 'discord_bot'),
    ('discord_bot/cogs/__init__.py', 'discord_bot.cogs'),
    ('tests/helpers.py', None),
    ('discord_bot/data.txt', None),
    ('README.md', None),
    # The post-split layout (criterion 7). These are not hypothetical spellings:
    # the folder classes are generated into docs/module-layout.md, and keeping
    # `discord_bot` as the import root is what lets one rule serve both layouts
    # while the tree is half-moved.
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
    ('discord_seam_dispatch/clients/http_dispatch_client.py',
     'discord_seam_dispatch.clients.http_dispatch_client'),
    ('discord_seam_queue_worker/workers/redis_guild_queue.py',
     'discord_seam_queue_worker.workers.redis_guild_queue'),
    ('discord_gateway/cogs/music.py', 'discord_gateway.cogs.music'),
    ('discord_db/database.py', 'discord_db.database'),
    ('discord_downloader/__init__.py', 'discord_downloader'),
    # An UNDECLARED root is not first-party. Recognising any `*/**.py` would
    # make scripts/foo.py an orphan and fail CI on a file that builds nothing.
    ('discord_notathing/x.py', None),
    ('discord_bot.py', None),
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
        deleted=['discord_bot/cogs/music.py'], base_closure=CLOSURE)
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
        ['discord_bot/cogs/music.py'], moved, CLOSURE, PYPROJECT, PYPROJECT)
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
    images, orphans = _affected(['README.md', 'docs/ha.md'])
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
        {'image': 'bot', 'entrypoint': 'discord_bot.cli.bot', 'dockerfile': 'docker/Dockerfile',
         'extra': 'bot', 'modules': ['discord_bot', 'discord_bot.gone']},
        {'image': 'db', 'entrypoint': 'discord_bot.cli.database', 'dockerfile': 'docker/Dockerfile.db',
         'extra': 'db', 'modules': ['discord_bot']},
    ]}
    images, orphans = _affected(['discord_bot/gone.py'], base_closure=base,
                                deleted=['discord_bot/gone.py'])
    assert not orphans, 'a deletion must not be reported as an unreachable module'
    assert images == {'bot'}


def test_dockerfile_change_builds_only_its_own_image():
    '''Each Dockerfile is an input to exactly one image.'''
    images, _ = _affected(['docker/Dockerfile.db'])
    assert images == {'db'}


def test_entrypoint_and_version_are_genuinely_all_six():
    '''Every image COPYs these, so all-six is correct rather than lazy.'''
    assert _affected(['docker/entrypoint.sh'])[0] == {'bot', 'db'}
    assert _affected(['VERSION'])[0] == {'bot', 'db'}


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


def test_test_only_dependency_bump_builds_nothing():
    '''
    A [test] bump changes no image, and used to rebuild and rescan all six.

    pylint is not installed by any image, so there is nothing for a build to
    pick up -- this is the single most common shape of dependency PR here.
    '''
    bumped = PYPROJECT.replace('pylint==4.0.7', 'pylint==4.0.8')
    images, _ = _affected(['pyproject.toml'], head=bumped)
    assert images == set()


def test_dependency_bump_reaches_only_the_images_whose_extras_pull_it():
    '''A [db] dependency is a db input; a [storage] one reaches the bot through its extra.'''
    db_bump = PYPROJECT.replace('sqlalchemy==2.0.0', 'sqlalchemy==2.0.1')
    assert _affected(['pyproject.toml'], head=db_bump)[0] == {'db'}
    storage_bump = PYPROJECT.replace('boto3==1.0.0', 'boto3==1.0.1')
    assert _affected(['pyproject.toml'], head=storage_bump)[0] == {'bot'}


def test_unparseable_base_pyproject_falls_back_to_every_image():
    '''
    Not being able to tell what changed is not evidence that nothing did.

    This is the branch that runs on a first commit, or when the base ref is
    unreachable -- exactly when guessing low would skip a real build.
    '''
    images = images_for_pyproject(PYPROJECT, 'not : valid ::: toml', {'bot': 'bot', 'db': 'db'})
    assert images == {'bot', 'db'}


def test_extra_closure_follows_self_references_and_survives_cycles():
    '''`discord_bot[...]` self-references are resolved transitively.'''
    extras = {'a': ['discord_bot[b]'], 'b': ['discord_bot[c]'], 'c': ['discord_bot[a]', 'x==1']}
    assert extra_closure(extras, 'a') == {'a', 'b', 'c'}


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
        assert 'discord_bot' in image['modules']


def test_every_declared_root_matches_release_yml_glob():
    """`release.yml`'s `case` glob has to recognise every declared root.

    It is a single boolean gating all six pushes, and a path it does not match
    sets `image=false`: no release build, no error, nothing published. That is
    the 2026-09-04 finding's shape -- a green run that shipped nothing for four
    days because nothing was looking -- and it is the one path-keyed reader in
    this repo whose failure mode is silence rather than an orphan.

    Asserted against the pattern in the workflow rather than against a copy of
    it here, so the two cannot drift.
    """
    workflow = (REPO_ROOT / '.github/workflows/release.yml').read_text(encoding='utf-8')
    patterns = [line.strip().rstrip(')').strip()
                for line in workflow.splitlines()
                if line.strip().endswith(')') and 'pyproject.toml|VERSION' in line]
    assert len(patterns) == 1, f'expected one image-input case arm, found {patterns}'
    arms = patterns[0].split('|')
    assert len(arms) > 3, f'the case arm stopped looking like a glob list: {arms}'
    for root in sorted(ROOTS):
        path = f'{root}/anything.py'
        assert any(fnmatch(path, arm) for arm in arms), (
            f'release.yml would not build for a change to {path}.\n'
            f'  case arms: {arms}\n'
            'A root the release filter does not match publishes nothing, silently.'
        )


def test_the_declared_roots_are_exactly_the_packages_on_disk():
    """Every import root in the tree is declared, and every declared one is real
    or still to come.

    This is what keeps `module_for` honest. It recognises a fixed set rather
    than any top-level directory, so a root that appears on disk WITHOUT being
    declared is not first-party to the filter -- its files contribute no images
    and build nothing. Equality in the on-disk direction is the check that
    cannot be satisfied by the migration quietly not happening.

    The other direction is deliberately NOT equality while the migration runs:
    twelve of the thirteen roots do not exist yet, which is the point of
    declaring them a step early.
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
    assert LEGACY_ROOT in on_disk, 'the legacy root vanished -- this test stopped checking anything'


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
        'coverage': [line for line in tox.splitlines() if '--cov=' in line],
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
