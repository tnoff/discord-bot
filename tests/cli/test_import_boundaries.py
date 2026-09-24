'''
Per-image import boundaries — what each published image is allowed to import.

Each entry in ``IMAGE_IMPORTS`` (tests/cli/_image_deps.py) is a contract: importing
that image's entrypoint must pull EXACTLY those tier-defining packages into
``sys.modules``. The check is the enforcement mechanism for the per-image
dependency split (projects/discord-bot-ha-only) — a folder layout *on its own*
cannot prevent ``from discord_bot.utils.integrations.youtube_music import ...``,
but this can, and this is what caught the ytmusicapi leak during the search-pod
work before it could CrashLoop a pod (see reference_slim_pod_import_chain_leak).

"On its own" is a qualification criterion 7 step 6 earned. A folder plus a test
comparing it to the measured closure does now constrain first-party imports --
see test_every_module_lives_where_its_closure_says. The two checks are still
different facts and neither subsumes the other: this one is about THIRD-PARTY
packages an image must not install, that one is about FIRST-PARTY modules an
image must not reach. A leak of the first kind is an ImportError at pod start; a
leak of the second kind runs fine and quietly rebuilds an image it should not.

Two things make these tests worth their weight:

- **A violation is a pod-start crash, not a test failure.** The slim images
  install only their own extra, so an import that reaches a package the image
  does not ship is an ImportError at startup — discovered in prod, on a rollout.
- **They are the discovery tool for the extras split.** The gap between what an
  image *imports* and what its extra *installs* is what the split acts on.

The assertion is EQUALITY, not absence, and that is deliberate. A forbidden-list
catches a leak — a package arriving where it should not be. It cannot catch the
opposite, an extra that has gone over-broad because the code that needed it was
deleted. That failure is silent, it costs image size rather than uptime, and it
is exactly how yt_dlp stayed in [bot] after the download dual path was collapsed
and how moviepy stayed installed while nothing imported it at all. Declaring what
each image DOES import catches both directions from one list.

There is deliberately no separate "forbidden packages" test. Equality already
implies it: if what an image imports equals what it declares, it cannot have
imported anything in ``VOCABULARY`` that it did not declare. A second test
asserting that would be tautological, and a tautological test is worse than no
test — it reads as coverage while checking nothing.
'''
import json
import os
import re

import pytest

from tests.cli._image_deps import (
    ALL_SIX, CLOSURE_DOC, IMAGE_DOCKERFILES, IMAGE_IMPORTS, IMAGE_NAMES, LAYOUT_DOC, NOT_YET_SPLIT,
    OWNERSHIP_DOC, REPO_ROOT, PACKAGE, SEAMS_DIR, VOCABULARY,
    codeless_inits, declared_home, layout_violations, measure, measured_owners,
    render_closure, route_leaf, render_layout, render_table,
)


@pytest.mark.parametrize('entrypoint', sorted(IMAGE_IMPORTS))
def test_image_imports_exactly_its_declared_packages(entrypoint):
    '''An image imports its declared packages — no more, and no fewer.'''
    declared = IMAGE_IMPORTS[entrypoint]
    imported = set(measure(entrypoint)['packages'])
    leaked = imported - declared
    unused = declared - imported
    assert not leaked, (
        f'{entrypoint} imported {sorted(leaked)}, which it does not declare and its '
        f'image may not install. On a slim image this is an ImportError at pod start, '
        f'not a test failure. Find the chain with: python -c "import {entrypoint}" '
        f'under a tracing __import__ hook, then split the light type out of the heavy '
        f'module — the same move as CheckoutResult, ClearGuildResult and BrokerClient.'
    )
    assert not unused, (
        f'{entrypoint} no longer imports {sorted(unused)}, which it still declares. '
        f'Nothing is broken, but its extra in pyproject is now shipping a package '
        f'nothing reaches — drop it from both, the way yt_dlp and moviepy should have '
        f'been dropped when the code that used them went away.'
    )


def test_vocabulary_covers_every_declaration():
    '''Nothing is declared that the vocabulary does not know about.'''
    declared = set().union(*IMAGE_IMPORTS.values())
    unknown = declared - set(VOCABULARY)
    assert not unknown, (
        f'{sorted(unknown)} declared but missing from VOCABULARY, so the derived '
        f'forbidden set would silently ignore them on every other image.'
    )


def test_extra_names_are_normalised_and_self_references_resolve():
    '''
    Every extra name is PEP 685 normalised, and every self-reference names a real one.

    This is a silence guard, not a style check. PEP 685 normalises extra names, so
    an extra declared as ``search_providers`` is published in the metadata as
    ``search-providers``. A self-reference written with the underscore then
    resolves to NOTHING — it installs no packages, and raises no error.

    That is not hypothetical: ``[bot]`` and ``[search]`` composed
    ``search_providers`` and ``youtube_music``, and the tox environment came up
    with no spotipy, no google-api-python-client, no beautifulsoup4 and no
    ytmusicapi while every hyphen-free group resolved correctly. The suite failed
    at collection, several layers away from the cause.
    '''
    import tomllib  # pylint: disable=import-outside-toplevel
    pyproject = tomllib.loads((REPO_ROOT / 'pyproject.toml').read_text(encoding='utf-8'))
    extras = pyproject['project']['optional-dependencies']

    unnormalised = sorted(name for name in extras
                          if name != re.sub(r'[-_.]+', '-', name).lower())
    assert not unnormalised, (
        f'extras {unnormalised} are not PEP 685 normalised. Publishing normalises '
        f'them anyway, so any self-reference using this spelling silently resolves '
        f'to nothing. Rename them with hyphens.'
    )

    dangling = []
    for name, specs in extras.items():
        for spec in specs:
            match = re.match(r'^discord_bot\[([^\]]+)\]$', spec.strip())
            if not match:
                continue
            for referenced in match.group(1).split(','):
                if referenced.strip() not in extras:
                    dangling.append(f'[{name}] -> [{referenced.strip()}]')
    assert not dangling, (
        f'self-references naming extras that do not exist: {dangling}. These install '
        f'nothing and fail silently — the package only goes missing wherever that '
        f'extra was the sole route to it.'
    )


def test_every_module_lives_where_its_closure_says():
    """
    A module's folder is a declaration, and the closure has to agree with it.

    This is criterion 7 step 6, and it is the first version of this check with
    any content in it. While homes were DERIVED from the closure -- steps 1
    through 5 -- a test comparing the two could not fail: the layout was computed
    from what imports what, so it agreed with what imports what by construction.
    That is the "passes because it no longer finds anything to check" shape, and
    it passed green for five steps while checking nothing.

    Now the folder comes off the tree and the closure comes off a live import, so
    the two are independent facts and can contradict each other. A module in
    ``services/bot/`` that the downloader starts reaching is a contradiction. So
    is one in ``core/`` that only five images reach.

    This REPLACES ``BOT_FORBIDDEN_MODULES``, which was the hand-maintained
    version of exactly this rule, narrowed to one importer and one module. That
    tuple's last entry was ``services/broker/servers/broker_server``, and the
    rule above covers it strictly more tightly: the tuple asserted the bot does
    not import it, while this asserts that nothing but the broker does, for every
    module in every service folder, without anyone maintaining a list. The
    diagnostic that made the tuple worth having is kept in the failure message --
    an in-process tier coming back, or an annotation against a concrete type
    where the client Protocol was meant.
    """
    owners = measured_owners()
    exempt = codeless_inits(owners)

    # Both populations are asserted before the rule runs, because this check is
    # two filters deep and either one swallowing everything would leave it
    # passing while looking at nothing -- the exact failure it was written to
    # end. If the codeless-init detector ever matched every module, or every
    # module lost its home, the assertion below would still be green.
    assert exempt, 'the codeless-init detector matched nothing -- it is broken, not the tree'
    checked = [m for m in owners if m not in exempt and declared_home(m)]
    assert checked, 'no homed module survived the exemption -- this test checked nothing'

    violations = layout_violations(owners, exempt=exempt)
    assert not violations, (
        'the tree and the closure disagree about where code lives:\n  '
        + '\n  '.join(violations)
    )


def test_a_misplaced_module_is_actually_caught():
    """
    The declared check reports a contradiction that is really there.

    A rule whose whole purpose is to be able to fail has to be shown failing,
    and against a constructed closure rather than the real one -- the real one
    passes, which is the outcome that proves nothing. Each case below is one of
    the three rules, with the violation built by hand.

    This test is the reason ``layout_violations`` takes its owner map as an
    argument instead of measuring one itself. A check that can only be run
    against the live tree can only be observed passing.
    """
    images = sorted(short for short in (n.replace('discord-', '') for n in IMAGE_NAMES.values()))
    assert len(images) == 6, f'expected six images, found {images}'
    all_six = set(images)

    # A core module reached by ONE image. Five-of-six used to be the violation
    # here; it is deliberately legal now -- core means shared, not all-six, and
    # ALL_SIX is what guards the fanout instead. See
    # test_the_all_six_set_is_exactly_declared.
    cases = {
        f'{PACKAGE}.core.thing': {'bot'},
        f'{PACKAGE}.services.bot.thing': {'bot', 'downloader'},
        f'{PACKAGE}.seams.database.thing': all_six,
        f'{PACKAGE}.seams.database.other': {'bot'},
    }
    for module, reached in cases.items():
        found = layout_violations({module: reached})
        assert len(found) == 1, f'{module} reached by {sorted(reached)} produced {found}'
        assert module in found[0]

    # And the same modules, placed consistently, produce nothing -- otherwise the
    # check above would pass by reporting everything.
    clean = {
        f'{PACKAGE}.core.thing': all_six,
        f'{PACKAGE}.core.subset_shared': set(images[:4]),   # legal under the new rule
        f'{PACKAGE}.services.bot.thing': {'bot'},
        f'{PACKAGE}.seams.database.thing': {'bot', 'db'},
        f'{PACKAGE}.unsplit.thing': {'bot', 'db'},
    }
    assert not layout_violations(clean)


def test_the_unsplit_packages_are_exactly_declared():
    """
    ``NOT_YET_SPLIT`` names every top-level package criterion 7 has not homed.

    Equality, not containment, and for the same reason the import boundaries
    above are equality: containment catches a new flat package arriving but not a
    finished one still listed. The second is how a list stops meaning anything --
    it reads as remaining work long after the work is done, and nothing says so.

    The check is at package granularity on purpose. A new module under ``utils/``
    is honest, because ``utils/`` has no home yet either. A new top-level package
    is the split running backwards, and that is what this fails on.
    """
    owners = measured_owners()
    exempt = codeless_inits(owners)
    unsplit = {m.split('.')[1] for m in owners
               if m not in exempt and declared_home(m) is None}
    assert unsplit == set(NOT_YET_SPLIT), (
        f'NOT_YET_SPLIT and the tree disagree: {sorted(unsplit ^ set(NOT_YET_SPLIT))}. '
        f'Added a top-level package under {PACKAGE}/? Give it a home instead. '
        f'Finished emptying one? Drop it from NOT_YET_SPLIT, so the list keeps '
        f'shrinking rather than carrying an entry nothing is left in.'
    )


def test_every_published_image_has_a_boundary():
    '''
    Every console script that ships as an image is covered above.

    Without this, adding a sixth image silently gets no boundary at all — the
    failure mode being guarded against is an image nobody thought to guard.

    The published set is read from pyproject rather than restated here. It used
    to be a hardcoded literal, which meant it agreed with ``[project.scripts]``
    only for as long as someone kept both in step by hand — and a stale copy
    passes just as green as a correct one.
    '''
    import tomllib  # pylint: disable=import-outside-toplevel
    pyproject = tomllib.loads((REPO_ROOT / 'pyproject.toml').read_text(encoding='utf-8'))
    published = {target.split(':', 1)[0]
                 for target in pyproject['project']['scripts'].values()}
    assert set(IMAGE_IMPORTS) == published, (
        f'console scripts and image boundaries disagree: '
        f'{published ^ set(IMAGE_IMPORTS)}. Every published script ships as an image, '
        f'so every one needs a boundary above.'
    )


def test_ownership_doc_is_current():
    '''
    docs/image-dependencies.md matches a live measurement.

    The doc is the human-readable answer to "what is used strictly by what". It is
    generated rather than written so it cannot drift into being confidently wrong,
    which is the failure mode this project keeps finding in restated facts.
    '''
    rendered = render_table()
    if os.environ.get('UPDATE_IMAGE_DEPS'):
        OWNERSHIP_DOC.write_text(rendered, encoding='utf-8')
    assert OWNERSHIP_DOC.read_text(encoding='utf-8') == rendered, (
        'docs/image-dependencies.md is out of date. Regenerate with:\n'
        '    UPDATE_IMAGE_DEPS=1 pytest tests/cli/test_import_boundaries.py'
    )


def test_layout_doc_is_current():
    '''
    docs/module-layout.md matches a live measurement.

    This test's SCOPE has not changed and its docstring's caveat has. Through
    steps 1 to 5 it carried a warning that the doc could not fail on a module
    living in the wrong place, because homes were derived from the closure and
    so agreed with it by construction. Step 6 removed the reason for the warning
    rather than the warning itself, and it is deleted here rather than softened:
    a stale caveat is the same failure as a stale figure, and it is worse for
    telling a reader not to trust a check that now works.

    What this test catches is still only the doc going stale. The check with
    teeth is test_every_module_lives_where_its_closure_says; this one keeps the
    rendering of it honest, which is the failure the hand-copied figures in the
    spec have already hit twice.
    '''
    rendered = render_layout()
    if os.environ.get('UPDATE_IMAGE_DEPS'):
        LAYOUT_DOC.write_text(rendered, encoding='utf-8')
    assert LAYOUT_DOC.read_text(encoding='utf-8') == rendered, (
        'docs/module-layout.md is out of date. Regenerate with:\n'
        '    UPDATE_IMAGE_DEPS=1 pytest tests/cli/test_import_boundaries.py'
    )




def test_seam_folders_are_named_by_exactly_one_route():
    '''
    Every seam folder on disk holds exactly the one route module that names it.

    This used to run over the DERIVED groups, where it was a check on the naming
    rule. It runs over the tree now, where it is a check on the tree: a seam
    folder is a contract, the route module IS the contract, and a folder holding
    two routes or none has a name that came from somewhere other than what it
    contains.

    The checked count is asserted because this test is a filter, and a filter
    that matches nothing passes while checking nothing. That is how the first
    version went quiet when the folder prefix changed -- twice, in two different
    files, which is why the rule is structural now rather than a string prefix.
    '''
    owners = measured_owners()
    seams = {}
    for module in owners:
        home = declared_home(module)
        if home and home[0] == SEAMS_DIR:
            seams.setdefault(home[1], []).append(module)
    assert seams, f'no {SEAMS_DIR}/ folders found on disk -- this test checked nothing'

    for seam, modules in sorted(seams.items()):
        routes = sorted(m for m in modules if route_leaf(m))
        assert len(routes) == 1, (
            f'{PACKAGE}/{SEAMS_DIR}/{seam}/ holds {len(routes)} route modules, not one: '
            f'{routes}. A seam is named by its contract; two contracts are two seams.'
        )
        assert route_leaf(routes[0]) == seam, (
            f'{PACKAGE}/{SEAMS_DIR}/{seam}/ is named {seam!r} but its route is '
            f'{routes[0]!r}. Rename the folder to follow the route, not the other way.'
        )


# The only image whose Dockerfile may COPY the migration scripts. Declared, not
# derived, because the point is that changing it takes a deliberate edit.
MIGRATION_IMAGE_DOCKERFILES = frozenset({'Dockerfile.db'})


def test_only_the_db_image_ships_the_migrations():
    """A Dockerfile may COPY alembic/ only if its image can run it.

    This exists because the property broke once already and nothing noticed:
    #917 removed the migration COPYs from the dispatcher, then the MR 4 cutover
    took `[database]` out of `[bot]` and `[broker]` -- dropping the alembic CLI
    from both images while their COPY lines stayed. An image shipping scripts it
    cannot execute is a lie about its own capability, and it was found by
    reading Dockerfiles by hand, weeks later.

    The import boundary above cannot catch it: whether a package is IMPORTED and
    whether files are COPYd are different facts, and this one lives in the
    Dockerfile rather than in the source tree.
    """
    dockerfiles = sorted((REPO_ROOT / 'docker').glob('Dockerfile*'))
    assert dockerfiles, 'no Dockerfiles found -- the glob is wrong, not the repo'

    ships = set()
    for path in dockerfiles:
        copies = [
            line for line in path.read_text(encoding='utf-8').splitlines()
            if line.startswith('COPY') and 'alembic' in line
        ]
        if copies:
            ships.add(path.name)

    assert ships == set(MIGRATION_IMAGE_DOCKERFILES), (
        f'{sorted(ships)} ship migration files, expected '
        f'{sorted(MIGRATION_IMAGE_DOCKERFILES)}. An image that COPYs alembic/ '
        'without installing the alembic CLI cannot run what it carries; one that '
        'installs it without the files has nothing to run. Change the constant '
        'above only when an image genuinely gains or loses schema ownership.'
    )


def test_every_dockerfile_exists():
    '''
    Every declared Dockerfile is on disk, and every image has exactly one.

    ci.yml's build matrix is generated from IMAGE_DOCKERFILES now, so a typo here
    no longer fails loudly at build time -- it produces a matrix leg pointing at a
    path that does not exist, which is a build failure with a confusing message,
    or worse a missing leg. Checking against the filesystem is cheap and the
    declaration is the only place the mapping is written down.
    '''
    assert set(IMAGE_DOCKERFILES) == set(IMAGE_IMPORTS), (
        f'images and dockerfiles disagree: {set(IMAGE_DOCKERFILES) ^ set(IMAGE_IMPORTS)}'
    )
    missing = sorted(path for path in IMAGE_DOCKERFILES.values()
                     if not (REPO_ROOT / path).is_file())
    assert not missing, f'declared Dockerfiles that do not exist: {missing}'
    duplicated = len(set(IMAGE_DOCKERFILES.values())) != len(IMAGE_DOCKERFILES)
    assert not duplicated, (
        f'two images share a Dockerfile: {sorted(IMAGE_DOCKERFILES.values())}. '
        f'The build matrix keys on it, so they would build the same thing twice.'
    )


def test_image_closure_is_current():
    '''
    docs/image-closure.json matches a live measurement.

    This one is load-bearing in a way the markdown table is not: ci.yml reads it
    to decide which images to build, so a stale copy does not merely misinform a
    reader, it skips a build. A module that moved into the bot's import chain
    since the last regeneration would be seen as not affecting the bot.
    '''
    rendered = render_closure()
    if os.environ.get('UPDATE_IMAGE_DEPS'):
        CLOSURE_DOC.write_text(rendered, encoding='utf-8')
    assert CLOSURE_DOC.read_text(encoding='utf-8') == rendered, (
        'docs/image-closure.json is out of date, and CI keys its build filter on it.\n'
        'Regenerate with:\n'
        '    UPDATE_IMAGE_DEPS=1 pytest tests/cli/test_import_boundaries.py'
    )


def test_every_module_is_claimed_by_some_image():
    '''
    No first-party module is reachable from no entrypoint.

    This is the property the CI filter depends on: if a file belongs to no
    image's closure, "which images does this change affect" has no safe answer,
    and the filter fails the build rather than guessing. Asserting it here means
    the answer arrives when the unreachable module is added, not weeks later on
    an unrelated PR that happens to touch it.
    '''
    closure = json.loads(CLOSURE_DOC.read_text(encoding='utf-8'))
    claimed = set().union(*(set(image['modules']) for image in closure['images']))
    orphans = []
    for path in sorted((REPO_ROOT / 'discord_bot').rglob('*.py')):
        module = str(path.relative_to(REPO_ROOT).with_suffix('')).replace('/', '.')
        if module.endswith('.__init__'):
            module = module[: -len('.__init__')]
        if module not in claimed:
            orphans.append(module)
    assert not orphans, (
        f'modules no entrypoint reaches: {orphans}. They ship in every image and no '
        f'test exercises them through a real import chain. Delete them, or move them '
        f'under tests/ if they are doubles.'
    )


def test_the_all_six_set_is_exactly_declared():
    '''
    The fanout guard that replaced `core means all six`.

    Placement and fanout used to be the same rule. Homing the last 25 modules
    separated them: core now means SHARED, so a four-image module can live there
    legitimately, and nothing in the placement rules notices a module becoming
    all-six any more.

    That mattered, because the all-six set is the one that forces six-image
    rebuilds, and this project has already watched a module drift into it
    unnoticed -- `utils/otel.py`, which took criterion 5 to get back out.

    Equality, not containment, so it fails in both directions: a module arriving
    is a new all-six dependency that should be argued for, and one leaving is
    usually good news that still should not be discovered from a graph weeks
    later.
    '''
    owners = measured_owners()
    exempt = codeless_inits(owners)
    measured = {m for m, images in owners.items()
                if len(images) == len(IMAGE_NAMES) and m not in exempt}
    assert measured, 'nothing is reached by all six -- the measurement is broken, not the tree'
    assert measured == set(ALL_SIX), (
        'the all-six set moved:\n'
        f'  arrived: {sorted(measured - set(ALL_SIX))}\n'
        f'  left:    {sorted(set(ALL_SIX) - measured)}\n'
        'Every module here rebuilds all six images on every change. Update '
        'ALL_SIX deliberately, not to make this pass.'
    )
