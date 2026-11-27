'''
A package's own tests may use itself, the pinned `discord_core`, and the shared test infrastructure under
`tests/` (the fakes and helpers). They may not import another pod's production code.

Why it matters. `discord_core` is the independent, released artifact the pods depend on, so its tests may
not depend on a pod at all. And a pod's test tree that quietly imports a sibling pod is not testing the pod:
it is an integration test sitting in the wrong place, which makes the pod's tests depend on code that is not
its own, widens which PRs have to run them, and hides what the seam between the two actually is. The one
tree that is allowed to import several pods is the central `tests/`, and what belongs there is exactly the
tests that need more than one.

No package gets an exception: core's tests import no pod, and neither do any pod's. A test that needs two pods
lives in the central `tests/` (`tests/integration/`, one directory per seam).

What counts as an import: a real `import` / `from ... import`, and a dotted string naming a pod's module
(the target of a `patch(...)`), the same two things `tests/cli/_affected_tests.py` follows, so the two cannot
disagree about what a test depends on.
'''
from tests.cli._affected_tests import referenced_modules
from tests.cli._git_diff import REPO_ROOT
from tests.cli._roots import CORE_ROOT, SERVICE_ROOTS

PODS = frozenset(SERVICE_ROOTS.values())


def cross_pod_imports(repo_root=REPO_ROOT) -> dict:
    '''Every test file under a package's own tests/ that names another pod, mapped to the pods it names.'''
    found = {}
    for package in (CORE_ROOT, *sorted(PODS)):
        for path in sorted((repo_root / package / 'tests').rglob('*.py')):
            relative = path.relative_to(repo_root).as_posix()
            modules = referenced_modules(path.read_text(encoding='utf-8'), '.'.join(relative.split('/')[:-1]))
            others = ({module.split('.')[0] for module in modules} & PODS) - {package}
            if others:
                found[relative] = frozenset(others)
    return found


def test_no_package_test_imports_another_pod():
    '''A cross-pod import in a package's tests is a test in the wrong tree: put it in tests/integration/, or, if it
    only needs a model class or a patch target, reach it through the shared helpers (tests/db_helpers.py,
    tests/fakes/in_process_stack.py).'''
    found = cross_pod_imports()
    assert not found, f'package tests that import another pod: {found}'


def test_core_tests_import_no_pod():
    '''Said on its own because it is the one that points a dependency the wrong way: core is the artifact the pods
    depend on.'''
    found = {path: pods for path, pods in cross_pod_imports().items() if path.startswith(f'{CORE_ROOT}/')}
    assert not found, f'discord_core tests must not import a pod: {found}'


def _write(root, relative, text):
    target = root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding='utf-8')


def test_the_scan_sees_an_import_a_patch_target_and_core(tmp_path):
    '''The guards above are vacuous if the scan cannot find these, and wrong if it flags the rest.'''
    _write(tmp_path, 'discord_gateway/tests/test_import.py', 'from discord_db.database import BASE\n')
    _write(tmp_path, 'discord_gateway/tests/test_patch.py',
           'def test_x(mocker):\n    mocker.patch("discord_downloader.interfaces.download_protocols.sleep")\n')
    _write(tmp_path, 'discord_core/tests/test_core.py', 'from discord_dispatcher.utils.retry import x\n')
    _write(tmp_path, 'discord_gateway/tests/test_fine.py',
           'from discord_core.utils.common import y\nfrom discord_gateway.cogs.music import z\n'
           'from tests.helpers import fake_context\n')
    _write(tmp_path, 'discord_db/tests/test_own.py', 'from discord_db.database import BASE\n')
    found = cross_pod_imports(tmp_path)
    assert found == {
        'discord_gateway/tests/test_import.py': frozenset({'discord_db'}),
        'discord_gateway/tests/test_patch.py': frozenset({'discord_downloader'}),
        'discord_core/tests/test_core.py': frozenset({'discord_dispatcher'}),
    }
