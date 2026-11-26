'''
A package's own tests may use itself, the pinned `discord_core`, and the shared test infrastructure under
`tests/` (the fakes and helpers). They may not import another pod's production code.

Why it matters. `discord_core` is the independent, released artifact the pods depend on, so its tests may
not depend on a pod at all. And a pod's test tree that quietly imports a sibling pod is not testing the pod:
it is an integration test sitting in the wrong place, which makes the pod's tests depend on code that is not
its own, widens which PRs have to run them, and hides what the seam between the two actually is. The one
tree that is allowed to import several pods is the central `tests/`, and what belongs there is exactly the
tests that need more than one.

Core gets no exceptions. The pods have a list of the integration tests that have not been moved yet
(`KNOWN_CROSS_POD_TESTS`); it may only shrink. A new entry means a new test was put in the wrong tree. An
entry that no longer violates must be deleted, which is what makes the list converge.

What counts as an import: a real `import` / `from ... import`, and a dotted string naming a pod's module
(the target of a `patch(...)`), the same two things `tests/cli/_affected_tests.py` follows, so the two cannot
disagree about what a test depends on.
'''
from tests.cli._affected_tests import referenced_modules
from tests.cli._git_diff import REPO_ROOT
from tests.cli._roots import CORE_ROOT, SERVICE_ROOTS

PODS = frozenset(SERVICE_ROOTS.values())

#: Integration tests that still live in a pod's own tree. Each is a test of a seam between two pods
#: (a client against another pod's real server, or a pod's logic over another pod's real code) and belongs in
#: the central tests/. Move one, delete its line.
KNOWN_CROSS_POD_TESTS = {
    'discord_commands/tests/clients/test_http_download_client.py': frozenset({'discord_downloader'}),
    'discord_commands/tests/clients/test_http_guild_analytics_store.py': frozenset({'discord_broker', 'discord_db'}),
    'discord_commands/tests/clients/test_http_markov_store.py': frozenset({'discord_db'}),
    'discord_commands/tests/clients/test_http_media_search_client.py': frozenset({'discord_search'}),
    'discord_commands/tests/clients/test_http_playlist_store.py': frozenset({'discord_broker', 'discord_db'}),
    'discord_commands/tests/clients/test_http_youtube_music_search_client.py': frozenset({'discord_search'}),
    'discord_commands/tests/clients/test_search_client.py': frozenset({'discord_search'}),
    'discord_commands/tests/command_logic/test_add_playlist_item.py': frozenset({'discord_db'}),
    'discord_commands/tests/command_logic/test_markov.py': frozenset({'discord_db'}),
    'discord_commands/tests/command_logic/test_playlist.py': frozenset({'discord_db'}),
    'discord_commands/tests/loop_logic/test_download_backoff.py': frozenset({'discord_downloader'}),
    'discord_commands/tests/loop_logic/test_playlist_results.py': frozenset({'discord_db'}),
    'discord_commands/tests/loop_logic/test_search_driver.py': frozenset({'discord_search'}),
    'discord_gateway/tests/cogs/music/test_cache_cleanup.py': frozenset({'discord_broker', 'discord_db',
                                                                         'discord_downloader'}),
    'discord_gateway/tests/cogs/music/test_resume_sessions.py': frozenset({'discord_broker'}),
    'discord_gateway/tests/cogs/test_music.py': frozenset({'discord_downloader'}),
}


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


def test_core_tests_import_no_pod():
    '''Core is the artifact the pods depend on; its own tests depending on a pod points the dependency
    backwards. No exceptions, no list.'''
    found = {path: pods for path, pods in cross_pod_imports().items() if path.startswith(f'{CORE_ROOT}/')}
    assert not found, f'discord_core tests must not import a pod: {found}'


def test_no_package_test_imports_another_pod_beyond_the_known_list():
    '''A new cross-pod import in a pod's tests is a test in the wrong tree: put it in the central tests/.'''
    found = cross_pod_imports()
    new = {path: sorted(pods - KNOWN_CROSS_POD_TESTS.get(path, frozenset())) for path, pods in found.items()
           if pods - KNOWN_CROSS_POD_TESTS.get(path, frozenset())}
    assert not new, f'tests that import another pod (move them to the central tests/): {new}'


def test_the_known_cross_pod_list_has_no_stale_entries():
    '''Once a test stops importing the other pod (or moves), its line has to go, or the list stops shrinking.'''
    found = cross_pod_imports()
    stale = {path: sorted(pods - found.get(path, frozenset())) for path, pods in KNOWN_CROSS_POD_TESTS.items()
             if pods - found.get(path, frozenset())}
    assert not stale, f'KNOWN_CROSS_POD_TESTS lists imports that are gone, delete them: {stale}'


def test_the_known_list_only_names_real_pods_and_files():
    '''A typo would silently excuse nothing, or everything.'''
    for path, pods in KNOWN_CROSS_POD_TESTS.items():
        assert (REPO_ROOT / path).is_file(), f'{path} does not exist'
        assert pods <= PODS, f'{path} names {sorted(pods - PODS)}, which is not a pod'
        assert not path.startswith(f'{CORE_ROOT}/'), 'core has no exceptions'


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
