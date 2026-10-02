'''
Enforce that every pod pins `discord_core` to the exact same released tag.

Criterion 8 step 7 gave each of the six pods (`discord_gateway`, `discord_broker`,
`discord_db`, `discord_dispatcher`, `discord_downloader`, `discord_search`) a
real, declared dependency on `discord_core` in its own `pyproject.toml`:

    "discord-core @ https://github.com/tnoff/discord-bot/archive/refs/tags/core-vX.Y.Z.tar.gz#subdirectory=discord_core"

-- the same shape `dappertable` already uses, pinned to a tagged archive
rather than installed from the local checkout path. See
`per-image-code-split.md` criterion 8 step 7 and the `discord_core` note in
tox.ini's `[testenv]` for why: a pod can no longer be `-e` installed alongside
an editable `discord_core` in the same resolve (pip refuses with
`ResolutionImpossible` -- two different sources claiming to satisfy the same
project name), so every pod now resolves `discord_core` from this pin
instead, in every context: a plain `pip install`, a Docker build and a real
release push alike.

NOTHING enforces the SIX pods staying in step with EACH OTHER except this
module. Nothing stops a PR from bumping one pod's pin to a newer `core-vX.Y.Z`
tag without bumping the rest -- pip would happily install six different pods
each pointed at a different `discord_core` release, and nothing downstream
would notice until two of them disagreed about `discord_core`'s own behaviour
in production. So this module reads all six pods' own pyproject.toml files
directly (standard library `tomllib`, the same precedent
`_image_deps.declared_dependencies` already reads `pyproject.toml` with) and
asserts the six `discord-core @ ...` entries are byte-identical.

Run by ci.yml's "Detect discord_core pin drift" job, which has no installed
package and no dependencies -- so, like `_affected_images.py` and
`_package_versions.py`, everything here is standard library:

    python3 -m tests.cli._package_pins

NO VERSION-COMPARISON LOGIC
----------------------------
This module does not parse `core-vX.Y.Z` into a version tuple and compare
magnitudes -- it only asserts the six raw dependency STRINGS are identical.
A pod pinned to an OLDER tag than the others is exactly as wrong as one
pinned to a newer one or to a different package name entirely: the property
this enforces is agreement, not recency. Bumping the shared pin forward is a
separate, coordinated edit this check does not prescribe -- it only refuses
to let that edit land half-done.
'''
import argparse
import sys
from pathlib import Path

from tests.cli._roots import CORE_ROOT, SERVICE_ROOTS

REPO_ROOT = Path(__file__).resolve().parents[2]

#: Every pod this convention covers. CORE_ROOT itself is excluded -- there is
#: nothing for discord_core to pin against its own name, and its
#: pyproject.toml declares no such dependency at all.
POD_ROOTS = dict(SERVICE_ROOTS.items())


def _pin_prefix() -> str:
    '''The project-name prefix every pod's `discord-core` dependency entry
    must start with, read off CORE_ROOT's own `pyproject.toml`.

    Reading the declared name rather than hardcoding `discord-core` is what
    keeps this check from silently going blind if that package is ever
    renamed -- the same precision `_package_versions.PACKAGE_ROOTS` borrows
    `_roots.py`'s declarations for instead of a second hand-rolled table.
    '''
    import tomllib  # pylint: disable=import-outside-toplevel
    candidate = REPO_ROOT / CORE_ROOT / 'pyproject.toml'
    declared = tomllib.loads(candidate.read_text(encoding='utf-8'))
    name = declared.get('project', {}).get('name')
    if not name:
        raise ValueError(f'{candidate} declares no [project] name')
    return name


def declared_core_pin(root: str) -> str | None:
    '''The `discord-core @ ...` entry in `root`'s own `dependencies` list, or
    None if that pod declares no such pin at all.

    Reads `pyproject.toml` with `tomllib` directly, the same precedent
    `_image_deps.declared_dependencies` already set, rather than installing
    the package and inspecting `importlib.metadata` -- this has to run with
    no installed package and no dependencies, exactly like
    `_package_versions.py` and `_affected_images.py`.
    '''
    import tomllib  # pylint: disable=import-outside-toplevel
    candidate = REPO_ROOT / root / 'pyproject.toml'
    declared = tomllib.loads(candidate.read_text(encoding='utf-8'))
    deps = declared.get('project', {}).get('dependencies', [])
    prefix = f'{_pin_prefix()} @ '
    matches = [dep for dep in deps if dep.startswith(prefix)]
    if not matches:
        return None
    # A pod declaring the same project name twice is its own, different bug
    # (pip would already refuse this at install time); asserting exactly one
    # here keeps that failure loud rather than silently picking the first.
    if len(matches) > 1:
        raise ValueError(
            f'{candidate} declares discord-core more than once: {matches}')
    return matches[0]


def collect_pins() -> dict:
    '''Every pod root -> its declared `discord-core` pin (or None if missing).'''
    return {root: declared_core_pin(root) for root in sorted(POD_ROOTS.values())}


def find_mismatches(pins: dict) -> dict:
    '''The pods that disagree with the majority pin, mapped to what they have.

    "Majority" rather than "the first one found" so the error message names
    the actual minority -- the pods that are wrong, not however many happen to
    sort first. A tie (three and three) reports every pod as a mismatch of
    every other, which is the honest answer: there is no majority to defer to.
    '''
    present = {root: pin for root, pin in pins.items() if pin is not None}
    missing = sorted(root for root, pin in pins.items() if pin is None)
    if not present:
        return {root: None for root in missing}

    counts: dict = {}
    for pin in present.values():
        counts[pin] = counts.get(pin, 0) + 1
    majority_pin = max(counts, key=counts.get)

    mismatches = {root: pin for root, pin in present.items() if pin != majority_pin}
    mismatches.update({root: None for root in missing})
    return mismatches


def main(argv=None) -> int:
    '''Fail if the six pods' `discord-core` pins are not byte-identical.'''
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args(argv)

    pins = collect_pins()
    print('declared discord-core pins:', file=sys.stderr)
    for root in sorted(pins):
        print(f'  {root}: {pins[root]!r}', file=sys.stderr)

    mismatches = find_mismatches(pins)
    if mismatches:
        print('\nERROR: these pods do not agree on a discord-core pin:', file=sys.stderr)
        for root in sorted(mismatches):
            pin = mismatches[root]
            shown = 'no discord-core dependency declared at all' if pin is None else repr(pin)
            print(f'  {root}: {shown}', file=sys.stderr)
        print(
            '\nAll six pods must pin the IDENTICAL discord-core tag -- see '
            'tests/cli/_package_pins.py. Bump every pod\'s pin together, in one '
            'coordinated PR, whenever discord_core releases a new tag.',
            file=sys.stderr)
        return 1

    print('\nevery pod pins the same discord-core tag', file=sys.stderr)
    return 0


if __name__ == '__main__':
    sys.exit(main())
