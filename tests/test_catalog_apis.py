'''
catalog-info.yaml's API wiring matches the measured seam topology.

Acceptance criterion eight of docs/projects/http-seam-contract.md: the six pod
Components carry `providesApis` / `consumesApis` derived from the registry
rather than hand-listed. This is what makes "derived" true rather than a claim
about how the file happened to be written -- the fields cannot drift from the
code, because a run that disagrees fails here.

**Verified rather than regenerated.** catalog-info.yaml is a hand-maintained
file with substantial prose in it, most of which no measurement can produce, so
a generator that rewrote it wholesale would be a generator that destroys the
part of it worth reading. Checking is the same guarantee for this file: the
fields cannot silently go stale either way.
'''
import yaml

from tests._catalog import api_by_peer_and_seam, api_ref, consumes, provides
from tests.cli._image_deps import REPO_ROOT
from tests.routes._openapi import OPENAPI_DIR, measure_all

CATALOG = REPO_ROOT / 'catalog-info.yaml'

ENTITIES = [doc for doc in yaml.safe_load_all(CATALOG.read_text()) if doc]
COMPONENTS = {doc['metadata']['name']: doc for doc in ENTITIES if doc['kind'] == 'Component'}
APIS = {doc['metadata']['name']: doc for doc in ENTITIES if doc['kind'] == 'API'}


def test_the_join_that_derives_all_of_this_is_unique():
    '''(peer image, seam) must identify exactly one API.

    discord-search serves TWO surfaces so the image alone is ambiguous, and
    queue_worker is served by two images so the seam alone is ambiguous. The pair
    is unique across all six -- and if that ever stops holding, consumesApis
    would start attributing a dependency to the wrong pod with nothing to say so.
    '''
    lookup = api_by_peer_and_seam()
    assert len(lookup) == 6
    assert len(set(lookup.values())) == 6


def test_an_api_entity_exists_for_every_served_surface():
    assert set(APIS) == set(measure_all())


def test_every_api_points_at_its_generated_document():
    '''A $text reference to a file that does not exist fails at ingestion.'''
    for name, entity in APIS.items():
        reference = entity['spec']['definition']['$text']
        assert reference == f'./docs/openapi/{name}.yaml'
        assert (OPENAPI_DIR / f'{name}.yaml').exists(), f'{name}: no such document'
        assert entity['spec']['type'] == 'openapi'
        assert entity['spec']['system'] == 'discord'


def test_provides_matches_what_each_pod_serves():
    for image, names in provides().items():
        declared = COMPONENTS[image]['spec'].get('providesApis', [])
        assert declared == [api_ref(name) for name in names], image


def test_consumes_matches_the_measured_topology():
    for image, names in consumes().items():
        declared = COMPONENTS[image]['spec'].get('consumesApis', [])
        assert declared == [api_ref(name) for name in names], image


def test_a_serve_only_pod_consumes_nothing():
    '''discord-db and discord-dispatcher answer seams and call none.

    Asserted because the absence is the interesting part: a catalog that gave
    them a consumesApis would be claiming an edge the topology does not have.
    '''
    for image in ('discord-db', 'discord-dispatcher'):
        assert 'consumesApis' not in COMPONENTS[image]['spec'], image
        assert COMPONENTS[image]['spec']['providesApis'], image


def test_the_catalog_renders_the_seam_topology():
    """The criterion's actual check: bot consumes broker, broker provides it.

    Stated as the one edge the project spec names, so this test fails for the
    reason the criterion cares about rather than only on a structural mismatch.
    """
    assert api_ref('broker') in COMPONENTS['discord-bot']['spec']['consumesApis']
    assert api_ref('broker') in COMPONENTS['discord-broker']['spec']['providesApis']
    assert api_ref('broker') not in COMPONENTS['discord-broker']['spec']['consumesApis']


def test_no_api_is_provided_by_two_components():
    '''Two providers would mean the graph cannot say which pod to page.'''
    seen = {}
    for image, names in provides().items():
        for name in names:
            assert name not in seen, f'{name} provided by {image} and {seen[name]}'
            seen[name] = image
