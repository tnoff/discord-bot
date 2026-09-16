'''
The generated OpenAPI documents, and that they stay current.

Acceptance criterion eight of docs/projects/http-seam-contract.md. The documents
are derived from the built router -- the same source GET /_contract/routes reads
-- so a pod and its published API description cannot disagree about which routes
exist.

Same shape as docs/seam-topology.md and docs/image-dependencies.md: a generator,
a committed artifact, and a test that fails when the two diverge. A generated
file nobody checks is a file that silently stops being true.
'''
import os

import pytest
from openapi_spec_validator import validate

from tests.routes._openapi import (FROZEN_VERSION, OPENAPI_DIR, OPENAPI_VERSION,
                                   document_name, measure_all, providers, render,
                                   write_all)
from tests.routes._seam_servers import SEAMS, served

MEASURED = measure_all()


def test_a_document_exists_for_every_served_surface():
    '''Six documents for five seams.

    queue_worker is a route shape rather than a pod -- served at /downloads by the
    downloader and /search/ytmusic by the search pod, with disjoint route sets --
    so a per-SEAM file would describe two pods as one and leave neither accurate.
    '''
    assert set(MEASURED) == {document_name(surface) for surface, *_rest in SEAMS}
    assert len(MEASURED) == 6


@pytest.mark.parametrize('name', sorted(MEASURED))
def test_each_document_is_valid_openapi(name):
    '''Validated, not merely well-formed YAML.

    An invalid document fails at Backstage ingestion, which is a long way from
    whichever change produced it.
    '''
    validate(MEASURED[name])


@pytest.mark.parametrize('name', sorted(MEASURED))
def test_the_committed_document_is_current(name):
    '''Regenerate with: UPDATE_OPENAPI_DOCS=1 pytest tests/routes/test_openapi_docs.py'''
    path = OPENAPI_DIR / f'{name}.yaml'
    assert path.exists(), f'{path} is missing; regenerate the OpenAPI documents'
    assert path.read_text() == render(MEASURED[name]), (
        f'{path} is stale; regenerate the OpenAPI documents')


@pytest.mark.parametrize('surface,factory,_expected,_image', SEAMS,
                         ids=[s[0] for s in SEAMS])
def test_every_served_route_reaches_the_document(surface, factory, _expected, _image):
    '''The generator must not quietly drop a route.

    Checked against the router directly rather than against `expected`, because
    the router is what the generator reads and what the pod advertises. A count
    would pass while naming the wrong routes, so this compares the sets.
    '''
    document = MEASURED[document_name(surface)]
    in_document = {(method.upper(), template)
                   for template, operations in document['paths'].items()
                   for method in operations}
    assert in_document == served(factory().build_app())


def test_paths_may_carry_several_methods():
    '''A path with two verbs is one OpenAPI path, not two.

    The broker serves 22 routes across 20 paths. Asserting the totals differ
    pins the nesting: a generator that emitted one path per route would still
    look right route-by-route above, and would be wrong OpenAPI.
    '''
    broker = MEASURED['broker']
    routes = sum(len(operations) for operations in broker['paths'].values())
    assert routes == 22
    assert len(broker['paths']) == 20


def test_templated_paths_declare_their_parameters():
    '''OpenAPI requires a declaration for every {placeholder} in a path.'''
    for name, document in MEASURED.items():
        for template, operations in document['paths'].items():
            if '{' not in template:
                continue
            for method, operation in operations.items():
                declared = {p['name'] for p in operation.get('parameters', [])}
                assert declared, f'{name} {method} {template}: no parameters declared'
                assert all(f'{{{p}}}' in template for p in declared)


def test_the_version_is_frozen():
    """OpenAPI requires info.version; criterion nine forbids maintaining one.

    Both hold only while this is a constant nothing reads. If it ever starts
    tracking the route set or a build, it becomes the version handshake the
    project rejected -- something that churns faster than the contract and gets
    muted within a week.
    """
    assert FROZEN_VERSION == '0.0.0'
    assert {document['info']['version'] for document in MEASURED.values()} == {FROZEN_VERSION}
    assert {document['openapi'] for document in MEASURED.values()} == {OPENAPI_VERSION}


def test_every_document_has_exactly_one_providing_image():
    '''providesApis is only derivable if each surface resolves to one pod.'''
    assert set(providers()) == set(MEASURED)
    assert providers()['queue-worker-downloads'] == 'discord-downloader'
    assert providers()['queue-worker-ytmusic'] == 'discord-search'


@pytest.mark.skipif(not os.environ.get('UPDATE_OPENAPI_DOCS'),
                    reason='set UPDATE_OPENAPI_DOCS=1 to rewrite the documents')
def test_update_the_documents():
    '''Not a test: the regeneration entry point, run explicitly.'''
    write_all()
