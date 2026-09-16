'''
Generate an OpenAPI document per served surface, from the built router.

Acceptance criterion eight of docs/projects/http-seam-contract.md. The document
is DERIVED, never written: it is the same route set the pod advertises at
GET /_contract/routes, read the same way, so an OpenAPI file and the running pod
cannot disagree about which routes exist.

**Read from the router rather than the registry**, as the criterion asks. The
two are pinned equal in both directions by test_registry_and_router_agree, so on
paper either source gives the same answer today -- but only the router stays
correct if a server ever stops registering something the registry declares, and
that is exactly the drift both incidents were. Reading the weaker source because
a test currently makes it equivalent would throw away the reason to prefer it.

**No schemas, deliberately.** A Route is (method, template) and nothing else.
The bodies are typed four different ways across the seams -- named models on the
broker, `result: Any` on database, a dict discriminated by req_type on dispatch
-- so describing them here would mean inventing a mapping that exists nowhere
else. That is the THIRD representation this project rejected when it rejected
hand-written OpenAPI, and it would rot the moment a model changed. What these
documents state is exactly what the seam contract actually guarantees: which
routes exist, and what they are called.
'''
import re

import yaml

from tests.cli._image_deps import REPO_ROOT
from tests.routes._seam_servers import SEAMS, served

OPENAPI_DIR = REPO_ROOT / 'docs' / 'openapi'

#: OpenAPI requires info.version, and acceptance criterion nine forbids
#: hand-maintained version integers anywhere in this design. Both hold only if
#: this is a FROZEN CONSTANT that nothing reads and nobody bumps. The moment it
#: starts tracking anything it becomes the version handshake criterion nine
#: rejected -- comparing something that churns faster than the contract does.
FROZEN_VERSION = '0.0.0'

OPENAPI_VERSION = '3.1.0'

#: aiohttp templates use {name}; so does OpenAPI, so paths pass through verbatim
#: and only the parameter declarations have to be derived.
_PLACEHOLDER = re.compile(r'\{([^}/]+)\}')

GENERATED_BY = 'tests/routes/test_openapi_docs.py'


def document_name(surface: str) -> str:
    '''File and API-entity name for a served surface: broker, queue-worker-downloads.'''
    return surface.replace('/', '-').replace('_', '-')


def _parameters(template: str) -> list:
    '''Path parameters OpenAPI requires to be declared for a templated path.

    Typed `string` because that is what a URL path segment is. The handlers coerce
    some of them to int, but the wire carries text, and claiming otherwise would
    be describing the handler rather than the route.
    '''
    return [{'name': name, 'in': 'path', 'required': True,
             'schema': {'type': 'string'}}
            for name in _PLACEHOLDER.findall(template)]


def build_document(surface: str, entries: set) -> dict:
    '''One OpenAPI document for one served surface.'''
    paths = {}
    for method, template in sorted(entries, key=lambda e: (e[1], e[0])):
        operation = {
            'operationId': _operation_id(method, template),
            'responses': {'default': {'description': 'See the handler; '
                                                     'the seam contract covers the route, '
                                                     'not the body.'}},
        }
        parameters = _parameters(template)
        if parameters:
            operation['parameters'] = parameters
        paths.setdefault(template, {})[method.lower()] = operation
    return {
        'openapi': OPENAPI_VERSION,
        'info': {
            'title': f'discord {surface}',
            'version': FROZEN_VERSION,
            'description': (
                f'The {surface} seam, generated from the built aiohttp router by '
                f'{GENERATED_BY}. Do not edit by hand. Routes only: the seam '
                f'contract guarantees which routes exist, not the shape of their '
                f'bodies -- see docs/projects/http-seam-contract.md.'),
        },
        'paths': paths,
    }


def _operation_id(method: str, template: str) -> str:
    '''A stable, unique id per (method, template), derived rather than named.'''
    slug = _PLACEHOLDER.sub(r'by-\1', template).strip('/').replace('/', '-')
    return f'{method.lower()}-{slug}' if slug else method.lower()


def measure_all() -> dict:
    '''Every served surface's document, keyed by document name.'''
    return {document_name(surface): build_document(surface, served(factory().build_app()))
            for surface, factory, _expected, _image in SEAMS}


def providers() -> dict:
    '''Document name -> the image that serves it.'''
    return {document_name(surface): image for surface, _f, _e, image in SEAMS}


def render(document: dict) -> str:
    '''One document as the YAML that lands on disk.

    sort_keys=False so the reading order is openapi/info/paths rather than
    alphabetical, and default_flow_style=False so a diff on a route addition is
    one block rather than one re-flowed line.
    '''
    header = (f'# GENERATED by {GENERATED_BY}. Do not edit by hand.\n'
              f'# Regenerate with: UPDATE_OPENAPI_DOCS=1 pytest {GENERATED_BY}\n')
    return header + yaml.safe_dump(document, sort_keys=False, default_flow_style=False,
                                   width=100)


def write_all() -> list:
    '''Render every document to docs/openapi/. Returns the paths written.'''
    OPENAPI_DIR.mkdir(parents=True, exist_ok=True)
    written = []
    for name, document in measure_all().items():
        path = OPENAPI_DIR / f'{name}.yaml'
        path.write_text(render(document))
        written.append(path)
    return written
