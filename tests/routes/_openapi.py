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

**Bodies come from the specs, and the specs are proved.** A Route is (method, template)
and nothing else, and the bodies are typed differently across the seams, so the mapping from
route to request and response models is written down once per surface in
``tests/routes/specs/``. That is a hand-kept table, which is the rot the original generator
avoided, so ``tests/routes/test_openapi_bodies.py`` makes it checkable: every reply a spec
declares is produced by calling the real handler, and what comes back must match the declared
model. A spec that drifts from its handler fails there, not in a reader's client.
'''
import http
import re

import yaml
from pydantic.json_schema import models_json_schema

from discord_core.routes.route import Route

from tests.cli._image_deps import REPO_ROOT
from tests.routes._seam_servers import SEAMS, served
from tests.routes.specs import SURFACES

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


_JSON_TYPES = {str: 'string', int: 'integer', bool: 'boolean', float: 'number'}

_SCHEMAS = '#/components/schemas/{model}'


def _parameters(template: str, operation) -> list:
    '''Path and query parameters, as the spec declares their types.

    A path parameter is a `string` unless the spec says otherwise: the wire carries text, and the
    spec only narrows it to `integer` where the handler rejects anything else with a 422.
    '''
    declared = operation.path
    parameters = [{'name': name, 'in': 'path', 'required': True,
                   'schema': {'type': _JSON_TYPES[declared.get(name, str)]}}
                  for name in _PLACEHOLDER.findall(template)]
    for query in operation.query:
        parameter = {'name': query.name, 'in': 'query', 'required': query.required,
                     'schema': {'type': _JSON_TYPES[query.type]}}
        if query.description:
            parameter['description'] = query.description
        parameters.append(parameter)
    return parameters


def _models(operation) -> list:
    '''The (model, mode) pairs one operation refers to: validation for what it takes,
    serialization for what it answers.'''
    pairs = [(operation.request, 'validation')] if operation.request else []
    pairs += [(model, 'serialization') for reply in operation.replies for model in reply.models]
    return pairs


def _responses(operation, refs: dict) -> dict:
    responses = {}
    for reply in operation.replies:
        entry = {'description': reply.description or http.HTTPStatus(reply.status).phrase}
        if reply.models:
            schemas = [dict(refs[(model, 'serialization')]) for model in reply.models]
            entry['content'] = {'application/json': {
                'schema': schemas[0] if len(schemas) == 1 else {'oneOf': schemas}}}
        responses[str(reply.status)] = entry
    return responses


def build_document(surface: str, entries: set, operations: dict) -> dict:
    '''One OpenAPI document for one served surface.

    `operations` is the surface's spec table. A served route with no entry is an error, not a
    placeholder: the coverage test names it, and this raises so a stale table cannot render.
    '''
    ordered = sorted(entries, key=lambda e: (e[1], e[0]))
    missing = [entry for entry in ordered if Route(*entry) not in operations]
    if missing:
        raise KeyError(f'{surface}: no spec for {missing}')
    pairs = [pair for entry in ordered for pair in _models(operations[Route(*entry)])]
    refs, definitions = models_json_schema(pairs, ref_template=_SCHEMAS) if pairs else ({}, {})
    paths = {}
    for method, template in ordered:
        operation = operations[Route(method, template)]
        document = {
            'operationId': _operation_id(method, template),
            'summary': operation.summary,
        }
        parameters = _parameters(template, operation)
        if parameters:
            document['parameters'] = parameters
        if operation.request:
            document['requestBody'] = {'required': True, 'content': {'application/json': {
                'schema': dict(refs[(operation.request, 'validation')])}}}
        document['responses'] = _responses(operation, refs)
        paths.setdefault(template, {})[method.lower()] = document
    result = {
        'openapi': OPENAPI_VERSION,
        'info': {
            'title': f'discord {surface}',
            'version': FROZEN_VERSION,
            'description': (
                f'The {surface} seam, generated from the built aiohttp router and the route '
                f'specs in tests/routes/specs by {GENERATED_BY}. Do not edit by hand. Every '
                f'request and response body here is exercised against the real handler by '
                f'tests/routes/test_openapi_bodies.py.'),
        },
        'paths': paths,
    }
    if definitions.get('$defs'):
        result['components'] = {'schemas': definitions['$defs']}
    return result


def _operation_id(method: str, template: str) -> str:
    '''A stable, unique id per (method, template), derived rather than named.'''
    slug = _PLACEHOLDER.sub(r'by-\1', template).strip('/').replace('/', '-')
    return f'{method.lower()}-{slug}' if slug else method.lower()


def measure_all() -> dict:
    '''Every served surface's document, keyed by document name.'''
    return {document_name(surface): build_document(surface, served(factory().build_app()),
                                                   SURFACES[surface].operations)
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
