'''
The route specs behind the OpenAPI documents are true.

``tests/routes/specs/`` says, per route, what it takes and what it answers. Nothing about that
table would fail on its own when a handler changed, so these tests run it: every declared reply
is produced by calling the real handler on a live server, and the body that comes back has to be
the declared model. In the other direction, a route with no spec, a spec for a route nobody
serves, and a declared reply no call reaches all fail.
'''
import copy

import pytest
from pydantic import BaseModel

from discord_core.routes.route import Route

from tests.routes._seam_servers import SEAMS, served
from tests.routes.specs import SURFACES

CASES = [(surface, route, operation, index, call)
         for surface, spec in SURFACES.items()
         for route, operation in spec.operations.items()
         for index, call in enumerate(operation.calls)]


def _id(case) -> str:
    surface, route, _operation, _index, call = case
    variant = f' {call.variant.__name__}' if call.variant else ''
    return f'{surface} {route.method} {route.template} {call.status}{variant}'


def _resolve(value, ctx):
    return value(ctx) if callable(value) else value


def _json(value):
    return value.model_dump(mode='json') if isinstance(value, BaseModel) else value


def _expand(template: str, path: dict) -> str:
    return template.format(**path)


async def _send(env, route: Route, call, ctx, body):
    '''One request, as the call describes it.'''
    path = {name: _resolve(value, ctx) for name, value in call.path.items()}
    query = {name: _resolve(value, ctx) for name, value in call.query.items()}
    kwargs = {'params': query} if query else {}
    if body is not None:
        kwargs['data' if call.raw else 'json'] = body
    return await env.client.request(route.method, _expand(route.template, path), **kwargs)


def _matches(model: type[BaseModel], payload) -> bool:
    '''The payload is exactly what ``model`` serialises: nothing missing, nothing extra.'''
    try:
        return model.model_validate(payload).model_dump(mode='json') == payload
    except Exception:  # pylint: disable=broad-exception-caught
        return False


@pytest.mark.parametrize('case', CASES, ids=[_id(case) for case in CASES])
@pytest.mark.asyncio
async def test_the_handler_answers_what_the_spec_says(case):
    '''The call produces its status, with a body of the declared model (or no body).'''
    surface, route, operation, _index, call = case
    async with SURFACES[surface].environment() as env:
        ctx = await call.arrange(env) if call.arrange else {}
        body = _json(_resolve(call.body, ctx))
        resp = await _send(env, route, call, ctx, body)
        text = await resp.text()
        assert resp.status == call.status, text
        declared = next(r for r in operation.replies if r.status == call.status)
        if not declared.models:
            assert resp.content_type != 'application/json', text
            return
        payload = await resp.json()
        matched = [model for model in declared.models if _matches(model, payload)]
        assert matched, f'{payload} is none of {[m.__name__ for m in declared.models]}'
        if call.variant:
            assert call.variant in matched, f'{payload} is not a {call.variant.__name__}'


def _operations():
    return [(surface, route, operation)
            for surface, spec in SURFACES.items()
            for route, operation in spec.operations.items()]


@pytest.mark.parametrize('surface,route,operation', _operations(),
                         ids=[f'{s} {r.method} {r.template}' for s, r, _o in _operations()])
def test_every_declared_reply_has_a_call_and_every_call_a_reply(surface, route, operation):
    '''Statuses match both ways, and each model of a many-model reply is reached by some call.'''
    del surface, route
    declared = {reply.status: reply for reply in operation.replies}
    assert len(declared) == len(operation.replies), 'a status is declared twice'
    assert {call.status for call in operation.calls} == set(declared)
    for status, reply in declared.items():
        if len(reply.models) > 1:
            variants = {call.variant for call in operation.calls if call.status == status}
            assert variants == set(reply.models)


@pytest.mark.parametrize('surface,factory,_expected,_image', SEAMS, ids=[s[0] for s in SEAMS])
def test_the_spec_table_is_the_served_route_set(surface, factory, _expected, _image):
    '''A route with no spec, or a spec for a route not served, is a gap in one direction or the other.'''
    described = {(route.method, route.template) for route in SURFACES[surface].operations}
    assert described == served(factory().build_app())


def _required(model: type[BaseModel]) -> set[str]:
    '''The top-level fields a request model insists on. For a union, those every branch insists on.'''
    schema = model.model_json_schema()
    definitions = schema.get('$defs', {})

    def resolve(node):
        while '$ref' in node:
            node = definitions[node['$ref'].rsplit('/', 1)[-1]]
        return node
    node = resolve(schema)
    if 'required' in node:
        return set(node['required'])
    branches = node.get('oneOf') or node.get('anyOf') or resolve(node.get('properties', {}).get('root', {})).get('oneOf')
    if not branches:
        return set()
    return set.intersection(*[set(resolve(branch).get('required', ())) for branch in branches])


def _body_cases():
    cases = []
    for surface, route, operation in _operations():
        if not operation.request:
            continue
        for call in operation.calls:
            if call.status < 300 and call.body is not None:
                cases.extend((surface, route, operation, call, field)
                             for field in sorted(_required(operation.request)))
    return cases


BODY_CASES = _body_cases()


@pytest.mark.parametrize('case', BODY_CASES, ids=[
    f'{s} {r.method} {r.template} without {f}' for s, r, _o, _c, f in BODY_CASES])
@pytest.mark.asyncio
async def test_a_request_without_a_required_field_is_refused(case):
    '''The request model's required fields are required: drop one and the handler says 422.'''
    surface, route, _operation, call, field = case
    async with SURFACES[surface].environment() as env:
        ctx = await call.arrange(env) if call.arrange else {}
        body = copy.deepcopy(_json(_resolve(call.body, ctx)))
        body.pop(field)
        resp = await _send(env, route, call, ctx, body)
        assert resp.status == 422, await resp.text()


@pytest.mark.parametrize('surface,route,operation', [o for o in _operations() if o[2].request],
                         ids=[f'{s} {r.method} {r.template}' for s, r, o in _operations() if o.request])
def test_the_success_examples_are_instances_of_the_request_model(surface, route, operation):
    '''What the calls send is what the document says may be sent.'''
    del surface, route
    for call in operation.calls:
        if call.status < 300 and call.body is not None and not callable(call.body):
            operation.request.model_validate(_json(call.body))

