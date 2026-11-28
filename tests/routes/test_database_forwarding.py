'''
What the db pod's server hands each store: the fields of the request model, as the store method's arguments.

`specs/database.py` proves what every route ANSWERS; this proves what it ASKS the store. A handler that read the
wrong field, swapped two arguments or dropped one would still answer the declared shape, and a mock store accepts
anything. So for every route that takes arguments, the body is read as its core model and the store method must
have been awaited with exactly those fields, in the order of the Protocol signature.

Two routes take objects the server rebuilds rather than fields, so they are checked on what the rebuilt object carries.
'''
import inspect

import pytest

from discord_core.interfaces import database_protocols as protocols
from discord_core.routes import database as database_routes
from discord_core.types.database_requests import REQUESTS

from tests.routes.specs.database import DOWNLOAD, MEDIA_REQUEST, OPERATIONS, environment

REBUILT = {('video_cache', 'iterate_file'), ('video_cache', 'get_webpage_url_item')}
ROUTES = [(group.name, name) for group in database_routes.GROUPS for name in group.routes
          if REQUESTS[(group.name, name)] is not None and (group.name, name) not in REBUILT]


def _signature(group: str, name: str) -> list:
    '''The argument names of the Protocol method a route is named for, in order.'''
    class_name = ''.join(part.title() for part in group.split('_')) + 'Store'
    store = getattr(protocols, 'Db' + class_name, None) or getattr(protocols, class_name)
    return [p for p in inspect.signature(getattr(store, name)).parameters if p != 'self']


async def _post(env, group: str, name: str):
    '''Post the route's first spec call and return the body it sent and the store method it reached.'''
    route = next(g for g in database_routes.GROUPS if g.name == group).routes[name]
    call = OPERATIONS[route].calls[0]
    await call.arrange(env)
    response = await env.client.post(route.template, json=call.body)
    assert response.status == 200, await response.text()
    return call.body, getattr(getattr(env.parts, group), name)


@pytest.mark.parametrize('group, name', ROUTES, ids=[f'{group}/{name}' for group, name in ROUTES])
@pytest.mark.asyncio
async def test_the_store_receives_the_fields_of_the_request(group, name):
    '''Each route passes the model's fields on as the arguments of the method it is named for.'''
    async with environment() as env:
        body, method = await _post(env, group, name)
    request = REQUESTS[(group, name)].model_validate(body)
    parameters = _signature(group, name)
    method.assert_awaited_once()
    received = list(method.await_args.args) + list(method.await_args.kwargs.values())
    assert received == [getattr(request, field) for field in parameters]


@pytest.mark.asyncio
async def test_a_finished_download_reaches_the_store_with_its_request_and_file():
    '''iterate_file gets a MediaDownload carrying the request, the file and the yt-dlp fields that crossed.'''
    async with environment() as env:
        await _post(env, 'video_cache', 'iterate_file')
        (sent,) = env.parts.video_cache.iterate_file.await_args.args
    assert sent.media_request.uuid == DOWNLOAD['request']['uuid']
    assert (str(sent.file_path), sent.file_size_bytes, sent.cache_hit) == ('a.mp3', 10, True)
    assert (sent.id, sent.webpage_url) == ('a', 'https://example.com/a')


@pytest.mark.asyncio
async def test_a_cache_lookup_reaches_the_store_with_the_request_asked_about():
    '''get_webpage_url_item gets the caller's own request, not one rebuilt from a download.'''
    async with environment() as env:
        await _post(env, 'video_cache', 'get_webpage_url_item')
        (sent,) = env.parts.video_cache.get_webpage_url_item.await_args.args
    assert sent.uuid == MEDIA_REQUEST['uuid']


@pytest.mark.asyncio
async def test_a_removal_forwards_the_object_keys_it_was_given():
    '''base_paths is the optional half of remove_video_cache, absent from the spec's body, and dropping it would
    leave the object-store keys behind.'''
    async with environment() as env:
        await env.client.post(database_routes.VIDEO_CACHE.routes['remove_video_cache'].template,
                              json={'video_cache_ids': [8, 9], 'base_paths': ['cache/a', 'cache/b']})
        env.parts.video_cache.remove_video_cache.assert_awaited_once_with(
            [8, 9], base_paths=['cache/a', 'cache/b'])


@pytest.mark.asyncio
@pytest.mark.parametrize('body, expected', [({}, False), ({'evict_all': True}, True)])
async def test_a_ready_remove_forwards_whether_to_evict_everything(body, expected):
    '''evict_all flags every row rather than applying the policy, and defaults to the policy.'''
    async with environment() as env:
        await env.client.post(database_routes.VIDEO_CACHE.routes['ready_remove'].template, json=body)
        env.parts.video_cache.ready_remove.assert_awaited_once_with(evict_all=expected)
