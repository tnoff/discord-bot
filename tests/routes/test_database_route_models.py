'''
The database seam's route-to-model map is DERIVED, not written.

projects/seam-body-typing. [[http-seam-contract]] rejected a route-to-model map
partly because writing one "would be exactly the third representation this
project rejected for OpenAPI", and partly because it looked "impossible for
`database` and `dispatch` without inventing types first -- 41 of 73 routes".

The second half was out of date and the first half is answered here rather than
argued with. `database`'s 33 routes are not typed by a map someone maintains:

    route name  ==  Protocol method name
    group name  ->  Protocol class name   (markov -> MarkovStore)

so the map is a derivation over `database_protocols`, and there is nothing to
keep in step. `DatabaseResponse.result` stays `Any` -- the envelope was never
the gap. The contract is the DTO each Protocol method already promises, exactly
as `database_wire` says:

    the client validates the payload into the DTO its Protocol method already
    promised, and that DTO is the contract.

What this file proves is that the derivation is total: every route resolves, and
every resolved method says what it returns. A schema generator can then walk it
without anyone writing a 33-line table.
'''
import typing

import pytest

from discord_seam_database.interfaces import database_protocols as protocols
from discord_seam_database.routes import database as db_routes


def store_protocol(group_name: str):
    '''`guild_analytics` -> `GuildAnalyticsStore`, by convention rather than by table.'''
    class_name = ''.join(part.title() for part in group_name.split('_')) + 'Store'
    return getattr(protocols, class_name, None)


def route_return_type(group_name: str, route_name: str):
    '''The DTO a route answers with, read off its Protocol method's annotation.'''
    store = store_protocol(group_name)
    if store is None:
        return None
    method = getattr(store, route_name, None)
    if method is None:
        return None
    return typing.get_type_hints(method).get('return', None)


def _all_routes():
    return [(group.name, route_name)
            for group in db_routes.GROUPS for route_name in group.routes]


def test_every_group_resolves_to_a_protocol():
    '''
    The group-to-class step is a naming convention, so it breaks silently if a
    group is renamed without its Protocol. Asserted per group rather than in
    aggregate so the failure names which one.
    '''
    assert db_routes.GROUPS, 'no route groups found -- the registry is empty or moved'
    for group in db_routes.GROUPS:
        assert store_protocol(group.name) is not None, (
            f'route group {group.name!r} has no matching Protocol class. Either the '
            f'group was renamed without its store, or the convention '
            f'(snake_case -> PascalCaseStore) no longer holds and this derivation '
            f'needs replacing rather than patching.'
        )


@pytest.mark.parametrize('group_name,route_name', _all_routes())
def test_every_route_resolves_to_an_annotated_protocol_method(group_name, route_name):
    '''
    Total derivation: a route with no method, or a method with no return
    annotation, is a hole a schema generator would have to guess at.
    '''
    store = store_protocol(group_name)
    assert hasattr(store, route_name), (
        f'/database/{group_name}/{route_name} has no {store.__name__}.{route_name}. '
        f'A route and its Protocol method are the same name by design; this one drifted.'
    )
    returns = route_return_type(group_name, route_name)
    assert returns is not None, (
        f'{store.__name__}.{route_name} has no return annotation, so nothing says '
        f'what /database/{group_name}/{route_name} answers with.'
    )


def test_the_map_covers_the_whole_seam_and_is_not_empty():
    '''
    Anti-vacuity, and the count is the point. The 33 here is what makes
    "41 of 73 routes are impossible" false: this seam is 33 of that 41, and all
    33 resolve.
    '''
    routes = _all_routes()
    assert len(routes) == len(db_routes.ALL), (
        f'derived {len(routes)} routes but the registry has {len(db_routes.ALL)} -- '
        f'the walk is missing a group'
    )
    assert len(routes) > 30, f'only {len(routes)} routes derived; the registry looks truncated'
    resolved = [r for r in routes if route_return_type(*r) is not None]
    assert len(resolved) == len(routes), (
        f'{len(routes) - len(resolved)} of {len(routes)} routes did not resolve'
    )


def test_a_new_route_is_covered_without_touching_this_file(monkeypatch):
    """
    The objection this answers was that a route-to-model map is a third
    representation. It is only that if someone maintains one.

    Proved behaviourally rather than by grepping this file for route names --
    the first version of this test did that, and failed on the names it had to
    write down to check for. Instead: invent a group and a store that did not
    exist when this was written, and show the derivation resolves them with no
    edit here.
    """
    class WidgetCacheStore(typing.Protocol):
        """A store this file has never heard of."""

        async def fetch_widget(self, widget_id: int) -> list[int]:
            """Annotated, like every real one."""

    monkeypatch.setattr(protocols, 'WidgetCacheStore', WidgetCacheStore, raising=False)
    assert store_protocol('widget_cache') is WidgetCacheStore
    assert route_return_type('widget_cache', 'fetch_widget') == list[int]


def test_an_unannotated_method_is_reported_rather_than_skipped(monkeypatch):
    """
    The failure mode that matters: a Protocol method with no return annotation
    leaves a route a schema generator has to guess at. It must read as a hole,
    not as an absent entry -- those look identical to a caller iterating the map.
    """
    class BlankStore(typing.Protocol):
        async def do_thing(self):  # no return annotation, deliberately
            """Unannotated on purpose."""

    monkeypatch.setattr(protocols, 'BlankStore', BlankStore, raising=False)
    assert store_protocol('blank') is BlankStore
    assert route_return_type('blank', 'do_thing') is None
