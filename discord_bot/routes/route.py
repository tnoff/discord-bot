'''
The `Route` value type shared by every seam registry.

Lives in its own module, imported by all of them, which makes it fanout-6 —
the thing acceptance criterion one exists to avoid. It is the deliberate
exception: the cost that criterion is written against is churn x fanout, and
this module is a frozen three-field dataclass with no seam knowledge in it, so
its churn is ~0. A route ADDITION touches a seam registry, never this file. If
this module ever starts changing with the seams, that is the signal it has
grown something that belonged in them.
'''
from dataclasses import dataclass


@dataclass(frozen=True)
class Route:
    '''
    One pod-to-pod HTTP route, named once and imported by both sides.

    `template` is the aiohttp route template, with `{param}` placeholders left
    unformatted — `/requests/{uuid}`, not the interpolated path. That is what
    makes client and server comparable: the server registers the template
    verbatim, the client formats it per call, and the drift test matches
    template-to-template. Never store a formatted path here.
    '''
    method: str
    template: str

    def path(self, **params: object) -> str:
        '''Format the template for a single call.

        Raises KeyError if a placeholder is unfilled, which is the point — a
        missing parameter fails at the call site rather than issuing a request
        to a literal `/requests/{uuid}`.
        '''
        return self.template.format(**params)


def collect(namespace: dict) -> tuple[Route, ...]:
    '''Every Route defined at module level in `namespace`, in definition order.

    Seam registries end with `ALL = collect(globals())` rather than a
    hand-written tuple. A maintained list would be a second place each route is
    written, and a route missing from it would be invisible to the drift test —
    the same failure mode one level up. Deriving it means a route that exists
    is a route the test sees.
    '''
    return tuple(value for value in namespace.values() if isinstance(value, Route))
