'''
The pod-neutral half of a route description: what a route takes and what it may answer.

A route spec used to be one object that also held the calls that prove it against a live handler, and those
calls reach into the serving pod, so nothing that only needed to know what a route says could use it without
importing the pod that serves it. A ``Contract`` is the part that does not: models, statuses and prose, built
from `discord_core` alone. `tests/routes/specs/_model.py` extends it with the proving calls (an ``Operation``),
and `testing.contracts.stub` serves one, so the same description is checked from both ends:

  server side  the real handler answers each declared reply with the declared body (tests/routes/)
  client side  the real client sends bodies the request model accepts and parses every declared reply

Imports nothing but pydantic: it must stay importable from any pod's tests.
'''
from collections.abc import Mapping
from dataclasses import KW_ONLY, dataclass, field

from pydantic import BaseModel


@dataclass(frozen=True)
class Reply:
    '''One documented answer. ``models`` is empty for a bodyless status and holds several for a
    route that answers one status with different bodies (a hit and a miss, say), which the
    document states as ``oneOf``.'''
    status: int
    models: tuple[type[BaseModel], ...] = ()
    description: str = ''


def reply(status: int, *models: type[BaseModel], doc: str = '') -> Reply:
    '''A ``Reply`` with the models given positionally.'''
    return Reply(status, tuple(models), doc)


@dataclass(frozen=True)
class Query:
    '''One query-string parameter.'''
    name: str
    type: type = str
    required: bool = False
    description: str = ''


@dataclass(frozen=True)
class Contract:
    '''What one route takes and what it answers.'''
    summary: str
    _: KW_ONLY
    replies: tuple[Reply, ...]
    request: type[BaseModel] | None = None
    path: Mapping[str, type] = field(default_factory=dict)
    query: tuple[Query, ...] = ()
