'''
The shapes a surface module uses to describe its routes.

An ``Operation`` is documentation (what the route takes and answers) plus the ``Call`` list that
proves it: every reply declared is exercised against the real handler by
``tests/routes/test_openapi_bodies.py``, so a description cannot claim a status or a body the
handler does not produce, and a handler cannot start answering something undescribed.
'''
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

from aiohttp.test_utils import TestClient, TestServer
from pydantic import BaseModel

from testing.contracts.model import Contract, Query, Reply, reply


__all__ = ['Call', 'Contract', 'Env', 'Operation', 'Query', 'Reply', 'Surface', 'operation', 'reply', 'serving']


@dataclass(frozen=True)
class Call:
    '''One request made to prove a reply.

    ``body``, ``path`` values and ``query`` values may be callables taking the dict that
    ``arrange`` returned, for things that only exist once the arrangement has run (a uuid).
    ``arrange`` gets the live environment and sets up whatever state the route needs.
    ``variant`` picks which of a reply's models the answer must match when it lists several.
    ``raw`` sends the body as given, without encoding it as JSON.
    '''
    status: int
    body: Any = None
    path: Mapping[str, Any] = field(default_factory=dict)
    query: Mapping[str, Any] = field(default_factory=dict)
    arrange: Callable[[Any], Awaitable[dict]] | None = None
    variant: type[BaseModel] | None = None
    raw: bool = False


@dataclass(frozen=True)
class Operation(Contract):
    '''A route's contract plus the calls that show it against the real handler.'''
    calls: tuple[Call, ...] = ()


def operation(contract: Contract, calls: tuple) -> Operation:
    '''``contract`` with the calls that prove it.'''
    return Operation(contract.summary, replies=contract.replies, request=contract.request,
                     path=contract.path, query=contract.query, calls=calls)


@dataclass
class Env:
    '''A live server and the fakes behind it. ``parts`` holds whatever the surface's calls reach
    into to arrange state: its broker, its stores, its queue.'''
    client: Any
    server: Any
    parts: SimpleNamespace


@asynccontextmanager
async def serving(server, **parts) -> AsyncIterator[Env]:
    '''Run ``server``'s app on a test server and yield it as an ``Env``.'''
    async with TestClient(TestServer(server.build_app())) as client:
        yield Env(client, server, SimpleNamespace(**parts))


@dataclass(frozen=True)
class Surface:
    '''One served surface: its route table, and how to stand a live copy of it up.'''
    operations: dict
    environment: Callable[[], Any]
