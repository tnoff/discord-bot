'''
Shared pieces of the store-client tests: a stub that serves the database contract and what it is told to answer.

The clients are tested against `testing/contracts/database.py` rather than the db pod, so what they send is checked
against the same request models the pod's server reads, and what they parse is built from the same response models the
pod's server is proved to produce.
'''
from discord_core.types.database_wire import DatabaseErrorBody, DatabaseResponse

from testing.contracts.database import CONTRACTS
from testing.contracts.stub import ContractStub


def answering(route, result) -> ContractStub:
    '''A stub whose ``route`` answers 200 with ``result``, as the pod does when the store succeeds.'''
    stub = ContractStub(CONTRACTS)
    answer(stub, route, result)
    return stub


def answer(stub: ContractStub, route, result) -> None:
    '''Have ``stub`` answer ``route`` with ``result``, validated against that route's declared response model.'''
    response_model = CONTRACTS[route].replies[0].models[0]
    stub.script(route, response_model(result=result))


def failing(route, detail: str = 'OperationalError: the database is gone') -> ContractStub:
    '''A stub whose ``route`` answers 200 with an error body, as the pod does when the store failed.'''
    stub = ContractStub(CONTRACTS)
    stub.script(route, DatabaseResponse(error=DatabaseErrorBody(detail=detail)))
    return stub
