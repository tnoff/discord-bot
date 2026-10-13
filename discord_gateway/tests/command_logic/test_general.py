from unittest.mock import MagicMock

import pytest

from discord_gateway.command_logic.general import GeneralCommands


@pytest.mark.asyncio
async def test_hello(envelope, reply, sent):  #pylint:disable=redefined-outer-name
    result = await GeneralCommands(reply).hello(envelope)
    assert result == 'Waddup Tyler'
    assert sent == [(11, 22, 'Waddup Tyler')]


@pytest.mark.asyncio
async def test_roll(mocker, envelope, reply):  #pylint:disable=redefined-outer-name
    logic = GeneralCommands(reply)
    mocker.patch('discord_gateway.command_logic.general.randint', return_value=3)
    assert await logic.roll(envelope, '5') == 'Tyler rolled a 3'
    assert await logic.roll(envelope, 'd5') == 'Tyler rolled a 3'
    assert await logic.roll(envelope, '3d10') == 'Tyler rolled: 3 + 3 + 3 = 9'


@pytest.mark.asyncio
async def test_roll_invalid_input(mocker, envelope, reply):  #pylint:disable=redefined-outer-name
    logic = GeneralCommands(reply)
    mocker.patch('discord_gateway.command_logic.general.randint', return_value=3)
    assert await logic.roll(envelope, 'foo') == 'Invalid input given "foo"'
    assert await logic.roll(envelope, '21d5') == 'Invalid input given, max rolls is 20 but "21" given'
    assert await logic.roll(envelope, '3d101') == 'Invalid input given, max sides is 100 but "101" given'


@pytest.mark.asyncio
async def test_roll_value_error(mocker, envelope, reply):  #pylint:disable=redefined-outer-name
    '''ValueError branch is reached when match groups cannot be converted to int'''
    mock_matcher = MagicMock()
    mock_matcher.group.side_effect = lambda key: 'abc' if key == 'sides' else None
    mocker.patch('discord_gateway.command_logic.general.match', return_value=mock_matcher)
    result = await GeneralCommands(reply).roll(envelope, 'abc')
    assert 'Non integer value given' in result


@pytest.mark.asyncio
async def test_meta(envelope, reply):  #pylint:disable=redefined-outer-name
    result = await GeneralCommands(reply).meta(envelope)
    assert result == '```Server id: 11\nChannel id: 22\nUser id: 33```'
