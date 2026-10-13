import logging

import pytest

from discord_gateway.command_logic.urban import UrbanCommands, BASE_URL

from discord_gateway.tests.command_logic.urban_data import HTML_DATA


@pytest.fixture
def logic(reply):  #pylint:disable=redefined-outer-name
    return UrbanCommands(reply, logging.getLogger('test'))


@pytest.mark.asyncio
async def test_urban_lookup(requests_mock, logic, envelope, sent):  #pylint:disable=redefined-outer-name
    requests_mock.get(f'{BASE_URL}define.php?term=foo bar', text=HTML_DATA)
    result = await logic.word_lookup(envelope, 'foo bar')
    assert result == '```1. foo bar is very often used in computer programming, used to declare a (temporary) variable.\n                                        Most probably, "foo" and "bar" came from "foobar," which in turn had its origins in the military slang acronym FUBAR. The most common rendition is "Fucked Up Beyond All Recognition"\n                                        \n2. A slang term meaning "fucked up beyond all recognition." The saying was used primarilly in programing and originated as fubar, but for political correctness it was changed to foo bar.\n                                        \nFoo and bar also often represent variables.\n```'
    assert sent == [(11, 22, result)]


@pytest.mark.asyncio
async def test_urban_lookup_http_error(requests_mock, logic, envelope):  #pylint:disable=redefined-outer-name
    '''Returns error message when HTTP response is not 200'''
    requests_mock.get(f'{BASE_URL}define.php?term=badword', status_code=503)
    result = await logic.word_lookup(envelope, 'badword')
    assert 'Unable to lookup word' in result


@pytest.mark.asyncio
async def test_urban_lookup_no_definitions(requests_mock, logic, envelope):  #pylint:disable=redefined-outer-name
    '''Returns no-results message when page has no definition panels'''
    requests_mock.get(f'{BASE_URL}define.php?term=unknownxyz', text='<html><body></body></html>')
    result = await logic.word_lookup(envelope, 'unknownxyz')
    assert 'No results found' in result
