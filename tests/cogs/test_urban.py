import pytest

from discord_bot.cogs.urban import UrbanDictionary, BASE_URL
from discord_bot.exceptions import CogMissingRequiredArg

from tests.data.urban_data import HTML_DATA
from tests.helpers import  fake_context #pylint:disable=unused-import

def test_urban_dictionary_startup(fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'general': {
            'include': {
                'urban': False
            }
        }
    }
    with pytest.raises(CogMissingRequiredArg) as exc:
        UrbanDictionary(fake_context['bot'], config, None)
    assert 'Urban not enabled' in str(exc.value)

@pytest.mark.asyncio
async def test_urban_lookup(requests_mock, fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'general': {
            'include': {
                'urban': True
            }
        }
    }
    requests_mock.get(f'{BASE_URL}define.php?term=foo bar', text=HTML_DATA)
    cog = UrbanDictionary(fake_context['bot'], config, None)
    result = await cog.word_lookup(cog, fake_context['context'], word='foo bar') #pylint:disable=too-many-function-args
    assert result == '```1. foo bar is very often used in computer programming, used to declare a (temporary) variable.\n                                        Most probably, "foo" and "bar" came from "foobar," which in turn had its origins in the military slang acronym FUBAR. The most common rendition is "Fucked Up Beyond All Recognition"\n                                        \n2. A slang term meaning "fucked up beyond all recognition." The saying was used primarilly in programing and originated as fubar, but for political correctness it was changed to foo bar.\n                                        \nFoo and bar also often represent variables.\n```'

URBAN_CONFIG = {
    'general': {
        'include': {
            'urban': True
        }
    }
}


@pytest.mark.asyncio
async def test_urban_lookup_http_error(requests_mock, fake_context):  #pylint:disable=redefined-outer-name
    '''Returns error message when HTTP response is not 200'''
    requests_mock.get(f'{BASE_URL}define.php?term=badword', status_code=503)
    cog = UrbanDictionary(fake_context['bot'], URBAN_CONFIG, None)
    result = await cog.word_lookup(cog, fake_context['context'], word='badword')  #pylint:disable=too-many-function-args
    assert 'Unable to lookup word' in result


@pytest.mark.asyncio
async def test_urban_lookup_no_definitions(requests_mock, fake_context):  #pylint:disable=redefined-outer-name
    '''Returns no-results message when page has no definition panels'''
    requests_mock.get(f'{BASE_URL}define.php?term=unknownxyz', text='<html><body></body></html>')
    cog = UrbanDictionary(fake_context['bot'], URBAN_CONFIG, None)
    result = await cog.word_lookup(cog, fake_context['context'], word='unknownxyz')  #pylint:disable=too-many-function-args
    assert 'No results found' in result
