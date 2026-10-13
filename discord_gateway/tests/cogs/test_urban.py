import pytest

from discord_core.exceptions import CogMissingRequiredArg

from discord_gateway.cogs.urban import UrbanDictionary
from discord_gateway.command_logic.urban import BASE_URL

from discord_gateway.tests.command_logic.urban_data import HTML_DATA
from tests.helpers import  fake_context #pylint:disable=unused-import

URBAN_CONFIG = {
    'general': {
        'include': {
            'urban': True
        }
    }
}


def test_urban_dictionary_startup(fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'general': {
            'include': {
                'urban': False
            }
        }
    }
    with pytest.raises(CogMissingRequiredArg) as exc:
        UrbanDictionary(fake_context['bot'], config, fake_context['dispatcher'])
    assert 'Urban not enabled' in str(exc.value)


@pytest.mark.asyncio
async def test_urban_stub_delegates(requests_mock, fake_context):  #pylint:disable=redefined-outer-name
    '''The stub builds the envelope from ctx and hands it, with the parsed word, to the logic'''
    requests_mock.get(f'{BASE_URL}define.php?term=foo bar', text=HTML_DATA)
    cog = UrbanDictionary(fake_context['bot'], URBAN_CONFIG, fake_context['dispatcher'])
    result = await cog.word_lookup(cog, fake_context['context'], word='foo bar') #pylint:disable=too-many-function-args
    assert result.startswith('```1. foo bar is very often used')
