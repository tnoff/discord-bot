import pytest

from discord_bot.cogs.urban import UrbanDictionary, BASE_URL
from discord_bot.exceptions import CogMissingRequiredArg

from tests.data.urban_data import HTML_DATA
from tests.helpers import fake_bot_yielder, FakeContext

def test_urban_dictionary_startup():
    config = {
        'general': {
            'include': {
                'urban': False
            }
        }
    }
    fake_bot = fake_bot_yielder()()
    with pytest.raises(CogMissingRequiredArg) as exc:
        UrbanDictionary(fake_bot, config, None)
    assert 'Urban not enabled' in str(exc.value)

@pytest.mark.asyncio
async def test_urban_lookup(requests_mock):
    config = {
        'general': {
            'include': {
                'urban': True
            }
        }
    }
    fake_bot = fake_bot_yielder()()
    requests_mock.get(f'{BASE_URL}define.php?term=foo bar', text=HTML_DATA)
    cog = UrbanDictionary(fake_bot, config, None)
    result = await cog.word_lookup(cog, FakeContext(), word='foo bar') #pylint:disable=too-many-function-args
    assert result == '```1. foo bar is very often used in computer programming, used to declare a (temporary) variable.\n                                        Most probably, "foo" and "bar" came from "foobar," which in turn had its origins in the military slang acronym FUBAR. The most common rendition is "Fucked Up Beyond All Recognition"\n                                        \n2. A slang term meaning "fucked up beyond all recognition." The saying was used primarilly in programing and originated as fubar, but for political correctness it was changed to foo bar.\n                                        \nFoo and bar also often represent variables.\n```'
