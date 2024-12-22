from logging import RootLogger

from bs4 import BeautifulSoup
from dappertable import shorten_string_cjk
from discord.ext.commands import Bot, command, Context
from sqlalchemy.engine.base import Engine
from requests import get as requests_get

from discord_bot.cogs.common import CogHelper
from discord_bot.exceptions import CogMissingRequiredArg


BASE_URL = 'https://www.urbandictionary.com/'

class UrbanDictionary(CogHelper):
    '''
    Class that looks up urban dictionary definitions
    '''

    def __init__(self, bot: Bot, logger: RootLogger, settings: dict, _db_engine: Engine):
        if not settings.get('general', {}).get('include', {}).get('urban', False):
            raise CogMissingRequiredArg('Urban not enabled')
        super().__init__(bot, logger, settings, None)

    @command(name='urban')
    async def word_lookup(self, ctx: Context, *, word: str):
        '''
        Lookup word on urban dictionary

        search: str [Required]
            The word or phrase to search in urban dictionary
        '''
        self.logger.debug(f'Urban :: Looking up word string "{word}" {ctx.guild.id}')
        word_url = f'{BASE_URL}define.php?term={word}'
        result = requests_get(word_url, timeout=60)
        if result.status_code != 200:
            return await ctx.send(f'Unable to lookup word "{word}"')
        soup = BeautifulSoup(result.content, 'html.parser')
        definition_panels = soup.find_all("div", class_="definition")

        definitions = []
        for panel in definition_panels:
            meanings = panel.find_all('div', class_='meaning')
            for mean in meanings:
                definitions.append(mean.text)
        text = ''
        for (count, define) in enumerate(definitions[:2]):
            definition = shorten_string_cjk(define, 400)
            text = f'{text}{count+1}. {definition}\n'
        return await ctx.send(f'```{text}```')
