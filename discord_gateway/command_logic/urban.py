import asyncio
from logging import Logger

from bs4 import BeautifulSoup
from dappertable import shorten_string
from requests import get as requests_get

from discord_gateway.command_logic.envelope import CommandEnvelope
from discord_gateway.command_logic.reply import Reply

BASE_URL = 'https://www.urbandictionary.com/'


class UrbanCommands:
    '''
    Urban dictionary lookups. Discord-free: see ``command_logic.envelope``.
    '''

    def __init__(self, reply: Reply, logger: Logger):
        self.reply = reply
        self.logger = logger

    async def word_lookup(self, env: CommandEnvelope, word: str) -> str:
        '''
        Look up ``word`` on urban dictionary and reply with the first two definitions.
        '''
        self.logger.debug(f'Looking up word string "{word}" in guild "{env.guild_id}"')
        word_url = f'{BASE_URL}define.php?term={word}'
        result = await asyncio.to_thread(requests_get, word_url, timeout=60)
        if result.status_code != 200:
            return await self.reply(env.guild_id, env.channel_id, f'Unable to lookup word "{word}"')
        soup = BeautifulSoup(result.content, 'html.parser')
        definition_panels = soup.find_all("div", class_="definition")

        definitions = []
        for panel in definition_panels:
            meanings = panel.find_all('div', class_='meaning')
            for mean in meanings:
                definitions.append(mean.text)
        text = ''
        for (count, define) in enumerate(definitions[:2]):
            definition = shorten_string(define, 400)
            text = f'{text}{count+1}. {definition}\n'
        if not text:
            return await self.reply(env.guild_id, env.channel_id, f'No results found for "{word}"')
        return await self.reply(env.guild_id, env.channel_id, f'```{text}```')
