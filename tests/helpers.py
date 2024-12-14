import asyncio

class FakeBotUser():
    def __init__(self):
        self.id = 'fake-user-1234'

    def __str__(self):
        return f'{self.id}'

class FakeGuild():
    def __init__(self):
        self.id = 'fake-guild-1234'
        self.name = 'fake-guild-name'
        self.left_guild = False

    async def leave(self):
        self.left_guild = True

class AsyncIteratorGuild():
    def __init__(self, items):
        self.items = items

    async def __aiter__(self):
        for item in self.items:
            yield item

def fake_bot_yielder(start_sleep=0, guilds=None):
    class FakeBot():
        def __init__(self, *_args, **_kwargs):
            self.startup_functions = []
            self.user = FakeBotUser()
            self.cogs = []
            self.guilds = guilds or []
            self.token = None

        def fetch_guilds(self, **_kwargs):
            return AsyncIteratorGuild(guilds)

        def event(self, func):
            self.startup_functions.append(func)

        async def start(self, token):
            self.token = token
            for func in self.startup_functions:
                await func()
            print('Sleeping for seconds', start_sleep)
            await asyncio.sleep(start_sleep)

        async def __aenter__(self):
            pass

        async def __aexit__(self, *args):
            pass

        async def add_cog(self, cog):
            self.cogs.append(cog)
    return FakeBot
