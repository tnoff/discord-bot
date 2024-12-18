import asyncio
from datetime import datetime, timezone

class AsyncIterator():
    def __init__(self, items):
        self.items = items

    async def __aiter__(self):
        for item in self.items:
            yield item

class FakeMessage():
    def __init__(self):
        self.id = 'fake-message-1234'
        self.created_at = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        self.deleted = False

    async def delete(self):
        self.deleted = True
        print('Calling delete')
        return True

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

class FakeAuthor():
    def __init__(self):
        self.id = 'fake-user-id-123'
        self.name = 'fake-user-name-123'
        self.display_name = 'fake-display-name-123'

class FakeChannel():
    def __init__(self, fake_message=None):
        self.id = 'fake-channel-id-123'
        self.fake_message = fake_message or FakeMessage()

    def history(self, **_kwargs):
        return AsyncIterator([self.fake_message])

def fake_bot_yielder(start_sleep=0, guilds=None, fake_channel=None):
    class FakeBot():
        def __init__(self, *_args, **_kwargs):
            self.startup_functions = []
            self.user = FakeBotUser()
            self.cogs = []
            self.guilds = guilds or []
            self.token = None
            self.fake_channel = fake_channel

        async def fetch_channel(self, _channel_id):
            return fake_channel

        def fetch_guilds(self, **_kwargs):
            return AsyncIterator(guilds)

        def event(self, func):
            self.startup_functions.append(func)

        def is_closed(self):
            return False

        async def start(self, token):
            self.token = token
            for func in self.startup_functions:
                await func()
            await asyncio.sleep(start_sleep)

        async def __aenter__(self):
            pass

        async def __aexit__(self, *args):
            pass

        async def add_cog(self, cog):
            self.cogs.append(cog)
        
        async def wait_until_ready(self):
            return True

    return FakeBot


class FakeContext():
    def __init__(self):
        self.author = FakeAuthor()
        self.guild = FakeGuild()
        self.channel = FakeChannel()

    async def send(self, message):
        return message