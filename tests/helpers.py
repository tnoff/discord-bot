import asyncio
from datetime import datetime, timezone

from discord import ChannelType
from discord.errors import NotFound

class AsyncIterator():
    def __init__(self, items):
        self.items = items

    async def __aiter__(self):
        for item in self.items:
            yield item

class FakeResponse():
    def __init__(self):
        self.status = 404
        self.reason = 'Cant find nothing'

class FakeEmjoi():
    def __init__(self):
        self.id = 1234

class FakeMessage():
    def __init__(self, id=None, content=None):
        self.id = id or 'fake-message-1234'
        self.created_at = datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc)
        self.deleted = False
        self.content = content
        if content is None:
            self.content = 'fake message content that was typed by a real human'
        self.author = FakeAuthor()

    async def delete(self):
        self.deleted = True
        return True

class FakeRole():
    def __init__(self, id=None, name=None):
        self.id = id or 'fake-role-1234'
        self.name = name or 'fake-role-name'
        self.members = []

class FakeBotUser():
    def __init__(self):
        self.id = 'fake-user-1234'

    def __str__(self):
        return f'{self.id}'

class FakeGuild():
    def __init__(self, emojis=None, members=None, roles=None):
        self.id = 'fake-guild-1234'
        self.name = 'fake-guild-name'
        self.emojis = emojis
        self.left_guild = False
        self.members = members or []
        self.roles = roles or []

    async def leave(self):
        self.left_guild = True

    async def fetch_emojis(self, **_kwargs):
        return self.emojis

    async def fetch_member(self, member_id):
        for member in self.members:
            if member_id == member.id:
                return member
        raise NotFound(FakeResponse(), 'Unable to find user')

    def get_role(self, role_id):
        for role in self.roles:
            if role.id == role_id:
                return role
        raise NotFound(FakeResponse(), 'Unable to find role')

class FakeAuthor():
    def __init__(self, id=None, roles=None):
        self.id = id or 'fake-user-id-123'
        self.name = 'fake-user-name-123'
        self.display_name = 'fake-display-name-123'
        self.bot = False
        self.roles = roles or []

    async def add_roles(self, role):
        self.roles.append(role)

    async def remove_roles(self, role):
        self.roles.remove(role)

class FakeChannel():
    def __init__(self, fake_message=None, no_messages=False):
        self.id = 'fake-channel-id-123'
        if no_messages:
            self.messages = []
        else:
            fake_message = fake_message or FakeMessage()
            self.messages = [fake_message]
        self.type = ChannelType.text

    def history(self, **_kwargs):
        return AsyncIterator(self.messages)

    async def fetch_message(self, message_id):
        for message in self.messages:
            if message.id == message_id:
                return message
        raise NotFound(FakeResponse(), 'Unable to find message')

class FakeIntents():
    def __init__(self):
        self.members = True


def fake_bot_yielder(start_sleep=0, guilds=None, fake_channel=None):
    class FakeBot():
        def __init__(self, *_args, **_kwargs):
            self.startup_functions = []
            self.user = FakeBotUser()
            self.cogs = []
            self.guilds = guilds or []
            self.token = None
            self.fake_channel = fake_channel
            if guilds:
                self.guild = guilds[0]
            self.intents = FakeIntents()

        async def fetch_channel(self, _channel_id):
            return self.fake_channel

        async def fetch_guild(self, guild_id):
            for guild in self.guilds:
                if guild.id == guild_id:
                    return guild
            return None

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
    def __init__(self, fake_guild=None, author=None):
        self.author = author or FakeAuthor()
        self.guild = fake_guild or FakeGuild()
        self.channel = FakeChannel()
        self.messages_sent = []

    async def send(self, message):
        self.messages_sent.append(message)
        return message
