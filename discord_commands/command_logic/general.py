from random import randint
from re import match

from discord_core.types.command_envelope import CommandEnvelope

from discord_commands.command_logic.reply import Reply

ROLL_REGEX = r'^(?P<rolls>\d+)?([dD])?(?P<sides>\d+)'


class GeneralCommands:
    '''
    General use commands. Discord-free: see ``discord_core.types.command_envelope``.
    '''

    def __init__(self, reply: Reply):
        self.reply = reply

    async def hello(self, env: CommandEnvelope) -> str:
        '''Say hello to the author.'''
        return await self.reply(env.guild_id, env.channel_id, f'Waddup {env.author_display_name}')

    async def roll(self, env: CommandEnvelope, input_value: str) -> str:
        '''Roll dice described by ``input_value`` (``6``, ``d6`` or ``2d6``).'''
        # First check if matches regex
        matcher = match(ROLL_REGEX, input_value)
        if not matcher:
            message = f'Invalid input given "{input_value}"'
            return await self.reply(env.guild_id, env.channel_id, message)
        try:
            sides = int(matcher.group('sides'))
            rolls = matcher.group('rolls')
            if rolls is None:
                rolls = 1
            else:
                rolls = int(rolls)
        except ValueError:
            message = f'Non integer value given {input_value}'
            return await self.reply(env.guild_id, env.channel_id, message)

        if rolls > 20:
            return await self.reply(env.guild_id, env.channel_id, f'Invalid input given, max rolls is 20 but "{rolls}" given')
        if sides > 100:
            return await self.reply(env.guild_id, env.channel_id, f'Invalid input given, max sides is 100 but "{sides}" given')

        roll_values = []
        total = 0
        for _ in range(rolls):
            # bandit B311: dice rolls, not security-sensitive
            num = randint(1, sides)  # nosec B311
            total += num
            roll_values.append(num)
        if rolls == 1:
            message = f'{env.author_display_name} rolled a {total}'
        else:
            roll_values_message = ' + '.join(f'{d}' for d in roll_values)
            message = f'{env.author_display_name} rolled: {roll_values_message} = {total}'

        return await self.reply(env.guild_id, env.channel_id, message)

    async def meta(self, env: CommandEnvelope) -> str:
        '''Report the guild, channel and user ids.'''
        message = f'```Server id: {env.guild_id}\n'\
                f'Channel id: {env.channel_id}\n'\
                f'User id: {env.author_id}```'
        return await self.reply(env.guild_id, env.channel_id, message)
