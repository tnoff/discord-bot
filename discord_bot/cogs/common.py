from asyncio import sleep

from discord.ext import commands
from jsonschema import ValidationError
from sqlalchemy.orm import sessionmaker

from discord_bot.exceptions import CogMissingRequiredArg, ExitEarlyException
from discord_bot.utils import validate_config

class CogHelper(commands.Cog):
    '''
    Cogs usually have the following bits
    '''

    def __init__(self, bot, logger, settings, db_engine=None, enable_loop=False, settings_prefix=None, section_schema=None):
        '''
        Init a basic cog
        bot                 :   Discord bot object
        logger              :   Common python logger obj
        settings            :   Common settings config
        db_engine           :   (Optional) Sqlalchemy db engine
        enable_loop         :   (Optional) Enable background loop (default False)
        settings_prefix     :   (Optional) Settings prefix, will load settings if given
        section_schema      :   (Optional) Json schema to use to validate config. settings_prefix must also be given
        '''
        # Check that prefix given if schema also given
        if section_schema and not settings_prefix:
            raise CogMissingRequiredArg('Section schema given but settings prefix not given')

        # Default args
        self.bot = bot
        self.logger = logger
        self.settings = settings
        self.db_engine = db_engine
        self.db_session = None
        if self.db_engine:
            self.db_session = sessionmaker(bind=db_engine)()

        # Task object for loops
        self._task = None
        self.enable_loop = enable_loop

        # Setup config
        if section_schema:
            try:
                validate_config(self.settings[settings_prefix], section_schema)
            except ValidationError as exc:
                raise CogMissingRequiredArg('Invalid config given for markov bot') from exc
            except KeyError:
                self.settings[settings_prefix] = {}
                self.enable_loop = False
                return

    async def cog_load(self):
        '''
        Load cog task
        '''
        if self.enable_loop:
            self._task = self.bot.loop.create_task(self.main_loop())

    async def cog_unload(self):
        '''
        Unload cog task
        '''
        if self.enable_loop and self._task:
            self._task.cancel()

    async def main_loop(self):
        '''
        Our main loop for the task object
        '''
        await self.bot.wait_until_ready()

        while not self.bot.is_closed():
            try:
                await self.__main_loop()
            except ExitEarlyException:
                return
            except Exception as e:
                # Sometimes the main loop can exit and there seems to be no real reason
                # Add in some logging on top just to print out errors
                self.logger.exception(e)
                print(f'Player loop exception {str(e)}')
                return

    async def __main_loop(self):
        '''
        Actual code that will contain main loop. This is here to be overriden
        '''
        await sleep(.01)

    async def check_user_role(self, ctx):
        '''
        Check if user has proper role to run command

        ctx : Standard context
        '''
        try:
            allowed_roles = self.settings['general_allowed_roles'][ctx.guild.id]
        except KeyError:
            # No settings set, assume true
            return True
        # First check if channel key set, if not see if we have an all
        try:
            channel_role = allowed_roles[ctx.channel.id]
        except KeyError:
            try:
                channel_role = allowed_roles['all']
            except KeyError:
                self.logger.warning(f'No role settings for channel {ctx.channel.id}, assuming false')
                return False
        channel_roles = channel_role.split(';;;')
        for role in ctx.author.roles:
            if role.name in channel_roles:
                return True
        return False
