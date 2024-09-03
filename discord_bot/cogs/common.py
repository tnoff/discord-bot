from discord.ext import commands
from jsonschema import ValidationError
from sqlalchemy.orm import sessionmaker

from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.utils import validate_config

class CogHelper(commands.Cog):
    '''
    Cogs usually have the following bits
    '''

    def __init__(self, bot, logger, settings, db_engine, settings_prefix=None, section_schema=None):
        '''
        Init a basic cog
        bot                 :   Discord bot object
        logger              :   Common python logger obj
        settings            :   Common settings config
        db_engine           :   (Optional) Sqlalchemy db engine
        settings_prefix     :   (Optional) Settings prefix, will load settings if given
        section_schema      :   (Optional) Json schema to use to validate config. settings_prefix must also be given
        '''
        # Check that prefix given if schema also given
        if section_schema and not settings_prefix:
            raise CogMissingRequiredArg('Section schema given but settings prefix not given')

        self.bot = bot
        self.logger = logger
        self.settings = settings
        self.db_engine = db_engine
        self.db_session = None
        if self.db_engine:
            self.db_session = sessionmaker(bind=db_engine)()

        # Task object for loops
        self._task = None

        # Setup config
        if section_schema:
            try:
                validate_config(self.settings[settings_prefix], section_schema)
            except ValidationError as exc:
                raise CogMissingRequiredArg(f'Invalid config given for {settings_prefix}') from exc
            except KeyError:
                self.settings[settings_prefix] = {}
                self.enable_loop = False
