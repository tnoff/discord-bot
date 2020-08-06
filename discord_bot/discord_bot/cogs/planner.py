from discord.ext import commands

from discord_bot.cogs.common import CogHelper
from discord_bot.database import Server, User


class Planner(CogHelper):
    '''
    Assistant for planning events
    '''

    @commands.group(name='planner', invoke_without_command=False)
    async def planner(self, ctx):
        '''
        Planner functions
        '''
        if ctx.invoked_subcommand is None:
            await ctx.send('Invalid sub command passed...')

    @planner.command(name='register')
    async def register(self, ctx):
        '''
        Register yourself with planning service
        '''
        server = self.db_session.query(Server).get(ctx.guild.id)
        if server:
            self.logger.info(f'Found server matching id {server.id}')
        else:
            server_args = {
                'id' : ctx.guild.id,
                'name' : ctx.guild.name,
            }
            self.logger.debug(f'Attempting to create server with args {server_args}')
            server_entry = Server(**server_args)
            self.db_session.add(server_entry)
            self.db_session.commit()
            self.logger.info(f'Created server with id {server_entry.id}')
        # Then check for user
        user = self.db_session.query(User).get(ctx.author.id)
        if user:
            self.logger.info(f'Found user matching id {user.id}')
        else:
            user_args = {
                'id' : ctx.author.id,
                'name' : ctx.author.name,
            }
            self.logger.debug(f'Attempting to create user with args {user_args}')
            user_entry = User(**user_args)
            self.db_session.add(user_entry)
            self.db_session.commit()
            self.logger.info(f'Created user with id {user_entry.id}')
            message = 'Successfully registered!'
            return await ctx.send(message)
