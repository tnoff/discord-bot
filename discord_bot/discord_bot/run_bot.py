import argparse
import os

from discord.ext import commands
from youtube_dl import YoutubeDL

from discord_bot.cogs.music import Music
from discord_bot.cogs.general import General
from discord_bot.cogs.planner import Planner
from discord_bot.cogs.role import RoleAssign
from discord_bot.defaults import CONFIG_PATH_DEFAULT
from discord_bot.utils import get_logger, load_args, get_db_session

def parse_args():
    '''
    Basic cli arg parser
    '''
    parser = argparse.ArgumentParser(description="Discord Bot Runner")
    parser.add_argument("--config-file", "-c", default=CONFIG_PATH_DEFAULT,
                        help="Config file")
    parser.add_argument("--log-file", "-l",
                        help="Logging file")
    parser.add_argument("--discord-token", "-t",
                        help="Discord token, defaults to DISCORD_TOKEN env arg")
    parser.add_argument("--download-dir", "-d", default="/tmp/",
                        help="Directory for downloading youtube files")
    return parser.parse_args()

def main():
    '''
    Main loop
    '''
    settings = load_args(vars(parse_args()))

    # Setup vars
    logger = get_logger(__name__, settings['log_file'])
    bot = commands.Bot(command_prefix='!')
    # Setup database
    db_session = get_db_session(settings)

    ytdlopts = {
        'format': 'bestaudio/best',
        'outtmpl': os.path.join(settings['download_dir'],
                                '%(extractor)s-%(id)s-%(title)s.%(ext)s'),
        'restrictfilenames': True,
        'noplaylist': True,
        'nocheckcertificate': True,
        'ignoreerrors': False,
        'logtostderr': False,
        'logger': logger,
        'default_search': 'auto',
        'source_address': '0.0.0.0'  # ipv6 addresses cause issues sometimes
    }
    ytdl = YoutubeDL(ytdlopts)

    # Run bot
    bot.add_cog(Music(bot, db_session, logger, ytdl))
    bot.add_cog(RoleAssign(bot, db_session, logger))
    bot.add_cog(Planner(bot, db_session, logger))
    bot.add_cog(General(bot, logger))
    bot.run(settings['discord_token'])
