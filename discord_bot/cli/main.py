import click

from discord_bot.cli.bot import run as bot_run
from discord_bot.cli.common import parse_and_validate_config
from discord_bot.cli.dispatcher import run as dispatcher_run
from discord_bot.cli.download_worker import run as download_worker_run


@click.command()
@click.argument('config_file', type=click.Path(dir_okay=False))
def main(config_file):
    '''
    Main entry point — routes to bot or dispatcher process based on config.
    '''
    settings, general_config = parse_and_validate_config(config_file)

    if general_config.dispatch_gateway:
        bot_run(settings, general_config)
    else:
        dispatcher_run(settings, general_config)


@click.command()
@click.argument('config_file', type=click.Path(dir_okay=False))
def download_worker(config_file):
    '''
    Standalone download worker — reads MediaRequests from a Redis Stream and downloads them.
    '''
    settings, general_config = parse_and_validate_config(config_file)
    download_worker_run(settings, general_config)


if __name__ == '__main__':
    main()  #pylint:disable=no-value-for-parameter
