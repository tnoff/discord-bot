import click

from discord_bot.cli.common import parse_and_validate_config


@click.command()
@click.argument('config_file', type=click.Path(dir_okay=False))
def main(config_file):
    '''
    Main entry point — routes to bot or dispatcher process based on config.
    '''
    settings, general_config = parse_and_validate_config(config_file)

    if general_config.dispatch_gateway:
        from discord_bot.cli.bot import run  #pylint:disable=import-outside-toplevel
    else:
        from discord_bot.cli.dispatcher import run  #pylint:disable=import-outside-toplevel

    run(settings, general_config)


if __name__ == '__main__':
    main()  #pylint:disable=no-value-for-parameter
