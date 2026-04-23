'''
CLI entry points for database backup and restore.

  discord-db-backup config.yaml
  discord-db-restore config.yaml [--key backups/db/db_backup_2024-01-01_00-00-00.json]

Both commands connect to the database and S3 bucket specified in the config.
The storage backend must be set to `s3`.
'''
import asyncio
import sys

import click
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from discord_bot.cli.common import make_async_db_url, parse_and_validate_config
from discord_bot.database import BASE
from discord_bot.exceptions import DiscordBotException
from discord_bot.clients.database_backup_client import DatabaseBackupClient
from discord_bot.utils.integrations.s3 import upload_file


def _build_engine(general_config):
    '''Return an async engine from the config, raising DiscordBotException if not configured.'''
    if not general_config.sql_connection_statement:
        raise DiscordBotException('sql_connection_statement is required in config')
    return create_async_engine(make_async_db_url(general_config.sql_connection_statement),
                               poolclass=NullPool)


def _get_backup_settings(settings):
    '''Return (bucket_name, object_prefix) from settings or raise DiscordBotException.'''
    storage_backend = settings.get('general', {}).get('storage', {}).get('backend')
    if storage_backend != 's3':
        raise DiscordBotException('Storage backend must be s3 for database backup/restore')
    db_backup = settings.get('database_backup', {})
    bucket_name = db_backup.get('bucket_name')
    if not bucket_name:
        raise DiscordBotException('database_backup.bucket_name is required in config')
    object_prefix = db_backup.get('object_prefix', 'backups/db/')
    return bucket_name, object_prefix


@click.command()
@click.argument('config_file', type=click.Path(dir_okay=False, exists=True))
def backup_main(config_file):
    '''
    Create a database backup and upload it to S3.

    Connects to the database and S3 bucket specified in CONFIG_FILE, dumps all
    SQLAlchemy-defined tables to a JSON file, and uploads it to S3 under the
    configured object_prefix.
    '''
    try:
        settings, general_config = parse_and_validate_config(config_file)
        bucket_name, object_prefix = _get_backup_settings(settings)
    except DiscordBotException as e:
        click.echo(f'Error: {e}', err=True)
        sys.exit(1)

    async def run():
        engine = _build_engine(general_config)
        try:
            async with engine.begin() as conn:
                await conn.run_sync(BASE.metadata.create_all)
            client = DatabaseBackupClient(db_engine=engine)
            backup_file = await client.create_backup()
            try:
                object_name = f'{object_prefix}{backup_file.name}'
                upload_file(bucket_name, backup_file, object_name)
                click.echo(f'Backup uploaded to s3://{bucket_name}/{object_name}')
            finally:
                if backup_file.exists():
                    backup_file.unlink()
        finally:
            await engine.dispose()

    try:
        asyncio.run(run())
    except DiscordBotException as e:
        click.echo(f'Error: {e}', err=True)
        sys.exit(1)
    except Exception as e:  # pylint: disable=broad-except
        click.echo(f'Backup failed: {e}', err=True)
        sys.exit(1)


@click.command()
@click.argument('config_file', type=click.Path(dir_okay=False, exists=True))
@click.option('--key', default=None, metavar='S3_KEY',
              help='S3 object key to restore from. Defaults to the most recent backup.')
def restore_main(config_file, key):
    '''
    Restore the database from an S3 backup.

    Downloads the specified backup (or the most recent one if --key is not given),
    clears all existing data, and restores from the JSON backup file.

    WARNING: This deletes all existing data in the database before restoring.
    '''
    try:
        settings, general_config = parse_and_validate_config(config_file)
        bucket_name, object_prefix = _get_backup_settings(settings)
    except DiscordBotException as e:
        click.echo(f'Error: {e}', err=True)
        sys.exit(1)

    async def run():
        engine = _build_engine(general_config)
        try:
            async with engine.begin() as conn:
                await conn.run_sync(BASE.metadata.create_all)
            client = DatabaseBackupClient(db_engine=engine)

            object_key = key
            if not object_key:
                object_key = client.find_latest_backup(bucket_name, object_prefix)
                if not object_key:
                    click.echo(f'No backups found under s3://{bucket_name}/{object_prefix}', err=True)
                    sys.exit(1)
                click.echo(f'Using latest backup: {object_key}')

            click.echo(f'Restoring from s3://{bucket_name}/{object_key} ...')
            stats = await client.restore_from_s3(bucket_name, object_key)

            click.echo(f'Restored {stats["tables_restored"]} tables, '
                       f'{stats["total_rows_inserted"]} total rows.')
            for table_name, row_count in stats['tables'].items():
                click.echo(f'  {table_name}: {row_count} rows')
        finally:
            await engine.dispose()

    try:
        asyncio.run(run())
    except DiscordBotException as e:
        click.echo(f'Error: {e}', err=True)
        sys.exit(1)
    except Exception as e:  # pylint: disable=broad-except
        click.echo(f'Restore failed: {e}', err=True)
        sys.exit(1)
