#!/usr/bin/env python3
'''
Database Restore Script

Restores a Discord bot database from a JSON backup file.
Can restore from a local file or download from S3.

Usage:
    # Restore from a local file
    python scripts/restore_database.py --config config.yaml --file /path/to/backup.json

    # Restore from S3
    python scripts/restore_database.py --config config.yaml --bucket my-bucket --object backups/db/backup.json

    # Clear existing data before restoring
    python scripts/restore_database.py --config config.yaml --file backup.json --clear
'''

import argparse
import logging
import sys
from pathlib import Path
import tempfile

from pyaml_env import parse_config
from sqlalchemy import create_engine

from discord_bot.database import BASE
from discord_bot.utils.database_backup_client import DatabaseBackupClient
from discord_bot.utils.clients.s3 import get_file


def setup_logging(verbose: bool = False):
    '''Configure logging'''
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger('database_restore')


def parse_args():
    '''Parse command line arguments'''
    parser = argparse.ArgumentParser(
        description='Restore Discord bot database from backup',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        '--config',
        type=str,
        required=True,
        help='Path to bot configuration YAML file'
    )

    # Source options (mutually exclusive)
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        '--file',
        type=str,
        help='Path to local backup JSON file'
    )
    source_group.add_argument(
        '--s3',
        nargs=2,
        metavar=('BUCKET', 'OBJECT'),
        help='S3 bucket and object path (e.g., --s3 my-bucket backups/db/backup.json)'
    )

    # Restore options
    parser.add_argument(
        '--clear',
        action='store_true',
        help='Clear existing data before restoring (CAUTION: deletes all data)'
    )

    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Enable verbose logging'
    )

    return parser.parse_args()


def main():
    '''Main restore script'''
    args = parse_args()
    logger = setup_logging(args.verbose)

    # Load configuration
    logger.info(f'Loading configuration from {args.config}')
    try:
        settings = parse_config(args.config)
    except Exception as e:
        logger.error(f'Failed to load configuration: {e}')
        sys.exit(1)

    # Create database engine
    try:
        sql_connection = settings['general']['sql_connection_statement']
        db_engine = create_engine(sql_connection, pool_pre_ping=True)
        BASE.metadata.create_all(db_engine)
        BASE.metadata.bind = db_engine
        logger.info('Database connection established')
    except KeyError:
        logger.error('Unable to find sql_connection_statement in settings')
        sys.exit(1)
    except Exception as e:
        logger.error(f'Failed to create database engine: {e}')
        sys.exit(1)

    # Initialize backup client
    backup_client = DatabaseBackupClient(db_engine=db_engine, logger=logger)

    # Get backup file
    backup_file = None
    temp_file = None

    try:
        if args.file:
            # Local file
            backup_file = Path(args.file)
            if not backup_file.exists():
                logger.error(f'Backup file not found: {backup_file}')
                sys.exit(1)
            logger.info(f'Using local backup file: {backup_file}')

        elif args.s3:
            # Download from S3
            bucket_name, object_path = args.s3
            logger.info(f'Downloading backup from s3://{bucket_name}/{object_path}')

            # Create temporary file for download
            temp_file = tempfile.NamedTemporaryFile(
                mode='w',
                suffix='.json',
                delete=False,
                prefix='db_restore_'
            )
            temp_file.close()
            backup_file = Path(temp_file.name)

            # Download file
            try:
                success = get_file(bucket_name, object_path, backup_file)
            except Exception as e:
                logger.error(f'Failed to download backup from S3: {e}')
                sys.exit(1)

            logger.info(f'Downloaded to temporary file: {backup_file}')

        # Confirm if clearing existing data
        if args.clear:
            logger.warning('WARNING: --clear option will DELETE ALL EXISTING DATA')
            response = input('Are you sure you want to continue? (yes/no): ')
            if response.lower() not in ['yes', 'y']:
                logger.info('Restore cancelled by user')
                sys.exit(0)

        # Perform restoration
        logger.info('Starting database restoration...')
        stats = backup_client.restore_backup(backup_file, clear_existing=args.clear)

        # Print statistics
        logger.info('=' * 60)
        logger.info('Restoration Summary:')

        # Display backup metadata if present
        if stats.get('metadata'):
            logger.info('')
            logger.info('Backup Information:')
            metadata = stats['metadata']
            logger.info(f'  Created: {metadata.get("backup_timestamp", "unknown")}')
            logger.info(f'  Alembic version: {metadata.get("alembic_version", "unknown")}')
            logger.info('')

        logger.info(f'  Tables restored: {stats["tables_restored"]}')
        logger.info(f'  Total rows inserted: {stats["total_rows_inserted"]}')
        logger.info('')
        logger.info('Per-table breakdown:')
        for table_name, row_count in stats['tables'].items():
            logger.info(f'  {table_name}: {row_count} rows')
        logger.info('=' * 60)
        logger.info('Database restoration completed successfully!')

    except Exception as e:
        logger.exception(f'Restoration failed: {e}')
        sys.exit(1)

    finally:
        # Clean up temporary file if it was created
        if temp_file and backup_file and backup_file.exists():
            try:
                backup_file.unlink()
                logger.debug(f'Cleaned up temporary file: {backup_file}')
            except Exception as e:
                logger.warning(f'Failed to clean up temporary file: {e}')


if __name__ == '__main__':
    main()
