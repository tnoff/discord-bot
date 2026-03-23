import asyncio
from asyncio import sleep
from datetime import datetime
from typing import Optional

from discord.ext.commands import Bot
from sqlalchemy.engine.base import Engine
from opentelemetry.metrics import Observation
from opentelemetry.trace import SpanKind
from croniter import croniter
from pydantic import BaseModel

from discord_bot.cogs.common import CogHelper
from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.utils.common import return_loop_runner
from discord_bot.utils.otel import async_otel_span_wrapper, otel_span_wrapper, MetricNaming, AttributeNaming, METER_PROVIDER, create_observable_gauge
from discord_bot.utils.integrations.s3 import upload_file, ObjectStorageException
from discord_bot.utils.database_backup_client import DatabaseBackupClient

# Pydantic config model
class DatabaseBackupConfig(BaseModel):
    '''Database backup configuration'''
    bucket_name: str
    cron_schedule: str
    object_prefix: Optional[str] = None
    restore_on_startup: bool = False

class DatabaseBackup(CogHelper):
    '''
    Database Backup to S3
    '''
    def __init__(self, bot: Bot, settings: dict, db_engine: Engine):
        # Check if enabled
        if not settings.get('general', {}).get('include', {}).get('database_backup', False):
            raise CogMissingRequiredArg('Database backup not enabled')

        # Check storage backend is configured
        storage_backend = settings.get('general', {}).get('storage', {}).get('backend', None)
        if storage_backend != 's3':
            raise CogMissingRequiredArg('Storage backend must be s3 for database backup')

        super().__init__(bot, settings, db_engine,
                         settings_prefix='database_backup',
                         config_model=DatabaseBackupConfig)

        # Load config from Pydantic model
        self.bucket_name = self.config.bucket_name
        self.cron_schedule = self.config.cron_schedule
        self.object_prefix = self.config.object_prefix if self.config.object_prefix else 'backups/db/'

        # Initialize backup client
        self.backup_client = DatabaseBackupClient(
            db_engine=self.db_engine,
        )

        self._task = None

        # OpenTelemetry heartbeat gauge
        create_observable_gauge(
            METER_PROVIDER,
            MetricNaming.HEARTBEAT.value,
            self.__loop_active_callback,
            'Database backup loop heartbeat'
        )

    def __loop_active_callback(self, _options):
        '''Check if backup loop is running'''
        value = 1 if (self._task and not self._task.done()) else 0
        return [
            Observation(value, attributes={
                AttributeNaming.BACKGROUND_JOB.value: 'database_backup_check'
            })
        ]

    async def cog_load(self):
        '''Start the backup loop when cog loads'''
        if self.config.restore_on_startup:
            await asyncio.to_thread(self._restore_on_startup)
        self._task = self.bot.loop.create_task(
            return_loop_runner(
                self.database_backup_loop,
                self.bot,
                self.logger
            )()
        )

    def _restore_on_startup(self):
        '''Sync: download and restore the latest S3 backup. Runs in a thread.'''
        with otel_span_wrapper('database_backup.startup_restore'):
            try:
                key = self.backup_client.find_latest_backup(self.bucket_name, self.object_prefix)
                if key is None:
                    self.logger.info('No backup found in S3, starting with empty DB')
                    return
                stats = self.backup_client.restore_from_s3(self.bucket_name, key)
                self.logger.info(
                    f'Startup restore complete from {key}: '
                    f'{stats["tables_restored"]} tables, '
                    f'{stats["total_rows_inserted"]} rows'
                )
            except ObjectStorageException as e:
                self.logger.warning(f'Startup restore failed, continuing with existing DB: {e}')

    async def cog_unload(self):
        '''Cancel backup task when cog unloads'''
        if self._task:
            self._task.cancel()

    async def database_backup_loop(self):
        '''Main backup loop - runs on cron schedule'''
        # Calculate next run time
        cron = croniter(self.cron_schedule, datetime.now())
        next_run = cron.get_next(datetime)
        seconds_until = (next_run - datetime.now()).total_seconds()

        self.logger.info(f'Next database backup scheduled for {next_run} ({seconds_until:.0f}s)')
        await sleep(seconds_until)

        # Run the backup with OpenTelemetry tracing
        async with async_otel_span_wrapper('database_backup.run', kind=SpanKind.INTERNAL):
            # Create backup file
            async with async_otel_span_wrapper('database_backup.create_file'):
                backup_file_path = await asyncio.to_thread(self.backup_client.create_backup)

            # Upload to S3
            async with async_otel_span_wrapper('database_backup.upload_to_s3',
                                               attributes={'s3.bucket': self.bucket_name}):
                object_name = f'{self.object_prefix}{backup_file_path.name}'
                success = await asyncio.to_thread(upload_file, self.bucket_name, backup_file_path, object_name)

                if success:
                    self.logger.info(f'Successfully uploaded backup to s3://{self.bucket_name}/{object_name}')
                else:
                    self.logger.error('Failed to upload backup to S3')

            # Cleanup local file
            backup_file_path.unlink()
