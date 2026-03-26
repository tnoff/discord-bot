from collections.abc import Callable
from datetime import datetime
import json
import logging
from pathlib import Path
import sqlite3
import tempfile

import ijson
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine

from discord_bot.database import BASE
from discord_bot.utils.integrations.s3 import get_file, list_objects
from discord_bot.utils.otel import otel_span_wrapper

logger = logging.getLogger(__name__)

class DatabaseBackupClient:
    '''
    Handles database backup and restoration with JSON serialization
    '''
    # Number of rows to fetch at a time (streaming to avoid loading entire table into memory)
    CHUNK_SIZE = 1000
    # Number of rows to insert at a time during restoration (batching for efficiency)
    BATCH_SIZE = 1000

    def __init__(self, db_engine: Engine):
        self.db_engine = db_engine

    def _create_sqlite_snapshot(self) -> tuple:
        '''
        Copies the live SQLite database to a temporary file using SQLite's internal
        backup API, then returns a (snapshot_engine, snapshot_path) tuple.
        The snapshot is taken by borrowing the raw DBAPI connection from the pool,
        so it works for both file-based and in-memory databases.
        The caller is responsible for deleting snapshot_path and disposing snapshot_engine.
        '''
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            snap_path = Path(f.name)
        dst_conn = sqlite3.connect(str(snap_path))
        try:
            with self.db_engine.connect() as src_conn:
                src_conn.connection.dbapi_connection.backup(dst_conn)
        finally:
            dst_conn.close()
        snap_engine = create_engine(f'sqlite:///{snap_path}')
        return snap_engine, snap_path

    def _get_alembic_version(self) -> str:
        '''
        Get the current alembic migration version from the database
        Returns None if alembic_version table doesn't exist
        '''
        try:
            with self.db_engine.connect() as connection:
                result = connection.execute(text('SELECT version_num FROM alembic_version'))
                row = result.fetchone()
                return row[0] if row else None
        except Exception:  # pylint: disable=broad-except
            # Table doesn't exist or other error
            return None

    def create_backup(self) -> Path:
        '''
        Dumps all database tables to a JSON file using streaming to minimize memory usage
        Only includes tables defined in SQLAlchemy models (BASE.metadata)
        Returns path to the created file
        '''
        with otel_span_wrapper('database_backup_client.create_backup') as span:
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

            # Get table names from SQLAlchemy metadata (only tables defined in models)
            table_names = list(BASE.metadata.tables.keys())

            # Get alembic version
            alembic_version = self._get_alembic_version()

            # For SQLite, snapshot the DB first so the live connection is released
            # before the (potentially slow) JSON export begins.
            if self.db_engine.dialect.name == 'sqlite':
                read_engine, snap_path = self._create_sqlite_snapshot()
            else:
                read_engine = self.db_engine
                snap_path = None

            try:
                # Create temporary file for backup (delete=False so it persists after closing)
                with tempfile.NamedTemporaryFile(mode='w', prefix=f'db_backup_{timestamp}_',
                                                  suffix='.json', delete=False, encoding='utf-8') as f:
                    backup_file = Path(f.name)
                    # Write JSON incrementally to avoid loading entire database into memory
                    f.write('{\n')  # Start JSON object

                    # Write metadata as first entry
                    f.write('  "_metadata": {\n')
                    f.write(f'    "backup_timestamp": "{timestamp}",\n')
                    f.write(f'    "alembic_version": {json.dumps(alembic_version)},\n')
                    f.write(f'    "table_count": {len(table_names)}\n')
                    f.write('  }')
                    if table_names:
                        f.write(',\n')  # Comma before tables if any exist

                    with read_engine.connect() as connection:
                        for table_idx, table_name in enumerate(table_names):
                            if table_idx > 0:
                                f.write(',\n')  # Comma separator between tables

                            logger.debug(f'Backing up table: {table_name}')
                            f.write(f'  "{table_name}": [\n')

                            # Stream rows in chunks to minimize memory usage
                            row_count = 0
                            result = connection.execution_options(stream_results=True).execute(
                                text(f'SELECT * FROM {table_name}')
                            )

                            while True:
                                # Fetch chunk of rows
                                chunk = result.fetchmany(self.CHUNK_SIZE)
                                if not chunk:
                                    break

                                # Write each row as JSON
                                for row in chunk:
                                    if row_count > 0:
                                        f.write(',\n')

                                    row_dict = dict(row._mapping)  #pylint:disable=protected-access
                                    # Write row with proper indentation
                                    f.write('    ' + json.dumps(row_dict, default=str))
                                    row_count += 1

                            logger.debug(f'  -> {table_name}: {row_count} rows')
                            f.write('\n  ]')  # Close table array

                    f.write('\n}\n')  # Close JSON object

                file_size = backup_file.stat().st_size
                span.set_attributes({'backup.table_count': len(table_names), 'backup.file_size_bytes': file_size})
                logger.info(f'Created backup file: {backup_file} ({file_size} bytes)')
                return backup_file
            finally:
                if snap_path is not None:
                    if snap_path.exists():
                        snap_path.unlink()
                    read_engine.dispose()

    # ijson scalar event types (all carry a Python value directly)
    _SCALAR_EVENTS = frozenset(['null', 'boolean', 'integer', 'double', 'number', 'string'])

    def restore_backup(self, backup_file: Path, clear_existing: bool = False,
                       table_groups: list[list[str]] | None = None,
                       on_table_restored: Callable[[str], None] | None = None) -> dict:
        '''
        Restores database tables from a JSON backup file.
        Uses streaming JSON parsing to avoid loading the entire file into memory.

        Args:
            backup_file: Path to the JSON backup file
            clear_existing: If True, truncates tables before restoring (default: False)
            table_groups: If provided, restore in multiple passes — one per group — with
                          per-table transactions. Tables not in any group are restored first.
            on_table_restored: Called with each table name after its transaction commits.

        Returns:
            Dictionary with restoration statistics
        '''
        if not backup_file.exists():
            raise FileNotFoundError(f'Backup file not found: {backup_file}')

        logger.info(f'Starting database restoration from {backup_file}')

        metadata = {}
        stats = {
            'tables_restored': 0,
            'total_rows_inserted': 0,
            'tables': {},
            'metadata': metadata,
        }
        table_metadata = BASE.metadata.tables

        if table_groups is None:
            # Original behavior: single pass inside a single transaction
            with self.db_engine.begin() as connection:
                if clear_existing:
                    self._truncate_tables(connection, list(table_metadata.keys()))
                self._restore_pass_single(backup_file, connection, stats, metadata)
        else:
            # Multi-pass: truncate first, then restore ungrouped tables, then each group
            if clear_existing:
                with self.db_engine.begin() as connection:
                    self._truncate_tables(connection, list(table_metadata.keys()))

            grouped = {t for group in table_groups for t in group}
            ungrouped = set(table_metadata.keys()) - grouped

            if ungrouped:
                self._restore_pass(backup_file, ungrouped, on_table_restored, stats, metadata)

            for group in table_groups:
                self._restore_pass(backup_file, set(group), on_table_restored, stats, metadata)

        if metadata:
            logger.info(
                f'Backup metadata: timestamp={metadata.get("backup_timestamp", "unknown")} '
                f'alembic={metadata.get("alembic_version", "unknown")} '
                f'tables={metadata.get("table_count", "unknown")}'
            )

        logger.info(f'Restoration complete: {stats["tables_restored"]} tables, '
                        f'{stats["total_rows_inserted"]} total rows')
        return stats

    def _restore_pass_single(self, backup_file: Path, connection, stats: dict,
                              metadata: dict) -> None:
        '''Single streaming pass using an existing connection (original behavior).'''
        table_metadata = BASE.metadata.tables

        current_table = None
        current_row = None
        row_buffer = []
        table_rows_inserted = 0

        def flush_buffer():
            nonlocal table_rows_inserted
            if row_buffer and current_table in table_metadata:
                table_rows_inserted += self._restore_table(connection, current_table, row_buffer)
                row_buffer.clear()

        def finalize_table():
            nonlocal table_rows_inserted
            flush_buffer()
            if current_table and current_table != '_metadata':
                if current_table not in table_metadata:
                    logger.info(f'Table {current_table} not found in current schema, skipping')
                else:
                    stats['tables'][current_table] = table_rows_inserted
                    stats['tables_restored'] += 1
                    stats['total_rows_inserted'] += table_rows_inserted
                    logger.info(f'Restored {current_table}: {table_rows_inserted} rows')
            table_rows_inserted = 0

        with open(backup_file, 'rb') as f:
            for prefix, event, value in ijson.parse(f, use_float=True):
                if prefix == '' and event == 'map_key':
                    finalize_table()
                    current_table = value
                    current_row = None

                elif current_table == '_metadata' and event in self._SCALAR_EVENTS:
                    field = prefix[len('_metadata.'):]
                    if field:
                        metadata[field] = value

                elif current_table and current_table != '_metadata':
                    item_prefix = f'{current_table}.item'

                    if prefix == item_prefix and event == 'start_map':
                        current_row = {}

                    elif prefix == item_prefix and event == 'end_map':
                        if current_row is not None:
                            row_buffer.append(current_row)
                            current_row = None
                            if len(row_buffer) >= self.BATCH_SIZE:
                                flush_buffer()

                    elif (current_row is not None
                          and event in self._SCALAR_EVENTS
                          and prefix.startswith(item_prefix + '.')):
                        field = prefix[len(item_prefix) + 1:]
                        if field and '.' not in field:
                            current_row[field] = value  #pylint:disable=unsupported-assignment-operation

        finalize_table()

    def _restore_pass(self, backup_file: Path, only_tables: set[str],
                      on_table_restored: Callable | None, stats: dict,
                      metadata: dict) -> None:
        '''
        One streaming pass over backup_file, restoring only tables in only_tables.
        Each table is committed in its own transaction; on_table_restored is called after commit.
        '''
        table_metadata = BASE.metadata.tables

        current_table = None
        current_row = None
        row_buffer = []
        table_rows_inserted = 0

        def flush_buffer():
            nonlocal table_rows_inserted
            if not row_buffer:
                return
            if current_table not in only_tables or current_table not in table_metadata:
                row_buffer.clear()
                return
            rows_to_insert = list(row_buffer)
            row_buffer.clear()
            with self.db_engine.begin() as connection:
                table_rows_inserted += self._restore_table(connection, current_table, rows_to_insert)

        def finalize_table():
            nonlocal table_rows_inserted
            flush_buffer()
            if current_table and current_table != '_metadata' and current_table in only_tables:
                if current_table not in table_metadata:
                    logger.info(f'Table {current_table} not found in current schema, skipping')
                else:
                    stats['tables'][current_table] = table_rows_inserted
                    stats['tables_restored'] += 1
                    stats['total_rows_inserted'] += table_rows_inserted
                    logger.info(f'Restored {current_table}: {table_rows_inserted} rows')
                    if on_table_restored:
                        on_table_restored(current_table)
            table_rows_inserted = 0

        with open(backup_file, 'rb') as f:
            for prefix, event, value in ijson.parse(f, use_float=True):
                if prefix == '' and event == 'map_key':
                    finalize_table()
                    current_table = value
                    current_row = None

                elif current_table == '_metadata' and event in self._SCALAR_EVENTS:
                    field = prefix[len('_metadata.'):]
                    if field:
                        metadata[field] = value

                elif current_table and current_table != '_metadata':
                    if current_table not in only_tables:
                        continue

                    item_prefix = f'{current_table}.item'

                    if prefix == item_prefix and event == 'start_map':
                        current_row = {}

                    elif prefix == item_prefix and event == 'end_map':
                        if current_row is not None:
                            row_buffer.append(current_row)
                            current_row = None
                            if len(row_buffer) >= self.BATCH_SIZE:
                                flush_buffer()

                    elif (current_row is not None
                          and event in self._SCALAR_EVENTS
                          and prefix.startswith(item_prefix + '.')):
                        field = prefix[len(item_prefix) + 1:]
                        if field and '.' not in field:
                            current_row[field] = value  #pylint:disable=unsupported-assignment-operation

        finalize_table()

    def _truncate_tables(self, connection, table_names: list):
        '''
        Truncates tables in the correct order (respecting foreign key constraints)
        '''
        with otel_span_wrapper('database_backup_client.truncate_tables',
                               attributes={'db.table_count': len(table_names)}):
            # Disable foreign key constraints temporarily (database-specific)
            # For SQLite
            try:
                connection.execute(text('PRAGMA foreign_keys = OFF'))
            except Exception:  # pylint: disable=broad-except
                pass  # Not SQLite or already disabled

            for table_name in table_names:
                if table_name in BASE.metadata.tables:
                    logger.debug(f'Truncating table: {table_name}')
                    try:
                        connection.execute(text(f'DELETE FROM {table_name}'))
                    except Exception as e:  # pylint: disable=broad-except
                        logger.debug(f'Failed to truncate {table_name}: {str(e)}')

            # Re-enable foreign key constraints
            try:
                connection.execute(text('PRAGMA foreign_keys = ON'))
            except Exception:  # pylint: disable=broad-except
                pass

    def _restore_table(self, connection, table_name: str, rows: list) -> int:
        '''
        Restores a single table from backup data

        Args:
            connection: Database connection
            table_name: Name of the table to restore
            rows: List of row dictionaries

        Returns:
            Number of rows inserted
        '''
        with otel_span_wrapper('database_backup_client.restore_table',
                               attributes={'db.table': table_name}) as span:
            if not rows:
                logger.debug(f'  -> {table_name}: No rows to restore')
                return 0

            rows_inserted = 0

            # Insert in batches
            for i in range(0, len(rows), self.BATCH_SIZE):
                batch = rows[i:i + self.BATCH_SIZE]

                # Get column names from first row
                columns = list(batch[0].keys())
                column_str = ', '.join(columns)
                placeholders = ', '.join([f':{col}' for col in columns])

                insert_sql = f'INSERT INTO {table_name} ({column_str}) VALUES ({placeholders})'

                try:
                    connection.execute(text(insert_sql), batch)
                    rows_inserted += len(batch)
                except Exception as e:  # pylint: disable=broad-except
                    logger.error(f'Failed to insert batch into {table_name}: {str(e)}')
                    # Continue with next batch instead of failing completely
                    continue

            span.set_attributes({'db.rows_inserted': rows_inserted})
            logger.debug(f'  -> {table_name}: {rows_inserted} rows restored')
            return rows_inserted

    def find_latest_backup(self, bucket_name: str, prefix: str) -> str | None:
        '''
        Returns the S3 key of the most recent backup object under prefix, or None if none exist.
        '''
        objects = list_objects(bucket_name, prefix)
        if not objects:
            return None
        return objects[0]['key']

    def restore_from_s3(self, bucket_name: str, object_key: str,
                        table_groups: list[list[str]] | None = None,
                        on_table_restored: Callable[[str], None] | None = None) -> dict:
        '''
        Downloads the backup object from S3 to a temp file and restores the database from it.
        Returns the restoration stats dict.
        '''
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as tmp:
            tmp_path = Path(tmp.name)
        try:
            get_file(bucket_name, object_key, tmp_path)
            return self.restore_backup(tmp_path, clear_existing=True,
                                       table_groups=table_groups,
                                       on_table_restored=on_table_restored)
        finally:
            if tmp_path.exists():
                tmp_path.unlink()
