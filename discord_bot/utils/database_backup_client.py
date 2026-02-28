from pathlib import Path
from datetime import datetime
import json
import logging
import tempfile
from sqlalchemy.engine.base import Engine
from sqlalchemy import text

from discord_bot.database import BASE


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
        self.logger = logging.getLogger('databasebackup')

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
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

        # Get table names from SQLAlchemy metadata (only tables defined in models)
        table_names = list(BASE.metadata.tables.keys())

        # Get alembic version
        alembic_version = self._get_alembic_version()

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

            for table_idx, table_name in enumerate(table_names):
                if table_idx > 0:
                    f.write(',\n')  # Comma separator between tables

                self.logger.debug(f'Backing up table: {table_name}')
                f.write(f'  "{table_name}": [\n')

                # Stream rows in chunks to minimize memory usage
                row_count = 0
                with self.db_engine.connect() as connection:
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

                self.logger.debug(f'  -> {row_count} rows')
                f.write('\n  ]')  # Close table array

            f.write('\n}\n')  # Close JSON object

        file_size = backup_file.stat().st_size
        self.logger.info(f'Created backup file: {backup_file} ({file_size} bytes)')
        return backup_file

    def restore_backup(self, backup_file: Path, clear_existing: bool = False) -> dict:
        '''
        Restores database tables from a JSON backup file

        Args:
            backup_file: Path to the JSON backup file
            clear_existing: If True, truncates tables before restoring (default: False)

        Returns:
            Dictionary with restoration statistics
        '''
        if not backup_file.exists():
            raise FileNotFoundError(f'Backup file not found: {backup_file}')

        self.logger.info(f'Starting database restoration from {backup_file}')

        # Load backup data
        with open(backup_file, 'r', encoding='utf-8') as f:
            backup_data = json.load(f)

        # Extract and log metadata if present
        metadata = backup_data.get('_metadata', {})
        if metadata:
            self.logger.info('Backup metadata:')
            self.logger.info(f'  Timestamp: {metadata.get("backup_timestamp", "unknown")}')
            self.logger.info(f'  Alembic version: {metadata.get("alembic_version", "unknown")}')
            self.logger.info(f'  Table count: {metadata.get("table_count", "unknown")}')

        stats = {
            'tables_restored': 0,
            'total_rows_inserted': 0,
            'tables': {},
            'metadata': metadata
        }

        # Get table metadata from SQLAlchemy
        table_metadata = BASE.metadata.tables

        # Get table names to restore (exclude metadata)
        table_names_to_restore = [name for name in backup_data.keys() if name != '_metadata']

        with self.db_engine.begin() as connection:
            # Clear existing data if requested
            if clear_existing:
                self._truncate_tables(connection, table_names_to_restore)

            # Restore each table
            for table_name, rows in backup_data.items():
                # Skip metadata entry
                if table_name == '_metadata':
                    continue
                if table_name not in table_metadata:
                    self.logger.warning(f'Table {table_name} not found in current schema, skipping')
                    continue

                self.logger.info(f'Restoring table: {table_name}')
                rows_inserted = self._restore_table(connection, table_name, rows)

                stats['tables'][table_name] = rows_inserted
                stats['tables_restored'] += 1
                stats['total_rows_inserted'] += rows_inserted

        self.logger.info(f'Restoration complete: {stats["tables_restored"]} tables, '
                        f'{stats["total_rows_inserted"]} total rows')
        return stats

    def _truncate_tables(self, connection, table_names: list):
        '''
        Truncates tables in the correct order (respecting foreign key constraints)
        '''
        # Disable foreign key constraints temporarily (database-specific)
        # For SQLite
        try:
            connection.execute(text('PRAGMA foreign_keys = OFF'))
        except Exception:  # pylint: disable=broad-except
            pass  # Not SQLite or already disabled

        for table_name in table_names:
            if table_name in BASE.metadata.tables:
                self.logger.info(f'Truncating table: {table_name}')
                try:
                    connection.execute(text(f'DELETE FROM {table_name}'))
                except Exception as e:  # pylint: disable=broad-except
                    self.logger.warning(f'Failed to truncate {table_name}: {str(e)}')

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
        if not rows:
            self.logger.debug(f'  -> {table_name}: No rows to restore')
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
                self.logger.error(f'Failed to insert batch into {table_name}: {str(e)}')
                # Continue with next batch instead of failing completely
                continue

        self.logger.debug(f'  -> {table_name}: {rows_inserted} rows restored')
        return rows_inserted
