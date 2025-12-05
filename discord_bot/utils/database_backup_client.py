from pathlib import Path
from datetime import datetime
import json
from logging import RootLogger
from sqlalchemy.engine.base import Engine
from sqlalchemy import text

from discord_bot.database import BASE


class DatabaseBackupClient:
    '''
    Handles database table extraction and JSON serialization
    '''
    # Number of rows to fetch at a time (streaming to avoid loading entire table into memory)
    CHUNK_SIZE = 1000

    def __init__(self, db_engine: Engine, logger: RootLogger):
        self.db_engine = db_engine
        self.logger = logger

    def create_backup(self) -> Path:
        '''
        Dumps all database tables to a JSON file using streaming to minimize memory usage
        Only includes tables defined in SQLAlchemy models (BASE.metadata)
        Returns path to the created file
        '''
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        backup_file = Path(f'/tmp/db_backup_{timestamp}.json')

        # Get table names from SQLAlchemy metadata (only tables defined in models)
        table_names = list(BASE.metadata.tables.keys())

        # Write JSON incrementally to avoid loading entire database into memory
        with open(backup_file, 'w', encoding='utf-8') as f:
            f.write('{\n')  # Start JSON object

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
