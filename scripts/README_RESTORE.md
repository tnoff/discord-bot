# Database Restore Script

This script restores your Discord bot database from backups created by the `database_backup` cog.

## Requirements

- Python 3.10 or higher
- Bot configuration file (YAML)
- A backup file (local or in S3)

## Basic Usage

### Restore from a Local File

```bash
python scripts/restore_database.py --config config.yaml --file /path/to/backup.json
```

### Restore from S3

```bash
python scripts/restore_database.py --config config.yaml --s3 my-bucket backups/db/db_backup_2025-01-15_12-30-00.json
```

### Clear Existing Data Before Restoring

**WARNING:** This will delete all existing data in your database!

```bash
python scripts/restore_database.py --config config.yaml --file backup.json --clear
```

### Verbose Logging

```bash
python scripts/restore_database.py --config config.yaml --file backup.json --verbose
```

## Command-Line Options

| Option | Description |
|--------|-------------|
| `--config PATH` | **Required.** Path to your bot configuration YAML file |
| `--file PATH` | Path to local backup JSON file |
| `--s3 BUCKET OBJECT` | S3 bucket name and object path |
| `--clear` | Clear all existing data before restoring (requires confirmation) |
| `--verbose`, `-v` | Enable verbose debug logging |

## How It Works

1. **Loads your configuration** - Reads database connection settings from your config file
2. **Gets the backup file** - Either uses a local file or downloads from S3
3. **Validates the backup** - Ensures the backup file exists and is valid JSON
4. **Displays backup metadata** - Shows when the backup was created and the alembic migration version
5. **Restores data** - Inserts data into database tables in batches (1000 rows at a time)
6. **Reports statistics** - Shows how many tables and rows were restored

## Backup Metadata

Each backup file includes metadata that helps you verify compatibility before restoring:

- **backup_timestamp**: When the backup was created
- **alembic_version**: The database schema version (alembic migration ID)
- **table_count**: Number of tables included in the backup

This metadata is displayed during restoration but is not used for validation. It's your responsibility to ensure the backup's alembic version matches your current database schema.

### Backup File Structure

Backup files are JSON with the following structure:

```json
{
  "_metadata": {
    "backup_timestamp": "2025-01-15_03-00-00",
    "alembic_version": "0f696315a882",
    "table_count": 8
  },
  "guild": [
    {"id": 1, "server_id": "123456789"},
    {"id": 2, "server_id": "987654321"}
  ],
  "playlist": [
    {"id": 1, "name": "My Playlist", "server_id": "123456789", ...}
  ],
  ...
}
```

The `_metadata` entry is always first and is automatically skipped during restoration.

## Restore Modes

### Merge Mode (Default)

By default, the script **merges** backup data with existing data. New rows are added, but existing rows are preserved.

```bash
python scripts/restore_database.py --config config.yaml --file backup.json
```
### Clear Mode

With the `--clear` flag, the script **deletes all existing data** before restoring.

```bash
python scripts/restore_database.py --config config.yaml --file backup.json --clear
```

**Note:** The script will ask for confirmation before clearing data.
