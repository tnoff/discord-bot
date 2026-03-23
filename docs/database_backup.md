# Database Backup

The database backup cog automatically backs up all SQLAlchemy-defined tables to S3 on a configurable cron schedule.

## Features

- **Scheduled backups**: Uses cron expressions to schedule automatic backups
- **Startup restore**: Optionally downloads and restores the latest S3 backup on container start
- **Consistent snapshots**: All tables are read within a single SQLite connection/transaction, preventing cross-table inconsistency
- **Memory efficient**: Streams data in chunks to minimize memory usage
- **S3 integration**: Uploads backups to S3 using existing storage infrastructure
- **Model-only backups**: Only backs up tables defined in SQLAlchemy models (excludes system tables)

## Configuration

To enable database backups, add the following to your config:

```yaml
general:
  # Shared storage configuration
  storage:
    backend: s3

  # Database backup configuration
  database_backup:
    bucket_name: my-db-backups
    cron_schedule: "0 2 * * *"  # Daily at 2 AM
    object_prefix: "backups/db/"  # Optional: S3 path prefix
    restore_on_startup: true     # Optional: restore latest backup on startup

  include:
    database_backup: true
```

### Configuration Options

#### Required

- `bucket_name` (string): S3 bucket name for storing backups
- `cron_schedule` (string): Cron expression for backup schedule

#### Optional

- `object_prefix` (string): S3 object key prefix (default: `backups/db/`)
- `restore_on_startup` (bool): If `true`, downloads and restores the most recent backup from S3 before starting the backup loop (default: `false`). Useful for ephemeral/container deployments where the database volume is wiped on restart. If no backup exists in S3, startup continues normally with an empty database. S3 errors are logged as warnings and do not prevent startup.

### Cron Schedule Examples

```yaml
"0 2 * * *"      # Daily at 2:00 AM
"0 */6 * * *"    # Every 6 hours
"0 0 * * 0"      # Weekly on Sunday at midnight
"*/30 * * * *"   # Every 30 minutes (for testing)
```

## AWS Credentials

The backup system uses boto3's default credential chain. Configure credentials via:

- Environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
- IAM role (if running on EC2/ECS)
- AWS credentials file (`~/.aws/credentials`)

## How It Works

1. **Initialization**: Cog loads and validates configuration
2. **Startup restore** (if `restore_on_startup: true`):
   - Lists objects under `object_prefix` in S3, sorted by last-modified date
   - Downloads the most recent backup to a temporary file
   - Restores all tables (clearing existing data first)
   - Deletes the temporary file
   - If no backup exists or S3 is unreachable, logs a warning and continues
3. **Scheduling**: Calculates next run time from cron expression
4. **Backup process**:
   - Opens a single database connection so all table reads share one consistent SQLite WAL snapshot
   - Queries all tables defined in SQLAlchemy models (`BASE.metadata`)
   - Streams rows in chunks of 1000 to minimize memory usage
   - Writes JSON incrementally to `/tmp/db_backup_YYYY-MM-DD_HH-MM-SS.json`
   - Uploads to S3: `s3://bucket_name/object_prefix/db_backup_YYYY-MM-DD_HH-MM-SS.json`
   - Cleans up local file
5. **Repeat**: Schedules next backup based on cron expression

## Backup Format

Backups are stored as JSON with the following structure:

```json
{
  "table_name_1": [
    {"column1": "value1", "column2": "value2"},
    {"column1": "value3", "column2": "value4"}
  ],
  "table_name_2": [
    {"id": 1, "name": "example"}
  ]
}
```

All dates and non-serializable types are converted to strings.

## Tables Included

Only tables defined in SQLAlchemy models (`discord_bot.database.BASE`) are backed up:

- `markov_channel`
- `markov_relation`
- `playlist`
- `playlist_item`
- `video_cache`
- `video_cache_backup`
- `guild`
- `guild_video_analytics`

System tables (like `alembic_version`) are automatically excluded.