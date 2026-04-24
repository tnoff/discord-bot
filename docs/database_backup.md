# Database Backup

The database backup cog validates S3 configuration at startup and exposes a `DatabaseBackupClient` for use by the CLI commands. Backups and restores are triggered manually (or via an external scheduler such as a Kubernetes CronJob) rather than by the bot itself.

## Configuration

To enable database backup, add the following to your config:

```yaml
general:
  storage:
    backend: s3

  include:
    database_backup: true

database_backup:
  bucket_name: my-db-backups
  object_prefix: "backups/db/"  # optional, default: backups/db/
```

### Configuration Options

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `bucket_name` | yes | — | S3 bucket to store backups |
| `object_prefix` | no | `backups/db/` | S3 key prefix for backup objects |

## CLI Commands

### `discord-db-backup`

Creates a backup of all SQLAlchemy-defined tables and uploads it to S3.

```
discord-db-backup CONFIG_FILE
```

Example:

```bash
discord-db-backup config.yaml
# Backup uploaded to s3://my-db-backups/backups/db/db_backup_2024-01-15_02-00-00.json
```

### `discord-db-restore`

Downloads a backup from S3 and restores the database. All existing data is cleared before restoring.

```
discord-db-restore CONFIG_FILE [--key S3_KEY]
```

If `--key` is omitted, the most recent backup under `object_prefix` is used.

Example:

```bash
# Restore the latest backup
discord-db-restore config.yaml

# Restore a specific backup
discord-db-restore config.yaml --key backups/db/db_backup_2024-01-15_02-00-00.json
```

> **Warning:** Restore deletes all existing data before inserting backup rows. Run this only before starting the bot, not while it is running.

## Scheduling Backups

Backups are not scheduled by the bot. Use an external scheduler:

**Kubernetes CronJob example:**

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: discord-db-backup
spec:
  schedule: "0 2 * * *"  # daily at 2 AM
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: backup
            image: your-discord-bot-image
            command: ["discord-db-backup", "/config/config.yaml"]
          restartPolicy: OnFailure
```

## AWS Credentials

The backup system uses boto3's default credential chain:

- Environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
- IAM role (EC2/ECS/EKS)
- AWS credentials file (`~/.aws/credentials`)

## Backup Format

Backups are JSON files with this structure:

```json
{
  "_metadata": {
    "backup_timestamp": "2024-01-15_02-00-00",
    "alembic_version": "abc123",
    "table_count": 8
  },
  "table_name_1": [
    {"column1": "value1", "column2": "value2"}
  ],
  "table_name_2": [
    {"id": 1, "name": "example"}
  ]
}
```

All non-serializable values (dates, etc.) are converted to strings. Only tables defined in `discord_bot.database.BASE` are included; system tables like `alembic_version` are excluded from the table data but the version is stored in `_metadata` and restored automatically.
