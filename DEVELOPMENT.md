# Development Documentation

The tests use the `pytest` framework, and there is linting through `pylint`. There are separate sources for the linting in the main code vs the tests.

Use `tox` to instrument the entire test suite.

## System Dependencies

`ffmpeg` must be installed and on `PATH` for music-related tests and the music cog to function.

```
# Debian/Ubuntu
$ apt install ffmpeg

# macOS
$ brew install ffmpeg
```

`deno` is required for local development. See the [installation guide](https://docs.deno.com/runtime/getting_started/installation/) for instructions.

## Installation

Install the deps

```
$ pip install -r requirements.txt -r tests/requirements.txt
```

It's recommended you install these within a virtual directory.

```
$ virtualenv .venv
$ source .venv/bin/activate
# Run pip commands
```

## Run Test Suite

Run the entire test suite including linting
```
$ tox
```

## Run Tests

Run just the tests

```
$ pytest tests/
```

### Tests With Coverage

Run tests with coverage

```
$ pytest --cov=discord_bot --cov-report=html tests
```

This will drop html files of coverage into the `htmlcov` directory.

## Entrypoints

The package exposes two CLI entrypoints defined in `setup.py`:

| Command | Module | Description |
|---------|--------|-------------|
| `discord-bot` | `discord_bot.cli:main` | Full bot (Discord gateway + all cogs) |
| `discord-bot-download-worker` | `discord_bot.cli:download_worker` | Standalone download worker (no Discord gateway) |

Run the download worker locally:

```bash
discord-bot-download-worker /path/to/worker.cnf
```

See [CLI documentation](./docs/cli.md#standalone-download-worker) for config details.

## Alembic Database Upgrades

Generate a revision

```
$ alembic revision --autogenerate -m "description of change"
```