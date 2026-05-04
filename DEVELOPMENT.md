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

It's recommended you install within a virtual environment.

```
$ virtualenv .venv
$ source .venv/bin/activate
```

Install the package with the desired extras:

```
$ pip install -e ".[bot,test,sqlite]"
```

Available extras:

| Extra | Use case |
|-------|----------|
| `bot` | Bot-specific dependencies (media, database, etc.) |
| `sqlite` | SQLite async driver |
| `postgres` | PostgreSQL async driver |
| `test` | Test tooling (pytest, pylint, etc.) |

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

## Alembic Database Upgrades

Generate a revision

```
$ alembic revision --autogenerate -m "description of change"
```