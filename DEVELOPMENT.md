# Development Documentation

The tests use the `pytest` framework, and there is linting through `pylint`. There are separate sources for the linting in the main code vs the tests.

Use `tox` to instrument the entire test suite.

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

## Alembic Database Upgrades

Generate a revision

```
$ alembic revision --autogenerate -m "description of change"
```