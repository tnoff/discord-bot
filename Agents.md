# Agents.md

This file provides guidance to AI agents when working with code in this repository.

## Common Commands

### Development
- **Install dependencies**: `pip install -r requirements.txt`
- **Install package in development mode**: `pip install -e .`
- **Run the bot**: `discord-bot /path/to/config.yml`

### Testing and Quality
- **Run full test suite with coverage**: `tox`
- **Run tests only**: `pytest --cov=discord_bot tests/`
- **Run linting**: `pylint discord_bot/`
- **Run test linting**: `pylint --rcfile .pylintrc.test tests/`

### Database Management
- **Upgrade database to latest**: `alembic upgrade head`
- **Generate new migration**: `alembic revision --autogenerate -m "description"`
- **Note**: Set `DATABASE_URL` environment variable for alembic operations

### Testing Individual Components
- **Run specific test file**: `pytest tests/path/to/test_file.py`
- **Run tests with verbose output**: `pytest -v`

## Architecture Overview

### Core Structure
- **Entry point**: `discord_bot/cli.py` - Main CLI interface and bot initialization
- **Database models**: `discord_bot/database.py` - SQLAlchemy models for all cogs
- **Cogs**: `discord_bot/cogs/` - Modular bot features (music, markov, urban, etc.)
- **Utilities**: `discord_bot/utils/` - Shared utilities, clients, and helpers

### Key Components

#### Bot Framework
- Uses discord.py with cog-based architecture
- Configuration via YAML files with pyaml-env support
- OpenTelemetry instrumentation for observability
- SQLAlchemy for database operations with Alembic migrations

#### Cog System
- **Common/General**: Basic bot commands and utilities
- **Music**: YouTube/Spotify integration with voice channel playback
- **Markov**: Message generation using Markov chains
- **Urban**: Urban Dictionary lookup
- **Delete Messages**: Auto-deletion in specific channels
- **Role**: Role-based access control system

#### Music Cog Architecture
- **Music Player**: `discord_bot/cogs/music_helpers/music_player.py` - Core playback logic
- **Download Client**: `discord_bot/cogs/music_helpers/download_client.py` - yt-dlp integration
- **Search Client**: `discord_bot/cogs/music_helpers/search_client.py` - YouTube/Spotify search
- **Video Cache**: `discord_bot/cogs/music_helpers/video_cache_client.py` - S3-based caching
- **Message Queue**: `discord_bot/cogs/music_helpers/message_queue.py` - Queue management

### Configuration
- Bot token required in config file under `general.discord_token`
- Database connection via `general.sql_connection_statement`
- Cogs enabled via `include` section
- Intents configured via `intents` array
- Logging and OTLP telemetry configurable

### Database Schema
- Shared database models across all cogs in `database.py`
- Markov chains, music history, role assignments stored in SQLAlchemy tables
- Uses Alembic for schema migrations

### Testing Strategy
- Comprehensive test suite in `tests/` directory
- Async test support with `asyncio_mode = strict`
- Mock-heavy testing for Discord API interactions
- Coverage reporting enabled