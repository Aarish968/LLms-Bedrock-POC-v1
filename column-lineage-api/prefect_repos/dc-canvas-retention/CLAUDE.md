# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Prefect flow for DC Canvas retention that performs a three-stage process:
1. **Deactivate** canvases older than `n_days_deactivate` (default: 70 days) by updating their DDL to return 0 rows and prepending "(DEACTIVATED)" to the canvas name
2. **Delete** canvases older than `n_days_delete` (default: 120 days) that are already deactivated by setting `is_deleted = 'T'` and emitting cleanup events
3. **Soft Deactivate** canvases exceeding cumulative count threshold by setting `enabled = FALSE`

## Development Commands

### Environment Setup
```bash
# Update and sync dependencies (run before deploy or development)
uv lock --upgrade-package common-prefect-next --upgrade-package common-canvas-next && uv sync --all-extras
```

### Testing
```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/test_deactivate_canvases.py

# Run tests with verbose output
uv run pytest -v
```

### Code Quality
```bash
# Run linting (Ruff is configured in pyproject.toml)
uv run ruff check

# Auto-fix linting issues
uv run ruff check --fix

# Format code
uv run ruff format
```

### Deployment
```bash
# Deploy to dev environment
uv run prefect --no-prompt deploy --prefect-file ./prefect.dev.yaml --all

# Deploy to prod environment  
uv run prefect --no-prompt deploy --prefect-file ./prefect.yaml --all
```

## Architecture

### Core Flow Structure
- **Main Flow**: `dc_canvas_retention.main:dc_canvas_retention_flow` - orchestrates the three-stage process
- **Deactivation**: `deactivate_canvases.py` - handles canvas deactivation logic
- **Deletion**: `delete_canvases.py` - handles canvas deletion and cleanup events
- **Soft Deactivation**: `soft_deactivate_canvases.py` - handles canvas soft deactivation by count threshold

### Key Components
- **Settings**: `common/settings.py` - centralizes configuration and DB connection management
- **Models**: `common/models/` - Pydantic models for canvas data and type definitions
- **Queries**: `common/queries.py` - SQL query builders following naming conventions (`query_*`, `get_*`, `make_*`)

### Dependencies
- **Prefect 3.x**: Workflow orchestration
- **Internal packages**: `common-prefect-next`, `common-canvas-next` (from internal git repos)
- **Database**: Snowflake via SQLAlchemy
- **Python**: 3.12+ required

### Database Operations
- Uses SQLAlchemy engine with transaction management (`engine.begin()`)
- Queries `DC_CANVAS_HDR` table and `information_schema.views` for canvas metadata
- Modifies view DDL to deactivate canvases and updates canvas records for deletion

## Development Guidelines

### Code Style (from REVIEW.md)
- Avoid booleans in function signatures - use enums or explicit parameters
- Pass specific parameters to functions rather than Settings objects for better testability
- Use keyword arguments over positional arguments for maintainability
- Open DB transactions outside loops and pass connections to functions
- Follow SQL statement naming: `query_*` for queries, `get_*` for parsing, `make_*` for updates
- Invert if statements for better readability (early returns)

### Testing
- Test fixtures available in `conftest.py` for DB engine and settings
- Tests cover deactivation, deletion, and deployment validation
- Use the dev database warehouse (x_small) for testing

### Prefect Configuration
- Separate YAML configs for dev and prod environments
- Scheduled runs: Dev at 2 AM, Prod at 4 AM (weekdays, America/New_York)
- Uses Kubernetes work pools with AWS S3 for code storage
- Canvas cleanup events are emitted via `common_prefect_next.events.canvas`