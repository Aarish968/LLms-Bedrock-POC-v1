# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Package Management
- `uv lock --upgrade` - Upgrade all packages
- `uv lock --upgrade-package common-canvas-next --upgrade-package common-prefect-next && uv sync --all-extras` - Upgrade internal packages and sync dependencies
- `uv sync --all-extras` - Install dependencies with all extras

### Code Quality
- `uv run ruff check` - Run linting
- `uv run ruff format` - Format code
- `uv run pytest` - Run tests

### Prefect Deployments
#### Development
```bash
uv lock --upgrade-package common-canvas-next --upgrade-package common-prefect-next && uv sync --all-extras
uv run prefect --no-prompt deploy --prefect-file ./prefect.dev.yaml --all 
```

#### Production
```bash
uv lock --upgrade-package common-canvas-next --upgrade-package common-prefect-next && uv sync --all-extras
uv run prefect --no-prompt deploy --prefect-file ./prefect.yaml --all 
```

## Architecture Overview

This is a Prefect flows wrapper around `common-canvas-next` repository. The business logic for canvas creation and management is implemented in the common library, while this repository provides the Prefect flow orchestration layer.

### Core Components

- **Flow Modules**: Located in `src/canvas_flows/`, each module contains a single Prefect flow
  - `create_canvas.py` - Creates new canvas instances
  - `rebuild_canvas.py` - Rebuilds existing canvas instances  
  - `refresh_canvas_view.py` - Refreshes canvas SQL views
  - `refresh_engagement_views.py` - Refreshes all canvas views for an engagement

- **Common Module**: `src/canvas_flows/common/` contains shared utilities
  - `settings.py` - Configuration and database connection management
  - `models.py` - Pydantic models for flow parameters

### Dependencies

- **Internal Libraries**: 
  - `common-canvas-next` (>=0.14.0) - Core canvas business logic
  - `common-prefect-next` (>=0.15.0) - Shared Prefect utilities and blocks
- **External Libraries**: Prefect 3.x, SQLAlchemy, Snowflake connector

### Deployment Configuration

- `prefect.dev.yaml` - Development environment deployments
- `prefect.yaml` - Production environment deployments
- Both configurations include event-driven triggers for engagement refresh flows
- Uses S3 for code storage and Kubernetes for execution

### Environment Requirements

- Python 3.12+
- UV package manager
- Access to internal git repositories via AWS CodeCommit
- Prefect server connection
- Snowflake database access

### Logging Configuration

Ensure `PREFECT_LOGGING_EXTRA_LOGGERS: common_canvas_next,canvas_flows` is set in deployment environment variables for proper log visibility.