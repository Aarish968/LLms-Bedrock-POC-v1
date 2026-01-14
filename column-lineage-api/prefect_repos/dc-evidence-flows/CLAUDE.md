# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository contains the `evidence-flows` Python package, which implements Prefect flows for handling uploads of evidence data for DC (Data Canvas) engagements. The primary flow (`create_evidence_customer_flow`) processes customer evidence files containing serial numbers or instance IDs.

## Development Environment

### Setup and Dependencies

```bash
# Install dependencies
uv sync --all-extras

# Update specific dependencies
uv lock --upgrade-package common-prefect-next --upgrade-package common-serial-resolution && uv sync --all-extras
```

### Testing

```bash
# Run all tests
uv run pytest

# Run a specific test file
uv run pytest tests/test_instance_tagging_flow.py

# Run tests with specific marker
uv run pytest -m ddl
```

### Linting and Formatting

```bash
# Run ruff linter
uv run ruff check .

# Format code with ruff
uv run ruff format .
```

## Deployment

### Deploy to Dev Environment

```bash
uv lock --upgrade-package common-prefect-next && uv sync --all-extras
uv run prefect --no-prompt deploy --prefect-file ./prefect.dev.yaml --all
```

### Deploy to Prod Environment

```bash
uv lock --upgrade-package common-prefect-next && uv sync --all-extras
uv run prefect --no-prompt deploy --prefect-file ./prefect.yaml --all
```

## Architecture

### Core Components

1. **Prefect Flows**
   - Main flow: `create_evidence_customer_flow` in `evidence_flows.main`
   - Triggered from the `/api/v2/workflows/lookups/tag-history` endpoint
   - Generates tag history reports

2. **Data Models**
   - Located in `evidence_flows.common.models`
   - Supports two schema types: `instance_id` and `serial_number`
   - Core payload models: `SerialNumberPayload` and `InstanceIdPayload`
   - Uses Pydantic for validation

3. **Handlers and Processing**
   - Serial number resolution: `handle_customer_serial_payload` 
   - Instance ID validation: `handle_customer_instance_payload`
   - Creates header entries and loads customer details into the database

4. **Database Integration**
   - Uses SQLAlchemy for database operations
   - Works with Snowflake through snowflake-sqlalchemy
   - Environment-specific schemas: `CPS_DSCI_BR` (dev) and `CPS_DSCI_API` (prod)

### Key Workflows

1. Processing begins when a payload is received with customer evidence data
2. The flow validates and processes the data based on schema type (serial number or instance ID)
3. Creates header entries in the database
4. Loads detailed customer data
5. Sends notifications on completion

## Logging

To ensure logs appear from the flow:
- Include `PREFECT_LOGGING_EXTRA_LOGGERS: evidence_flows` in deployments
- Snowflake logging is included in the base image if using common_prefect_next

## Code Conventions

- Python 3.12+ is required
- Uses typed parameters and returns (PEP 585 style)
- Pydantic v2 for data validation
- Enums implemented as `StrEnum` classes with `__str__` methods
- Soft delete flags in SQL tables with `IS_DELETED` defaulted to 'F'
- Environment is defined as a `StrEnum` with members 'dev' and 'prod'