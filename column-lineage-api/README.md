# Column Lineage API

A FastAPI-based backend service for analyzing database view column lineage. This service provides RESTful APIs to trace column dependencies from database views to their source tables and columns.

## Overview

This system automatically analyzes Snowflake database views and traces each column back to its source table and column, handling complex SQL patterns including CTEs, JOINs, derived columns, and window functions.

## Technology Stack

- **Framework**: FastAPI 0.116.2
- **Python Version**: >=3.10
- **Package Manager**: UV (recommended) with uv.lock
- **Database**: Snowflake with SQLAlchemy 2.0.43
- **Authentication**: AWS Cognito JWT
- **Data Processing**: Pandas, SQLGlot for SQL parsing
- **Cloud Services**: Boto3 for AWS integration
- **Documentation**: FastAPI auto-generated OpenAPI docs
- **Testing**: Pytest with async support
- **Code Quality**: Ruff for linting and formatting
- **Containerization**: Docker with multi-stage builds

## Quick Start

### Option 1: Using UV (Recommended)
```bash
# Install dependencies
uv sync

# Run development server
uv run uvicorn api.main:app --reload
python -m uv run uvicorn api.main:app --reload
```

### Option 2: Using pip
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -e .

# For development dependencies
pip install -e .[dev]

# Run development server
python -m uvicorn api.main:app --reload
```

### Option 3: Using the install script
```bash
python install.py
```

2. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

3. Access the API documentation:
- OpenAPI docs: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## API Endpoints

### Column Lineage Analysis
- `POST /api/v1/lineage/analyze` - Start column lineage analysis
- `GET /api/v1/lineage/status/{job_id}` - Check analysis status
- `GET /api/v1/lineage/results/{job_id}` - Get analysis results
- `GET /api/v1/lineage/views` - List available views
- `GET /api/v1/lineage/export/{job_id}` - Export results as CSV

### Health & Monitoring
- `GET /health` - Health check endpoint
- `GET /health/detailed` - Detailed health status

## Features

- **Automated View Discovery**: Automatically discovers views from Snowflake
- **Complex SQL Parsing**: Handles CTEs, JOINs, derived columns, window functions
- **Async Processing**: Background job processing for large datasets
- **Export Capabilities**: CSV export of lineage results
- **Comprehensive Logging**: Structured logging with request tracing
- **Error Handling**: Graceful error handling and reporting

## Development

See [CONVENTIONS.md](CONVENTIONS.md) for development guidelines and coding standards.