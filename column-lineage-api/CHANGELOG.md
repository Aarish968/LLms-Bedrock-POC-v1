# Changelog

All notable changes to the Column Lineage API project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial project structure and FastAPI application
- Column lineage analysis endpoints
- SQL DDL parsing with SQLGlot
- Snowflake database integration
- JWT authentication system
- Health check endpoints
- Background job processing
- Export functionality (CSV, JSON, Excel)
- Comprehensive logging with structured logs
- Docker containerization
- AWS CodeBuild integration
- Pre-commit hooks for code quality
- Unit and integration tests
- API documentation with OpenAPI

### Features
- **POST /api/v1/lineage/analyze** - Start column lineage analysis
- **GET /api/v1/lineage/status/{job_id}** - Check analysis job status
- **GET /api/v1/lineage/results/{job_id}** - Get analysis results
- **GET /api/v1/lineage/views** - List available database views
- **POST /api/v1/lineage/export/{job_id}** - Export results in multiple formats
- **GET /health** - Basic health check
- **GET /health/detailed** - Detailed health status with service checks

### Technical Details
- FastAPI 0.116.2 with async support
- SQLGlot for SQL parsing and analysis
- Snowflake database connectivity
- Pydantic models for data validation
- Structured logging with rich console output
- Docker multi-stage builds
- Pre-commit hooks with Ruff linting
- Comprehensive test suite with pytest

## [0.1.0] - 2024-12-28

### Added
- Initial release of Column Lineage API
- Core functionality for database view column lineage analysis
- RESTful API endpoints for lineage operations
- Background job processing system
- Multiple export formats support
- Health monitoring and observability
- Production-ready Docker containerization
- CI/CD pipeline with AWS CodeBuild