# Development Conventions

## Code Style and Quality

### Python Code Standards
- Follow PEP 8 style guidelines
- Use type hints for all function parameters and return values
- Maximum line length: 88 characters (Black default)
- Use descriptive variable and function names
- Write docstrings for all public functions and classes

### Code Quality Tools
- **Ruff**: Fast Python linter and formatter
- **MyPy**: Static type checking
- **Pre-commit hooks**: Automated code quality checks

### Import Organization
```python
# Standard library imports
import os
from datetime import datetime
from typing import List, Optional

# Third-party imports
from fastapi import FastAPI, Depends
from pydantic import BaseModel

# Local application imports
from api.core.config import get_settings
from api.v1.models.lineage import ColumnLineageResult
```

## Project Structure

### Directory Organization
```
api/
├── core/           # Core application modules (config, logging)
├── dependencies/   # Dependency injection modules
├── health/         # Health check endpoints
├── v1/            # API version 1
│   ├── models/    # Pydantic data models
│   ├── routers/   # FastAPI route handlers
│   └── services/  # Business logic services
└── main.py        # FastAPI application entry point
```

### File Naming
- Use snake_case for Python files and directories
- Use descriptive names that indicate the module's purpose
- Group related functionality in the same module

## API Design

### RESTful Endpoints
- Use appropriate HTTP methods (GET, POST, PUT, DELETE)
- Use plural nouns for resource collections
- Use consistent URL patterns
- Include version prefix (`/api/v1/`)

### Request/Response Models
- Use Pydantic models for all request and response data
- Include field descriptions and validation rules
- Use appropriate HTTP status codes
- Provide meaningful error messages

### Error Handling
- Use FastAPI's HTTPException for API errors
- Include detailed error messages in development
- Log all errors with appropriate context
- Return consistent error response format

## Database

### Connection Management
- Use connection pooling for database connections
- Implement proper connection cleanup
- Handle database connection failures gracefully
- Use environment-specific connection settings

### Query Patterns
- Use parameterized queries to prevent SQL injection
- Implement proper error handling for database operations
- Log slow queries for performance monitoring
- Use transactions for multi-step operations

## Testing

### Test Organization
```
tests/
├── conftest.py          # Test configuration and fixtures
├── test_health.py       # Health endpoint tests
├── test_lineage_api.py  # Lineage API tests
└── unit/               # Unit tests
    ├── test_services/  # Service layer tests
    └── test_models/    # Model tests
```

### Test Standards
- Write tests for all public APIs
- Use descriptive test names that explain what is being tested
- Use fixtures for common test data
- Mock external dependencies
- Aim for high test coverage (>80%)

### Test Types
- **Unit Tests**: Test individual functions and classes
- **Integration Tests**: Test API endpoints and database interactions
- **Performance Tests**: Test response times and throughput

## Logging

### Structured Logging
- Use structured logging with JSON format in production
- Include relevant context in log messages
- Use appropriate log levels (DEBUG, INFO, WARNING, ERROR)
- Log all API requests and responses

### Log Levels
- **DEBUG**: Detailed diagnostic information
- **INFO**: General operational messages
- **WARNING**: Warning messages for recoverable issues
- **ERROR**: Error messages for serious problems

## Security

### Authentication and Authorization
- Use JWT tokens for API authentication
- Implement role-based access control
- Validate all input data
- Use HTTPS in production

### Data Protection
- Never log sensitive information (passwords, tokens)
- Use environment variables for secrets
- Implement proper CORS configuration
- Validate and sanitize all user input

## Performance

### Optimization Guidelines
- Use async/await for I/O operations
- Implement proper caching strategies
- Use database connection pooling
- Monitor and optimize slow queries
- Use background tasks for long-running operations

### Monitoring
- Implement health check endpoints
- Monitor API response times
- Track error rates and types
- Monitor database connection health

## Documentation

### Code Documentation
- Write clear docstrings for all public functions
- Include parameter and return type information
- Provide usage examples for complex functions
- Keep documentation up to date with code changes

### API Documentation
- Use FastAPI's automatic OpenAPI documentation
- Provide clear descriptions for all endpoints
- Include request/response examples
- Document error responses

## Deployment

### Environment Configuration
- Use environment variables for configuration
- Provide example configuration files
- Document all required environment variables
- Use different configurations for dev/staging/production

### Container Best Practices
- Use multi-stage Docker builds
- Run containers as non-root user
- Implement proper health checks
- Use minimal base images
- Keep container images small

## Git Workflow

### Commit Messages
- Use clear, descriptive commit messages
- Start with a verb in present tense
- Keep first line under 50 characters
- Include detailed description if needed

### Branch Naming
- Use descriptive branch names
- Include ticket/issue numbers when applicable
- Use prefixes: `feature/`, `bugfix/`, `hotfix/`

### Pull Requests
- Write clear PR descriptions
- Include testing instructions
- Request appropriate reviewers
- Ensure all checks pass before merging