# Repository Analysis Integration

This document describes the repository analysis integration in the Column Lineage API.

## Overview

The repository analysis feature provides automated discovery, cloning, and analysis of frontend and backend repositories from AWS CodeCommit. It creates comprehensive mappings between frontend API calls, backend endpoints, and database tables/columns.

## Features

- **Repository Discovery**: Automatically discover repositories from AWS CodeCommit
- **Smart Cloning**: Clone repositories if they don't exist locally
- **Nested Project Detection**: Find frontend/backend projects inside cloned repositories
- **API Mapping**: Map frontend API calls to backend endpoints and database tables
- **Multiple Export Formats**: Export results as CSV, JSON, or Excel
- **Async Processing**: Run analysis jobs asynchronously with progress tracking
- **Job Management**: Track, monitor, and cancel analysis jobs

## API Endpoints

### Start Repository Analysis
```
POST /api/v1/repository-analysis/analyze
```

Start a new repository analysis job with customizable parameters.

**Request Body:**
```json
{
  "analysis_type": "FRONTEND_BACKEND",
  "auto_discover": true,
  "clone_if_missing": true,
  "include_database_analysis": true,
  "max_repositories": 10,
  "async_processing": true,
  "output_formats": ["csv"],
  "credentials_file": "credentials.txt",
  "clone_directory": "auto_cloned_repos"
}
```

**Response:**
```json
{
  "job_id": "123e4567-e89b-12d3-a456-426614174000",
  "status": "PENDING",
  "message": "Repository analysis job started successfully",
  "analysis_type": "FRONTEND_BACKEND",
  "estimated_duration_minutes": 10,
  "results_url": "/api/v1/repository-analysis/123e4567-e89b-12d3-a456-426614174000/results",
  "progress_url": "/api/v1/repository-analysis/123e4567-e89b-12d3-a456-426614174000/progress"
}
```

### Get Job Status
```
GET /api/v1/repository-analysis/jobs/{job_id}
```

Get the current status of an analysis job.

### Get Job Progress
```
GET /api/v1/repository-analysis/jobs/{job_id}/progress
```

Get detailed progress information including percentage completion and current step.

### Get Job Results
```
GET /api/v1/repository-analysis/jobs/{job_id}/results
```

Get the complete results of a finished analysis job.

### Export Results
```
GET /api/v1/repository-analysis/jobs/{job_id}/export?format=csv&include_metadata=true
```

Export analysis results in the specified format (csv, json, excel).

### Discover Repositories
```
GET /api/v1/repository-analysis/discover-repositories
```

Discover available repositories from AWS CodeCommit without running a full analysis.

### List Jobs
```
GET /api/v1/repository-analysis/jobs?status=COMPLETED&limit=10
```

List analysis jobs with optional filtering by status and analysis type.

### Cancel Job
```
POST /api/v1/repository-analysis/jobs/{job_id}/cancel
```

Cancel a running analysis job.

### Delete Job
```
DELETE /api/v1/repository-analysis/jobs/{job_id}
```

Delete a job and its results.

## Analysis Types

- **FRONTEND_BACKEND**: Full frontend-backend analysis with API mapping
- **REPOSITORY_DISCOVERY**: Repository discovery only
- **API_MAPPING**: API endpoint mapping analysis
- **DATABASE_LINEAGE**: Database lineage analysis

## Job Statuses

- **PENDING**: Job created but not started
- **DISCOVERING**: Discovering repositories from CodeCommit
- **CLONING**: Cloning repositories locally
- **ANALYZING**: Performing analysis
- **COMPLETED**: Analysis completed successfully
- **FAILED**: Analysis failed
- **CANCELLED**: Job was cancelled

## Configuration

### Environment Variables

The repository analysis service uses the following configuration:

- `REPOSITORY_ANALYSIS_ENABLED`: Enable/disable repository analysis features
- `DEFAULT_CREDENTIALS_FILE`: Default AWS credentials file path
- `DEFAULT_CLONE_DIRECTORY`: Default directory for cloned repositories
- `MAX_CONCURRENT_JOBS`: Maximum number of concurrent analysis jobs
- `JOB_TIMEOUT_MINUTES`: Timeout for analysis jobs

### AWS Credentials

The service requires AWS credentials for CodeCommit access. Credentials should be provided in a file with the format:

```
username=your-codecommit-username
password=your-codecommit-password
```

## Usage Examples

### Python Client Example

```python
import requests
import time

# Start analysis
response = requests.post(
    "http://localhost:8000/api/v1/repository-analysis/analyze",
    json={
        "analysis_type": "FRONTEND_BACKEND",
        "auto_discover": true,
        "async_processing": true
    },
    headers={"Authorization": "Bearer your-token"}
)

job = response.json()
job_id = job["job_id"]

# Monitor progress
while True:
    progress = requests.get(
        f"http://localhost:8000/api/v1/repository-analysis/jobs/{job_id}/progress",
        headers={"Authorization": "Bearer your-token"}
    ).json()
    
    print(f"Progress: {progress['progress_percentage']:.1f}% - {progress['current_step']}")
    
    if progress["status"] in ["COMPLETED", "FAILED", "CANCELLED"]:
        break
    
    time.sleep(5)

# Get results
if progress["status"] == "COMPLETED":
    results = requests.get(
        f"http://localhost:8000/api/v1/repository-analysis/jobs/{job_id}/results",
        headers={"Authorization": "Bearer your-token"}
    ).json()
    
    print(f"Analysis completed! Found {len(results['api_mappings'])} API mappings")
    
    # Export as CSV
    csv_response = requests.get(
        f"http://localhost:8000/api/v1/repository-analysis/jobs/{job_id}/export?format=csv",
        headers={"Authorization": "Bearer your-token"}
    )
    
    with open("analysis_results.csv", "wb") as f:
        f.write(csv_response.content)
```

### cURL Examples

```bash
# Start analysis
curl -X POST "http://localhost:8000/api/v1/repository-analysis/analyze" \
  -H "Authorization: Bearer your-token" \
  -H "Content-Type: application/json" \
  -d '{
    "analysis_type": "FRONTEND_BACKEND",
    "auto_discover": true
  }'

# Check progress
curl "http://localhost:8000/api/v1/repository-analysis/jobs/{job_id}/progress" \
  -H "Authorization: Bearer your-token"

# Export results
curl "http://localhost:8000/api/v1/repository-analysis/jobs/{job_id}/export?format=csv" \
  -H "Authorization: Bearer your-token" \
  -o analysis_results.csv
```

## Integration Notes

### Dependencies

The repository analysis feature requires the following additional components:

1. **Smart Analyzer Module**: Core analysis logic
2. **Repository Cloner**: AWS CodeCommit integration
3. **Analysis Components**: Frontend/backend parsing logic

### File Structure

```
column-lineage-api/
├── api/
│   └── v1/
│       ├── models/
│       │   └── repository_analysis.py      # Repository analysis data models
│       ├── services/
│       │   └── repository_analysis_service.py  # Repository analysis service
│       └── routers/
│           └── repository_analysis.py      # Repository analysis API routes
├── smart_analyzer.py                  # Core smart analyzer
└── REPOSITORY_ANALYSIS_INTEGRATION.md     # This documentation
```

### Error Handling

The service includes comprehensive error handling:

- Repository discovery failures
- Cloning errors
- Analysis timeouts
- Invalid repository structures
- Missing dependencies

### Performance Considerations

- Analysis jobs run asynchronously to avoid blocking the API
- Repository cloning is cached to avoid redundant operations
- Large repositories may take significant time to analyze
- Consider implementing job queuing for high-volume usage

## Troubleshooting

### Common Issues

1. **Repository Discovery Fails**
   - Check AWS credentials
   - Verify CodeCommit access permissions
   - Ensure credentials file exists and is readable

2. **Cloning Fails**
   - Check network connectivity
   - Verify repository permissions
   - Ensure sufficient disk space

3. **Analysis Fails**
   - Check repository structure
   - Verify required files exist
   - Review error logs for specific issues

4. **Import Errors**
   - Ensure all required modules are available
   - Check Python path configuration
   - Verify dependencies are installed

### Logging

The service uses structured logging with the following levels:

- **INFO**: General operation information
- **WARNING**: Non-critical issues
- **ERROR**: Analysis failures and errors
- **DEBUG**: Detailed debugging information

Enable debug logging for troubleshooting:

```python
import logging
logging.getLogger("api.v1.services.repository_analysis_service").setLevel(logging.DEBUG)
```