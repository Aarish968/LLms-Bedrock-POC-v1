# Repository Analysis API

This document describes the new Repository Analysis API endpoints that have been added to the Column Lineage API.

## Overview

The Repository Analysis API provides endpoints to analyze codebases and generate comprehensive mappings showing how data flows through your application from frontend to backend to database.

## Architecture

The API follows a clean architecture pattern with:

- **Models** (`api/v1/models/repository_analysis.py`): Pydantic models for request/response validation
- **Services** (`api/v1/services/repository_analysis_service.py`): Business logic and job management
- **Routers** (`api/v1/routers/repository_analysis.py`): HTTP endpoints and request handling

## Endpoints

### Base URL
All endpoints are available under `/api/v1/repo-analysis`

### Authentication
Most endpoints require authentication. Public endpoints are available for testing without authentication.

## Available Endpoints

### 1. Start Repository Analysis

**POST** `/api/v1/repo-analysis/analyze`
**POST** `/api/v1/repo-analysis/public/analyze` (public, no auth required)

Start a new repository analysis job that will clone repositories from AWS CodeCommit and analyze them.

**Request Body:**
```json
{
  "frontend_repo_name": "guided-workflow",
  "backend_repo_name": "guided-workflow-backend", 
  "output_filename": "custom_analysis.csv",
  "async_processing": true
}
```

**Response:**
```json
{
  "job_id": "123e4567-e89b-12d3-a456-426614174000",
  "status": "pending",
  "message": "Repository analysis started. Repositories will be cloned and analyzed.",
  "started_at": "2024-01-06T10:30:00Z"
}
```

### 2. Check Job Status

**GET** `/api/v1/repo-analysis/status/{job_id}`
**GET** `/api/v1/repo-analysis/public/status/{job_id}` (public)

Get the status of a repository analysis job.

**Response:**
```json
{
  "job_id": "123e4567-e89b-12d3-a456-426614174000",
  "status": "completed",
  "message": "Analysis completed successfully",
  "output_file": "repo_analysis_20240106_103000.csv",
  "started_at": "2024-01-06T10:30:00Z",
  "completed_at": "2024-01-06T10:35:00Z",
  "error_message": null,
  "frontend_repo_name": "guided-workflow",
  "backend_repo_name": "guided-workflow-backend"
}
```

**Status Values:**
- `pending` - Job is queued for processing
- `cloning` - Repositories are being cloned from AWS CodeCommit
- `running` - Job is currently analyzing the cloned repositories
- `completed` - Job completed successfully
- `failed` - Job failed with an error
- `cancelled` - Job was cancelled by user

### 3. List Jobs

**GET** `/api/v1/repo-analysis/jobs?limit=50&offset=0`
**GET** `/api/v1/repo-analysis/public/jobs?limit=50&offset=0` (public)

List all repository analysis jobs with pagination.

**Query Parameters:**
- `limit` (optional): Number of jobs to return (default: 50)
- `offset` (optional): Number of jobs to skip (default: 0)

### 4. Get Results

**GET** `/api/v1/repo-analysis/results/{job_id}`

Get information about the analysis results file.

**Response:**
```json
{
  "job_id": "123e4567-e89b-12d3-a456-426614174000",
  "status": "completed",
  "output_file": "repo_analysis_20240106_103000.csv",
  "file_size": 1024000,
  "created_at": "2024-01-06T10:35:00Z",
  "modified_at": "2024-01-06T10:35:00Z",
  "message": "Results are ready for download"
}
```

### 5. Cancel Job

**DELETE** `/api/v1/repo-analysis/jobs/{job_id}`

Cancel a running or pending job.

**Response:**
```json
{
  "message": "Job cancelled successfully"
}
```

## Data Models

### AnalysisStatus (Enum)
- `pending`: Job is queued for processing
- `cloning`: Repositories are being cloned from AWS CodeCommit
- `running`: Job is currently analyzing the cloned repositories
- `completed`: Job completed successfully
- `failed`: Job failed with an error
- `cancelled`: Job was cancelled by user

### RepositoryAnalysisRequest
- `frontend_repo_name` (optional): Name of frontend repository to clone (default: "guided-workflow")
- `backend_repo_name` (optional): Name of backend repository to clone (default: "guided-workflow-backend")
- `output_filename` (optional): Custom output filename
- `async_processing` (optional): Process asynchronously (default: true)

### RepositoryAnalysisJob
- `job_id`: Unique job identifier
- `status`: Current job status
- `message`: Human-readable status message
- `output_file`: Output filename (when completed)
- `started_at`: Job start timestamp
- `completed_at`: Job completion timestamp (optional)
- `error_message`: Error details (if failed)
- `frontend_repo_name`: Frontend repository name being analyzed
- `backend_repo_name`: Backend repository name being analyzed

## Usage Examples

### Using curl

1. **Start an analysis:**
```bash
curl -X POST "http://localhost:8000/api/v1/repo-analysis/public/analyze" \
  -H "Content-Type: application/json" \
  -d '{
    "frontend_repo_name": "guided-workflow",
    "backend_repo_name": "guided-workflow-backend",
    "output_filename": "my_analysis.csv"
  }'
```

2. **Check status:**
```bash
curl "http://localhost:8000/api/v1/repo-analysis/public/status/123e4567-e89b-12d3-a456-426614174000"
```

3. **List all jobs:**
```bash
curl "http://localhost:8000/api/v1/repo-analysis/public/jobs?limit=10"
```

### Using Python requests

```python
import requests
import time

# Start analysis
response = requests.post(
    "http://localhost:8000/api/v1/repo-analysis/public/analyze",
    json={
        "frontend_repo_name": "guided-workflow",
        "backend_repo_name": "guided-workflow-backend",
        "output_filename": "my_analysis.csv"
    }
)
job_data = response.json()
job_id = job_data["job_id"]

# Poll for completion
while True:
    status_response = requests.get(
        f"http://localhost:8000/api/v1/repo-analysis/public/status/{job_id}"
    )
    status_data = status_response.json()
    
    if status_data["status"] in ["completed", "failed", "cancelled"]:
        break
    
    time.sleep(5)  # Wait 5 seconds before checking again

print(f"Job {job_id} finished with status: {status_data['status']}")
if status_data["status"] == "completed":
    print(f"Output file: {status_data['output_file']}")
    print(f"Analyzed repos: {status_data['frontend_repo_name']} & {status_data['backend_repo_name']}")
```

## What the Analysis Does

The repository analysis system now includes automatic repository cloning and comprehensive analysis:

1. **Clone Repositories**: Automatically clones frontend and backend repositories from AWS CodeCommit
   - Creates folder structure: `Cloned_repo/Frontend/{frontend_repo_name}` and `Cloned_repo/Backend/{backend_repo_name}`
   - Uses AWS credentials for authentication
   - Handles repository updates if already cloned

2. **Analyzes Frontend Code**: Uses `CompleteFrontendAnalyzer` to scan TypeScript files in the cloned frontend repository
   - Identifies API calls and HTTP methods
   - Extracts URL patterns and function names
   - Maps frontend functions to API endpoints

3. **Analyzes Backend Code**: Uses `CompleteBackendAnalyzer` to scan Python files in the cloned backend repository
   - Identifies route handlers and decorators
   - Performs deep AST analysis for database operations
   - Extracts table references, stored procedures, and flow calls

4. **Maps Relationships**: Uses `APIMapper` to create comprehensive mappings between frontend and backend
   - Matches frontend API calls to backend route handlers
   - Correlates HTTP methods and URL patterns
   - Identifies unmatched endpoints

5. **Extracts Database Info**: Performs detailed database analysis using `TableColumnExtractor`
   - Identifies database tables accessed by each endpoint
   - Extracts column information from ORM definitions
   - Maps stored procedures and response models
   - Analyzes nested data structures

6. **Generates Enhanced CSV Report**: Uses the `main.py` script with `EnhancedCSVGenerator` to create comprehensive reports with columns:
   - Frontend File, Frontend Function, HTTP Method, Frontend URL
   - Backend File, Backend Function, Backend Route
   - Database Tables, Response Model, Response Fields
   - Nested Fields, Table Column Details, Column Count
   - Relationship Type (single_table, multi_table_join, no_tables, unmatched)

## Prerequisites

1. **Environment Variables**: You need to set AWS CodeCommit credentials in your environment:
   ```bash
   AWS_CODECOMMIT_USERNAME=your_codecommit_username
   AWS_CODECOMMIT_PASSWORD=your_codecommit_password
   AWS_CODECOMMIT_REGION=us-east-1
   ```

2. **Git**: Git must be installed and available in the system PATH

3. **Network Access**: Access to AWS CodeCommit repositories

## Environment Configuration

Create a `.env` file in your project root with the following variables:

```bash
# AWS CodeCommit Configuration
AWS_CODECOMMIT_USERNAME=your_codecommit_username
AWS_CODECOMMIT_PASSWORD=your_codecommit_password
AWS_CODECOMMIT_REGION=us-east-1

# Other configuration...
```

Or set them as system environment variables.

## Output Format

The generated CSV file contains detailed columns created by the `main.py` script using `EnhancedCSVGenerator`:

### CSV Columns (from main.py)
- `Frontend_File` - Source frontend TypeScript file
- `Frontend_Function` - Frontend function making the API call
- `HTTP_Method` - HTTP method (GET, POST, PUT, DELETE, etc.)
- `Frontend_URL` - API endpoint URL pattern
- `Backend_File` - Backend Python file handling the request
- `Backend_Function` - Backend function/route handler name
- `Backend_Route` - Backend route pattern
- `Database_Tables` - Database tables accessed by the endpoint (semicolon-separated)
- `Stored_Procedures` - Stored procedures called (comma-separated)
- `Flow_Calls` - Flow calls made (comma-separated)
- `Response_Model` - Pydantic response model used
- `Response_Fields` - Fields in the response model (comma-separated)
- `Nested_Fields` - Nested object fields in the response (comma-separated)
- `Table_Column_Details` - Detailed column information per table in format `TABLE:[col1,col2,col3]`

### Enhanced Features
- **Table-Column Mapping**: Shows actual database columns for each table
- **ORM Analysis**: Extracts column information from SQLAlchemy ORM definitions
- **Response Model Analysis**: Maps Pydantic response models to database fields
- **Nested Field Support**: Handles complex nested data structures
- **Stored Procedure Tracking**: Identifies stored procedures called by endpoints

## Error Handling

If a job fails, check the `error_message` field in the job status response for details about what went wrong.

Common issues:
- Invalid repository names
- Missing or invalid AWS CodeCommit credentials in environment variables
- Network connectivity issues to AWS CodeCommit
- Git not installed or not in PATH
- Repository access permissions
- Missing dependencies
- File permission issues
- Script execution errors

## Integration

This API integrates with the existing Column Lineage API and follows the same patterns for authentication, logging, and error handling.