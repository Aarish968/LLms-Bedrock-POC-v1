# Simplified Repository Analysis API Usage

## Overview

The simplified API now accepts just frontend and backend paths directly. The system will:

1. **Check if paths exist locally** - If they exist, use them directly
2. **Clone from CodeCommit if missing** - Automatically clone repositories if paths don't exist
3. **Run analysis** - Perform frontend-backend mapping analysis
4. **Return results** - Provide API mappings and database lineage

## API Request Format

### Simplified Request Payload

```json
{
  "frontend_path": "D:\\LLms-Bedrock-POC-v1\\column-lineage-api\\cloned-repo\\frontend\\guided-workflow",
  "backend_path": "D:\\LLms-Bedrock-POC-v1\\column-lineage-api\\cloned-repo\\backend\\guided-workflow-backend",
  "include_database_analysis": true,
  "output_format": "csv",
  "credentials_file": "credentials.txt"
}
```

### Field Descriptions

- **`frontend_path`** (required): Full path to frontend project
- **`backend_path`** (required): Full path to backend project  
- **`include_database_analysis`** (optional): Include database table/column analysis (default: true)
- **`output_format`** (optional): Output format - "csv", "json", or "excel" (default: "csv")
- **`credentials_file`** (optional): AWS credentials file path (default: "credentials.txt")

## API Endpoints

### 1. Start Analysis

```bash
curl -X POST "http://localhost:8000/api/v1/repository-analysis/analyze" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your-token" \
  -d '{
    "frontend_path": "D:\\LLms-Bedrock-POC-v1\\column-lineage-api\\cloned-repo\\frontend\\guided-workflow",
    "backend_path": "D:\\LLms-Bedrock-POC-v1\\column-lineage-api\\cloned-repo\\backend\\guided-workflow-backend",
    "include_database_analysis": true,
    "output_format": "csv"
  }'
```

**Response:**
```json
{
  "job_id": "123e4567-e89b-12d3-a456-426614174000",
  "status": "PENDING",
  "message": "Repository analysis job started successfully. Analyzing: D:\\LLms-Bedrock-POC-v1\\column-lineage-api\\cloned-repo\\frontend\\guided-workflow and D:\\LLms-Bedrock-POC-v1\\column-lineage-api\\cloned-repo\\backend\\guided-workflow-backend",
  "analysis_type": "FRONTEND_BACKEND",
  "estimated_duration_minutes": 5,
  "results_url": "/api/v1/repository-analysis/123e4567-e89b-12d3-a456-426614174000/results",
  "progress_url": "/api/v1/repository-analysis/123e4567-e89b-12d3-a456-426614174000/progress"
}
```

### 2. Check Progress

```bash
curl -X GET "http://localhost:8000/api/v1/repository-analysis/jobs/{job_id}/progress" \
  -H "Authorization: Bearer your-token"
```

**Response:**
```json
{
  "job_id": "123e4567-e89b-12d3-a456-426614174000",
  "status": "ANALYZING",
  "progress_percentage": 75.0,
  "current_step": "Analyzing frontend-backend mappings",
  "total_repositories": 2,
  "discovered_repositories": 2,
  "cloned_repositories": 2,
  "analyzed_repositories": 1,
  "failed_repositories": 0,
  "elapsed_time_seconds": 45.2,
  "estimated_remaining_seconds": 15.1
}
```

### 3. Get Results

```bash
curl -X GET "http://localhost:8000/api/v1/repository-analysis/jobs/{job_id}/results" \
  -H "Authorization: Bearer your-token"
```

**Response:**
```json
{
  "job_id": "123e4567-e89b-12d3-a456-426614174000",
  "status": "COMPLETED",
  "analysis_type": "FRONTEND_BACKEND",
  "repositories": [
    {
      "repository_name": "guided-workflow",
      "repository_type": "FRONTEND",
      "local_path": "D:\\LLms-Bedrock-POC-v1\\column-lineage-api\\cloned-repo\\frontend\\guided-workflow"
    },
    {
      "repository_name": "guided-workflow-backend",
      "repository_type": "BACKEND",
      "local_path": "D:\\LLms-Bedrock-POC-v1\\column-lineage-api\\cloned-repo\\backend\\guided-workflow-backend"
    }
  ],
  "api_mappings": [
    {
      "frontend_call": "api.get('/workflows')",
      "backend_endpoint": "/api/v1/workflows",
      "http_method": "GET",
      "database_tables": ["workflows", "workflow_steps"],
      "database_columns": ["id", "name", "status", "created_at"],
      "confidence_score": 0.95
    }
  ],
  "summary": {
    "total_repositories": 2,
    "total_api_mappings": 15,
    "frontend_path": "D:\\LLms-Bedrock-POC-v1\\column-lineage-api\\cloned-repo\\frontend\\guided-workflow",
    "backend_path": "D:\\LLms-Bedrock-POC-v1\\column-lineage-api\\cloned-repo\\backend\\guided-workflow-backend",
    "execution_time_seconds": 62.3,
    "include_database_analysis": true
  },
  "output_files": ["repository_analysis_123e4567.csv"],
  "execution_time_seconds": 62.3,
  "created_at": "2024-01-15T10:30:45.123456"
}
```

### 4. Export Results

```bash
curl -X GET "http://localhost:8000/api/v1/repository-analysis/jobs/{job_id}/export?format=csv&include_metadata=true" \
  -H "Authorization: Bearer your-token" \
  -o "analysis_results.csv"
```

## How It Works

### Path Resolution Logic

1. **Local Check**: System checks if `frontend_path` and `backend_path` exist locally
2. **Auto-Clone**: If paths don't exist, extracts repository name from path and clones from CodeCommit
3. **Validation**: Verifies paths exist after cloning
4. **Analysis**: Runs the enhanced CSV generator on the resolved paths

### Example Scenarios

#### Scenario 1: Both Paths Exist Locally
```json
{
  "frontend_path": "D:\\existing\\frontend\\project",
  "backend_path": "D:\\existing\\backend\\project"
}
```
→ Uses existing local paths directly

#### Scenario 2: Paths Don't Exist (Will Clone)
```json
{
  "frontend_path": "D:\\cloned-repo\\frontend\\guided-workflow",
  "backend_path": "D:\\cloned-repo\\backend\\guided-workflow-backend"
}
```
→ Clones `guided-workflow` and `guided-workflow-backend` from CodeCommit

#### Scenario 3: Mixed (One Exists, One Needs Cloning)
```json
{
  "frontend_path": "D:\\existing\\frontend\\project",
  "backend_path": "D:\\cloned-repo\\backend\\new-backend"
}
```
→ Uses existing frontend, clones `new-backend` from CodeCommit

## Status Flow

1. **PENDING** → Job created, waiting to start
2. **DISCOVERING** → Checking if paths exist locally
3. **CLONING** → Cloning missing repositories from CodeCommit
4. **ANALYZING** → Running frontend-backend analysis
5. **COMPLETED** → Analysis finished successfully
6. **FAILED** → Analysis failed (check error_message)

## Error Handling

Common error scenarios:
- **Invalid paths**: Path format is incorrect
- **Repository not found**: Repository doesn't exist in CodeCommit
- **Clone failure**: Network issues or permission problems
- **Analysis failure**: Code parsing or analysis errors

Check the `error_message` field in the job status for detailed error information.

## Python Example

```python
import requests
import time

# Start analysis
response = requests.post("http://localhost:8000/api/v1/repository-analysis/analyze", 
    json={
        "frontend_path": "D:\\LLms-Bedrock-POC-v1\\column-lineage-api\\cloned-repo\\frontend\\guided-workflow",
        "backend_path": "D:\\LLms-Bedrock-POC-v1\\column-lineage-api\\cloned-repo\\backend\\guided-workflow-backend",
        "include_database_analysis": True,
        "output_format": "csv"
    },
    headers={"Authorization": "Bearer your-token"}
)

job_id = response.json()["job_id"]

# Monitor progress
while True:
    progress = requests.get(f"http://localhost:8000/api/v1/repository-analysis/jobs/{job_id}/progress",
        headers={"Authorization": "Bearer your-token"}
    ).json()
    
    print(f"Status: {progress['status']} ({progress['progress_percentage']:.1f}%)")
    
    if progress['status'] in ['COMPLETED', 'FAILED']:
        break
    
    time.sleep(5)

# Get results
if progress['status'] == 'COMPLETED':
    results = requests.get(f"http://localhost:8000/api/v1/repository-analysis/jobs/{job_id}/results",
        headers={"Authorization": "Bearer your-token"}
    ).json()
    
    print(f"Found {len(results['api_mappings'])} API mappings")
```

This simplified approach makes it much easier to use the API with known paths while still providing the flexibility to auto-clone missing repositories.