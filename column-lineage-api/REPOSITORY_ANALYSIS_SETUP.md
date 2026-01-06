# Repository Analysis Setup Guide

This guide explains how to set up and use the Repository Analysis feature that automatically clones repositories from AWS CodeCommit and generates comprehensive API mapping reports.

## Overview

The Repository Analysis system:
1. **Clones repositories** from AWS CodeCommit automatically
2. **Analyzes frontend** TypeScript code to find API calls
3. **Analyzes backend** Python code to find route handlers and database operations
4. **Maps relationships** between frontend calls and backend endpoints
5. **Generates CSV reports** with detailed table and column information

## Prerequisites

### 1. Environment Setup

Create a `.env` file in the project root with AWS CodeCommit credentials:

```bash
# AWS CodeCommit Configuration
AWS_CODECOMMIT_USERNAME=your_codecommit_username
AWS_CODECOMMIT_PASSWORD=your_codecommit_password
AWS_CODECOMMIT_REGION=us-east-1

# Other configuration...
PROJECT_NAME=Column Lineage API
VERSION=0.1.0
DEBUG=false
```

### 2. System Requirements

- **Python 3.8+** with required packages
- **Git** installed and available in PATH
- **Network access** to AWS CodeCommit
- **Valid AWS CodeCommit credentials**

### 3. Directory Structure

The system creates this folder structure:
```
project_root/
├── Cloned_repo/
│   ├── Frontend/
│   │   └── {frontend_repo_name}/
│   └── Backend/
│       └── {backend_repo_name}/
└── {output_file}.csv
```

## Quick Start

### 1. Start the API Server

```bash
cd column-lineage-api
python -m uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

### 2. Test the Setup

Run the test script to verify everything works:

```bash
python test_repo_analysis.py
```

### 3. Use the Example Script

```bash
python example_usage.py
```

## API Usage

### Start Analysis

```bash
curl -X POST "http://localhost:8000/api/v1/repo-analysis/public/analyze" \
  -H "Content-Type: application/json" \
  -d '{
    "frontend_repo_name": "guided-workflow",
    "backend_repo_name": "guided-workflow-backend",
    "output_filename": "my_analysis.csv"
  }'
```

### Check Status

```bash
curl "http://localhost:8000/api/v1/repo-analysis/public/status/{job_id}"
```

### List Jobs

```bash
curl "http://localhost:8000/api/v1/repo-analysis/public/jobs"
```

## Python Usage

```python
import requests
import time

# Start analysis
response = requests.post(
    "http://localhost:8000/api/v1/repo-analysis/public/analyze",
    json={
        "frontend_repo_name": "guided-workflow",
        "backend_repo_name": "guided-workflow-backend",
        "output_filename": "analysis.csv"
    }
)

job_data = response.json()
job_id = job_data["job_id"]

# Wait for completion
while True:
    status_response = requests.get(
        f"http://localhost:8000/api/v1/repo-analysis/public/status/{job_id}"
    )
    status_data = status_response.json()
    
    if status_data["status"] in ["completed", "failed", "cancelled"]:
        break
    
    time.sleep(10)

print(f"Analysis completed: {status_data['output_file']}")
```

## Output Format

The generated CSV contains these columns:

| Column | Description |
|--------|-------------|
| Frontend File | Source TypeScript file |
| Frontend Function | Function making the API call |
| HTTP Method | GET, POST, PUT, DELETE, etc. |
| Frontend URL | API endpoint URL pattern |
| Backend File | Python file handling the request |
| Backend Function | Route handler function name |
| Backend Route | Backend route pattern |
| Tables | Database tables accessed |
| Response Model | Pydantic response model |
| Response Fields | Fields in the response |
| Nested Fields | Nested object fields |
| Table Column Details | Detailed column info per table |
| Column Count | Total columns in response |
| Relationship Type | single_table, multi_table_join, no_tables, unmatched |

## Troubleshooting

### Common Issues

1. **Missing Environment Variables**
   ```bash
   # Check if variables are set
   echo $AWS_CODECOMMIT_USERNAME
   echo $AWS_CODECOMMIT_PASSWORD
   ```

2. **Git Not Found**
   ```bash
   # Verify git is installed
   git --version
   ```

3. **Repository Access Issues**
   - Verify AWS CodeCommit credentials
   - Check repository names are correct
   - Ensure network connectivity to AWS

4. **Import Errors**
   - Verify all Python dependencies are installed
   - Check that action_to_table.py is in the correct location

### Debug Mode

Enable debug logging by setting:
```bash
DEBUG=true
LOG_LEVEL=DEBUG
```

### Manual Testing

Test individual components:

```bash
# Test repository cloning
python -c "
from api.core.repo_analysis.repository_cloning_service import RepositoryCloningService
service = RepositoryCloningService()
service.setup_aws_credentials()
print('Credentials loaded successfully')
"

# Test action_to_table import
python -c "
import sys
sys.path.append('api/core/repo_analysis')
from action_to_table import CompleteFrontendAnalyzer
print('action_to_table imported successfully')
"
```

## Advanced Configuration

### Custom Repository Patterns

Modify `repository_cloning_service.py` to add custom repository patterns:

```python
self.frontend_patterns = [
    r'.*-frontend$',
    r'.*-ui$',
    r'your-custom-pattern$'
]
```

### Custom Output Location

Specify custom output directory in the service:

```python
# In repository_analysis_service.py
self.base_clone_dir = Path("custom_clone_directory")
```

## Performance Considerations

- **Repository Size**: Large repositories take longer to clone
- **Analysis Complexity**: Complex codebases require more processing time
- **Network Speed**: Cloning speed depends on network connectivity
- **Disk Space**: Ensure sufficient space for cloned repositories

## Security Notes

- AWS credentials are loaded from environment variables only
- No credentials are stored in files or logs
- Repository cloning uses HTTPS with authentication
- Cloned repositories are stored locally and can be cleaned up

## Support

For issues or questions:
1. Check the logs in `action_to_table.log`
2. Run the test script to verify setup
3. Check environment variables are correctly set
4. Verify network connectivity to AWS CodeCommit