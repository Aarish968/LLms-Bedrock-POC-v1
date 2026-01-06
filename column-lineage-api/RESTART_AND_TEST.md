# Repository Analysis API - Ultra Simple Configuration

## 🎯 Ultra Minimal API Payload

### New Ultra Simple Payload:
```json
{
    "async_processing": true
}
```

**That's it! Just one field!** 🎉

## 🔧 Environment Variable Configuration

Repository names are now configured via environment variables:

```bash
# .env file
DEFAULT_FRONTEND_REPO=guided-workflow
DEFAULT_BACKEND_REPO=guided-workflow-backend

# AWS CodeCommit (existing)
AWS_CODECOMMIT_USERNAME=your_username
AWS_CODECOMMIT_PASSWORD=your_password
AWS_CODECOMMIT_REGION=us-east-1
```

## 🚀 Updated API Usage

### Example API Call:
```bash
curl -X POST "http://localhost:8000/api/v1/repo-analysis/analyze" \
  -H "Content-Type: application/json" \
  -d '{"async_processing": true}'
```

### Python Example:
```python
import requests

response = requests.post(
    "http://localhost:8000/api/v1/repo-analysis/analyze",
    json={"async_processing": True}
)
```

## 📁 File Organization (Unchanged)

```
column-lineage-api/
├── Cloned_repo/                    # Repository clones
│   ├── Frontend/
│   │   └── guided-workflow/        # From DEFAULT_FRONTEND_REPO
│   └── Backend/
│       └── guided-workflow-backend/ # From DEFAULT_BACKEND_REPO
└── Repo_Analyze/                   # Analysis results
    ├── repo_analysis_20260106_182634.csv
    └── ...
```

## ⚙️ Configuration Options

### Environment Variables:
- **`DEFAULT_FRONTEND_REPO`**: Frontend repository name (default: "guided-workflow")
- **`DEFAULT_BACKEND_REPO`**: Backend repository name (default: "guided-workflow-backend")
- **`AWS_CODECOMMIT_USERNAME`**: CodeCommit username
- **`AWS_CODECOMMIT_PASSWORD`**: CodeCommit password
- **`AWS_CODECOMMIT_REGION`**: AWS region (default: "us-east-1")

### Different Environments:
```bash
# Development
DEFAULT_FRONTEND_REPO=dev-frontend
DEFAULT_BACKEND_REPO=dev-backend

# Staging
DEFAULT_FRONTEND_REPO=staging-ui
DEFAULT_BACKEND_REPO=staging-api

# Production
DEFAULT_FRONTEND_REPO=prod-frontend
DEFAULT_BACKEND_REPO=prod-backend
```

## 🔍 How It Works

1. **API Call**: Submit ultra-simple payload with just `async_processing`
2. **Environment Lookup**: Service reads repository names from environment variables
3. **Repository Cloning**: Clones/updates the configured repositories
4. **Analysis**: Runs comprehensive analysis script
5. **File Management**: Saves results to `Repo_Analyze/` with timestamp
6. **Response**: Returns job status with full path to generated file

## 🧪 Testing

### Option 1: Use Updated Test Script
```bash
python test_api_call.py
```

### Option 2: Test Environment Configuration
```bash
python test_env_config.py
```

### Option 3: Manual Structure Test
```bash
python manual_test.py
```

## ✨ Benefits

- **🎯 Ultra Simple**: Only 1 field in API payload
- **🔧 Configurable**: Easy to change repos via environment variables
- **🚀 Environment-Friendly**: Perfect for dev/staging/prod setups
- **🔒 Secure**: No hardcoded repository names in code
- **📦 Container-Ready**: Ideal for Docker/Kubernetes deployments
- **⚡ Fast**: Minimal payload processing overhead

## 🎉 Migration Guide

### Before:
```json
{
    "frontend_repo_name": "guided-workflow",
    "backend_repo_name": "guided-workflow-backend",
    "output_filename": "my-analysis.csv",
    "credentials_file": "credentials.txt",
    "async_processing": true
}
```

### After:
```json
{
    "async_processing": true
}
```

**Environment setup:**
```bash
DEFAULT_FRONTEND_REPO=guided-workflow
DEFAULT_BACKEND_REPO=guided-workflow-backend
```

The repository analysis system is now ultra-streamlined and production-ready! 🚀