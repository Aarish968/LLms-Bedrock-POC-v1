# Repository Analysis API - Updated Structure

## ✅ Recent Updates

### Removed Fields from API Payload:
- **`credentials_file`**: Removed since we now use environment variables (`AWS_CODECOMMIT_USERNAME`, `AWS_CODECOMMIT_PASSWORD`, `AWS_CODECOMMIT_REGION`)
- **`output_filename`**: Removed since filenames are now auto-generated with timestamps

### New Structure:
- **Auto-generated filenames**: `repo_analysis_YYYYMMDD_HHMMSS.csv`
- **Dedicated directory**: All CSV files are stored in `Repo_Analyze/` directory
- **Simplified API payload**: Only requires repository names and async processing flag

## 🚀 Updated API Usage

### New API Payload Format:
```json
{
    "frontend_repo_name": "guided-workflow",
    "backend_repo_name": "guided-workflow-backend",
    "async_processing": true
}
```

### Example API Call:
```bash
curl -X POST "http://localhost:8000/api/v1/repo-analysis/analyze" \
  -H "Content-Type: application/json" \
  -d '{
    "frontend_repo_name": "guided-workflow",
    "backend_repo_name": "guided-workflow-backend",
    "async_processing": true
  }'
```

## 📁 File Organization

```
column-lineage-api/
├── Cloned_repo/                    # Repository clones
│   ├── Frontend/
│   │   └── guided-workflow/
│   └── Backend/
│       └── guided-workflow-backend/
└── Repo_Analyze/                   # Analysis results
    ├── repo_analysis_20260106_182634.csv
    ├── repo_analysis_20260106_183045.csv
    └── ...
```

## 🔍 How It Works

1. **API Call**: Submit analysis request with simplified payload
2. **Auto-naming**: System generates unique filename with timestamp
3. **Repository Cloning**: Clones/updates repositories as before
4. **Analysis**: Runs main.py script to generate comprehensive CSV
5. **File Management**: Moves output to `Repo_Analyze/` directory with `.csv` extension
6. **Response**: Returns job status with full path to generated file

## 📊 Expected Results

When the API is called, you should see:

1. **Repository Cloning**: Both frontend and backend repos are cloned/updated
2. **Analysis Execution**: The main.py script runs successfully  
3. **File Creation**: Output file created in `Repo_Analyze/` directory
4. **Auto-naming**: File named as `repo_analysis_YYYYMMDD_HHMMSS.csv`
5. **Success Response**: Job status shows COMPLETED with full file path

## 🎯 Testing

### Option 1: Use Test Script
```bash
python test_api_call.py
```

### Option 2: Manual Structure Test
```bash
python manual_test.py
```

### Option 3: Direct API Server
```bash
python run.py
# Then make API calls to http://localhost:8000/api/v1/repo-analysis/analyze
```

## ✨ Benefits

- **Cleaner API**: Simplified payload without unnecessary fields
- **Better Organization**: Dedicated directory for analysis results
- **No Conflicts**: Timestamp-based naming prevents file overwrites
- **Environment-based Auth**: Secure credential management via environment variables
- **Automatic Management**: No need to specify output filenames manually

The repository analysis system is now more streamlined and production-ready!