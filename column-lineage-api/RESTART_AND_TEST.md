# How to Apply the Fix and Test

## What Was Changed

The `database_filter` and `schema_filter` fields in the `LineageAnalysisRequest` model are now **optional with default values** from your environment variables.

### Before (Required Fields):
```python
database_filter: str = Field(description="Database name to filter views (required)")
schema_filter: str = Field(description="Schema name to filter views (required)")
```

### After (Optional with Defaults):
```python
database_filter: str = Field(default=DEFAULT_DATABASE, description="Database name...")
schema_filter: str = Field(default=DEFAULT_SCHEMA, description="Schema name...")
```

Where:
- `DEFAULT_DATABASE = "CPS_DB"` (from your .env file)
- `DEFAULT_SCHEMA = "CPS_DSCI_BR"` (from your .env file)

## Steps to Apply the Fix

### 1. Restart Your API Server
**IMPORTANT**: You must restart your server to load the updated code.

```bash
# Stop the current server (Ctrl+C if running in terminal)
# Then restart it
cd column-lineage-api
python run.py
# OR
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

### 2. Test the Fix

#### Option A: Test with Python Script
```bash
cd column-lineage-api
python test_final_validation.py
```

#### Option B: Test with curl (after server restart)
```bash
# This should now work (empty request):
curl -X POST 'http://localhost:8000/api/v1/lineage/analyze' \
  -H 'Content-Type: application/json' \
  -d '{}'

# This should also work (with view names only):
curl -X POST 'http://localhost:8000/api/v1/lineage/analyze' \
  -H 'Content-Type: application/json' \
  -d '{"view_names": ["TEST_VIEW"]}'
```

## Expected Results

### Before Fix (Error):
```json
{
  "detail": [
    {
      "type": "missing",
      "loc": ["body", "database_filter"],
      "msg": "Field required"
    },
    {
      "type": "missing",
      "loc": ["body", "schema_filter"], 
      "msg": "Field required"
    }
  ]
}
```

### After Fix (Success):
```json
{
  "job_id": "some-uuid",
  "status": "PENDING",
  "message": "Analysis started...",
  "results_url": "/api/v1/lineage/results/some-uuid"
}
```

## Troubleshooting

If you still get "Field required" errors:

1. **Make sure you restarted the server** - This is the most common issue
2. **Check you're hitting the right endpoint** - Make sure the URL is correct
3. **Verify the changes** - Run `python test_final_validation.py` to test the model
4. **Check server logs** - Look for any import errors or startup issues

## What Happens Now

- **Empty requests work**: `{}` will use defaults from environment
- **Partial requests work**: `{"view_names": ["VIEW1"]}` will use defaults for database/schema
- **Full requests still work**: Explicit values override the defaults
- **Backward compatible**: Existing API calls continue to work unchanged

The API will automatically use:
- Database: `CPS_DB` (from your .env)
- Schema: `CPS_DSCI_BR` (from your .env)

When these fields are not provided in the request.