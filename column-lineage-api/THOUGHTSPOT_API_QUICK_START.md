# ThoughtSpot API - Quick Start Guide

## 🚀 Quick Setup

### Files Created
```
api/v1/models/thoughtspot_analysis.py          # Pydantic models
api/v1/services/thoughtspot_analysis_service.py # Business logic
api/v1/routers/thoughtspot_analysis.py          # API endpoints
test_thoughtspot_api.py                         # Test script
```

### Files Updated
```
api/v1/models/__init__.py      # Added ThoughtSpot models
api/v1/services/__init__.py    # Added ThoughtSpot service
api/v1/routers/__init__.py     # Added ThoughtSpot router
api/main.py                    # Registered router
```

---

## 📡 API Endpoints

### Base URL
```
http://localhost:8000/api/v1/thoughtspot-analysis
```

### Endpoints Summary

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| POST | `/analyze` | ✅ | Start analysis |
| GET | `/status/{job_id}` | ✅ | Get job status |
| GET | `/results/{job_id}` | ✅ | Get results |
| GET | `/results/{job_id}/download` | ✅ | Download CSV |
| GET | `/jobs` | ✅ | List all jobs |
| DELETE | `/jobs/{job_id}` | ✅ | Delete job |
| GET | `/tables` | ✅ | List tables |
| POST | `/public/analyze` | ❌ | Start analysis (public) |
| GET | `/public/status/{job_id}` | ❌ | Get status (public) |
| GET | `/public/results/{job_id}` | ❌ | Get results (public) |
| GET | `/public/results/{job_id}/download` | ❌ | Download CSV (public) |

---

## 🧪 Testing

### Method 1: Using Test Script

```bash
# Run full test suite
python test_thoughtspot_api.py

# Check status of existing job
python test_thoughtspot_api.py <job_id>
```

### Method 2: Using cURL

```bash
# Start analysis
curl -X POST "http://localhost:8000/api/v1/thoughtspot-analysis/public/analyze" \
  -H "Content-Type: application/json" \
  -d '{"sf_environment": "prod", "max_workers": 5, "include_views": true}'

# Check status
curl "http://localhost:8000/api/v1/thoughtspot-analysis/public/status/{job_id}"

# Get results
curl "http://localhost:8000/api/v1/thoughtspot-analysis/public/results/{job_id}"

# Download CSV
curl "http://localhost:8000/api/v1/thoughtspot-analysis/public/results/{job_id}/download" -o results.csv
```

### Method 3: Using Swagger UI

1. Start the API server
2. Open browser: `http://localhost:8000/docs`
3. Navigate to "thoughtspot-analysis" section
4. Try out the endpoints

---

## 📊 Request/Response Examples

### Start Analysis Request
```json
{
  "sf_environment": "prod",
  "table_pattern": null,
  "max_workers": 5,
  "include_views": true,
  "force_prod_urls": true
}
```

### Start Analysis Response
```json
{
  "job_id": "123e4567-e89b-12d3-a456-426614174000",
  "status": "PENDING",
  "message": "ThoughtSpot liveboard analysis started...",
  "results_url": "/api/v1/thoughtspot-analysis/results/123e4567-e89b-12d3-a456-426614174000",
  "started_at": "2024-01-16T10:30:00Z"
}
```

### Job Status Response
```json
{
  "job_id": "123e4567-e89b-12d3-a456-426614174000",
  "status": "RUNNING",
  "sf_environment": "prod",
  "total_tables": 150,
  "processed_tables": 75,
  "total_relationships": 0,
  "started_at": "2024-01-16T10:30:00Z"
}
```

### Results Response
```json
{
  "job_id": "123e4567-e89b-12d3-a456-426614174000",
  "status": "COMPLETED",
  "total_tables": 150,
  "total_relationships": 342,
  "unique_liveboards": 45,
  "download_url": "/api/v1/thoughtspot-analysis/results/123e4567-e89b-12d3-a456-426614174000/download",
  "summary": {
    "schema_distribution": {"CPS_DSCI_API": 200, "CPS_DSCI_BR": 142},
    "type_distribution": {"BASE TABLE": 250, "VIEW": 92},
    "execution_time": 245.5
  }
}
```

---

## 🗄️ Database Table

### Table Name
```
CPS_DB.CPS_DSCI_BR.TS_TABLE_LIVEBOARD_MAPPING
```

### Schema
```sql
TABLE_NAME VARCHAR(255)
LIVEBOARD_NAME VARCHAR(500)
GUID VARCHAR(255)
SCHEMA VARCHAR(255)
TYPE VARCHAR(50)
ANALYSIS_TIMESTAMP TIMESTAMP_NTZ(9)
CREATED_AT TIMESTAMP_NTZ(9)
```

### Query Example
```sql
-- Get all liveboards for a table
SELECT * FROM CPS_DB.CPS_DSCI_BR.TS_TABLE_LIVEBOARD_MAPPING
WHERE TABLE_NAME = 'BOOKINGS';

-- Count relationships by schema
SELECT SCHEMA, COUNT(*) as relationship_count
FROM CPS_DB.CPS_DSCI_BR.TS_TABLE_LIVEBOARD_MAPPING
GROUP BY SCHEMA;

-- Find tables with most liveboards
SELECT TABLE_NAME, COUNT(DISTINCT GUID) as liveboard_count
FROM CPS_DB.CPS_DSCI_BR.TS_TABLE_LIVEBOARD_MAPPING
GROUP BY TABLE_NAME
ORDER BY liveboard_count DESC
LIMIT 10;
```

---

## 🔧 Configuration

### Request Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sf_environment` | string | "prod" | Snowflake environment (dev/stage/prod) |
| `table_pattern` | string | null | Filter tables by pattern |
| `max_workers` | int | 5 | Parallel workers (1-10) |
| `include_views` | bool | true | Include views in analysis |
| `force_prod_urls` | bool | true | Force production ThoughtSpot URLs |

### Environment Variables
```bash
# In .env file
BULK_INSERT_BATCH_SIZE=500  # Database insert batch size
```

---

## 📈 Job Status Flow

```
PENDING → RUNNING → COMPLETED
                 ↘ FAILED
                 ↘ CANCELLED
```

---

## ⚡ Performance Tips

1. **Parallel Workers**: Use 5-10 workers for faster processing
2. **Table Pattern**: Filter tables to reduce analysis time
3. **Include Views**: Set to `false` if only base tables needed
4. **Batch Size**: Adjust `BULK_INSERT_BATCH_SIZE` for optimal database performance

---

## 🐛 Troubleshooting

### Issue: Job stuck in PENDING
- Check API server logs
- Verify background tasks are running
- Check ThoughtSpot service connectivity

### Issue: Analysis fails
- Check Snowflake credentials
- Verify ThoughtSpot credentials
- Check network connectivity
- Review error message in job status

### Issue: Database insertion fails
- Check database connection
- Verify table permissions
- Check bulk insert batch size
- Review service logs

---

## 📝 Integration Checklist

- ✅ Models created (`thoughtspot_analysis.py`)
- ✅ Service implemented (`thoughtspot_analysis_service.py`)
- ✅ Router created (`thoughtspot_analysis.py`)
- ✅ Models registered in `__init__.py`
- ✅ Service registered in `__init__.py`
- ✅ Router registered in `__init__.py`
- ✅ Router added to `main.py`
- ✅ Database table schema defined
- ✅ Bulk insert optimization implemented
- ✅ Error handling added
- ✅ Logging configured
- ✅ Test script created
- ✅ Documentation written

---

## 🎯 Next Steps

1. **Start API Server**
   ```bash
   cd column-lineage-api
   uvicorn api.main:app --reload
   ```

2. **Run Test**
   ```bash
   python test_thoughtspot_api.py
   ```

3. **Check Swagger Docs**
   - Open: `http://localhost:8000/docs`
   - Navigate to "thoughtspot-analysis" section

4. **Monitor Logs**
   - Check console output
   - Review `thoughtspot_analysis_results/` directory

5. **Query Database**
   ```sql
   SELECT * FROM CPS_DB.CPS_DSCI_BR.TS_TABLE_LIVEBOARD_MAPPING
   LIMIT 10;
   ```

---

## 📚 Additional Resources

- Full Documentation: `THOUGHTSPOT_API_INTEGRATION.md`
- Original Script: `api/core/toughtspot_to_table/thoughtspot_to_table_analysis.py`
- Test Script: `test_thoughtspot_api.py`
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

---

## ✅ Summary

The ThoughtSpot API is now fully integrated and ready to use! It follows the same patterns as your existing SP Analysis and Prefect Analysis APIs, providing:

- Complete REST API with authentication
- Background job processing
- CSV result generation
- Database integration
- Comprehensive error handling
- Test script for validation
- Full documentation

**Happy coding! 🚀**
