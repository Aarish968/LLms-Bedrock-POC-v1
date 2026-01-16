# ThoughtSpot Liveboard Analysis API Integration

## 🎯 Overview

Complete API integration for ThoughtSpot liveboard analysis that maps database tables to ThoughtSpot liveboards. The integration follows existing patterns from SP Analysis and Prefect Analysis APIs.

---

## 📁 Files Created

### 1. Models
**File**: `api/v1/models/thoughtspot_analysis.py`

**Models**:
- `TSJobStatus` - Job status enumeration (PENDING, RUNNING, COMPLETED, FAILED, CANCELLED)
- `LiveboardInfo` - ThoughtSpot liveboard information
- `TableLiveboardRelationship` - Table to liveboard relationship
- `TSAnalysisRequest` - Request model for starting analysis
- `TSAnalysisResponse` - Response model for analysis start
- `TSAnalysisJob` - Job tracking model
- `TSResultsResponse` - Results response model
- `TableInfo` - Table information model
- `TableListResponse` - List of tables response

### 2. Service
**File**: `api/v1/services/thoughtspot_analysis_service.py`

**Key Methods**:
- `create_job()` - Create new analysis job
- `process_analysis()` - Run ThoughtSpot analysis in background
- `get_job()` - Get job by ID
- `update_job_status()` - Update job status
- `list_jobs()` - List all jobs with pagination
- `delete_job()` - Delete job and result file
- `get_results()` - Get analysis results
- `get_tables_list()` - Get list of tables from Snowflake
- `_insert_csv_data_to_database()` - Insert results into database
- `_bulk_insert_ts_data()` - Bulk insert with batching

**Features**:
- Background job processing
- CSV result generation
- Database insertion (TS_TABLE_LIVEBOARD_MAPPING table)
- Bulk insert with configurable batch size
- Comprehensive error handling and logging
- Job status tracking

### 3. Router
**File**: `api/v1/routers/thoughtspot_analysis.py`

**Protected Endpoints** (require JWT authentication):
- `POST /api/v1/thoughtspot-analysis/analyze` - Start analysis
- `GET /api/v1/thoughtspot-analysis/status/{job_id}` - Get job status
- `GET /api/v1/thoughtspot-analysis/results/{job_id}` - Get results
- `GET /api/v1/thoughtspot-analysis/results/{job_id}/download` - Download CSV
- `GET /api/v1/thoughtspot-analysis/jobs` - List all jobs
- `DELETE /api/v1/thoughtspot-analysis/jobs/{job_id}` - Delete job
- `GET /api/v1/thoughtspot-analysis/tables` - List tables

**Public Endpoints** (no authentication):
- `POST /api/v1/thoughtspot-analysis/public/analyze` - Start analysis
- `GET /api/v1/thoughtspot-analysis/public/status/{job_id}` - Get job status
- `GET /api/v1/thoughtspot-analysis/public/results/{job_id}` - Get results
- `GET /api/v1/thoughtspot-analysis/public/results/{job_id}/download` - Download CSV

### 4. Updated Files
- `api/v1/models/__init__.py` - Added ThoughtSpot models export
- `api/v1/services/__init__.py` - Added ThoughtSpotAnalysisService export
- `api/v1/routers/__init__.py` - Added thoughtspot_analysis router
- `api/main.py` - Registered ThoughtSpot router

---

## 🗄️ Database Schema

### Table: `TS_TABLE_LIVEBOARD_MAPPING`

```sql
CREATE OR REPLACE TABLE CPS_DB.CPS_DSCI_BR.TS_TABLE_LIVEBOARD_MAPPING (
    TABLE_NAME VARCHAR(255),
    LIVEBOARD_NAME VARCHAR(500),
    GUID VARCHAR(255),
    SCHEMA VARCHAR(255),
    TYPE VARCHAR(50),
    ANALYSIS_TIMESTAMP TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    CREATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
)
```

**Columns**:
- `TABLE_NAME` - Name of the database table
- `LIVEBOARD_NAME` - Name of the ThoughtSpot liveboard
- `GUID` - ThoughtSpot liveboard GUID
- `SCHEMA` - Database schema (CPS_DSCI_API or CPS_DSCI_BR)
- `TYPE` - Table type (BASE TABLE or VIEW)
- `ANALYSIS_TIMESTAMP` - When the analysis was performed
- `CREATED_AT` - Record creation timestamp

---

## 🚀 API Usage Examples

### 1. Start ThoughtSpot Analysis (Protected)

```bash
curl -X POST "http://localhost:8000/api/v1/thoughtspot-analysis/analyze" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "sf_environment": "prod",
    "table_pattern": null,
    "max_workers": 5,
    "include_views": true,
    "force_prod_urls": true
  }'
```

**Response**:
```json
{
  "job_id": "123e4567-e89b-12d3-a456-426614174000",
  "status": "PENDING",
  "message": "ThoughtSpot liveboard analysis started. Use the job_id to check status and retrieve results.",
  "results_url": "/api/v1/thoughtspot-analysis/results/123e4567-e89b-12d3-a456-426614174000",
  "started_at": "2024-01-16T10:30:00Z"
}
```

### 2. Start Analysis (Public - No Auth)

```bash
curl -X POST "http://localhost:8000/api/v1/thoughtspot-analysis/public/analyze" \
  -H "Content-Type: application/json" \
  -d '{
    "sf_environment": "prod",
    "max_workers": 5,
    "include_views": true
  }'
```

### 3. Check Job Status

```bash
curl -X GET "http://localhost:8000/api/v1/thoughtspot-analysis/status/{job_id}" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"
```

**Response**:
```json
{
  "job_id": "123e4567-e89b-12d3-a456-426614174000",
  "status": "RUNNING",
  "sf_environment": "prod",
  "table_pattern": null,
  "max_workers": 5,
  "include_views": true,
  "force_prod_urls": true,
  "total_tables": 150,
  "processed_tables": 75,
  "total_relationships": 0,
  "result_file": "thoughtspot_analysis_results/thoughtspot_analysis_123e4567_20240116_103000.csv",
  "error_message": null,
  "started_at": "2024-01-16T10:30:00Z",
  "completed_at": null,
  "request_params": {...}
}
```

### 4. Get Analysis Results

```bash
curl -X GET "http://localhost:8000/api/v1/thoughtspot-analysis/results/{job_id}" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"
```

**Response**:
```json
{
  "job_id": "123e4567-e89b-12d3-a456-426614174000",
  "status": "COMPLETED",
  "total_tables": 150,
  "total_relationships": 342,
  "unique_liveboards": 45,
  "result_file": "thoughtspot_analysis_results/thoughtspot_analysis_123e4567_20240116_103000.csv",
  "download_url": "/api/v1/thoughtspot-analysis/results/123e4567-e89b-12d3-a456-426614174000/download",
  "summary": {
    "schema_distribution": {
      "CPS_DSCI_API": 200,
      "CPS_DSCI_BR": 142
    },
    "type_distribution": {
      "BASE TABLE": 250,
      "VIEW": 92
    },
    "execution_time": 245.5
  }
}
```

### 5. Download Results CSV

```bash
curl -X GET "http://localhost:8000/api/v1/thoughtspot-analysis/results/{job_id}/download" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -o thoughtspot_results.csv
```

### 6. List All Jobs

```bash
curl -X GET "http://localhost:8000/api/v1/thoughtspot-analysis/jobs?limit=50&offset=0" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"
```

### 7. List Tables

```bash
curl -X GET "http://localhost:8000/api/v1/thoughtspot-analysis/tables?sf_environment=prod&include_views=true" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"
```

### 8. Delete Job

```bash
curl -X DELETE "http://localhost:8000/api/v1/thoughtspot-analysis/jobs/{job_id}" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"
```

---

## 📊 CSV Output Format

**Columns**:
- `Table_name` - Database table name
- `Liveboard_name` - ThoughtSpot liveboard name
- `GUID` - Liveboard unique identifier
- `Schema` - Database schema
- `Type` - BASE TABLE or VIEW

**Example**:
```csv
Table_name,Liveboard_name,GUID,Schema,Type
BOOKINGS,Sales Dashboard,abc123-def456-ghi789,CPS_DSCI_API,BASE TABLE
REVENUE,Revenue Analysis,xyz789-uvw456-rst123,CPS_DSCI_API,BASE TABLE
CUSTOMER_VIEW,Customer Insights,mno345-pqr678-stu901,CPS_DSCI_BR,VIEW
```

---

## 🔧 Configuration

### Request Parameters

**TSAnalysisRequest**:
- `sf_environment` (string, default: "prod") - Snowflake environment (dev/stage/prod)
- `table_pattern` (string, optional) - Filter tables by pattern
- `max_workers` (int, default: 5, range: 1-10) - Parallel workers
- `include_views` (bool, default: true) - Include views in analysis
- `force_prod_urls` (bool, default: true) - Force production ThoughtSpot URLs

### Environment Variables

Required in `.env`:
```bash
# ThoughtSpot Configuration (inherited from dc-canvas-service)
# AWS Credentials for ThoughtSpot service
# Snowflake credentials for database access

# Bulk Insert Configuration
BULK_INSERT_BATCH_SIZE=500  # Batch size for database inserts
```

---

## 🏗️ Architecture

### Service Flow

1. **Job Creation**
   - User submits analysis request
   - Service creates job with unique ID
   - Job status set to PENDING

2. **Background Processing**
   - FastAPI BackgroundTasks starts analysis
   - Service calls original ThoughtSpot script
   - Progress tracked in job object

3. **Analysis Execution**
   - Fetch tables from Snowflake (CPS_DSCI_API, CPS_DSCI_BR schemas)
   - Query ThoughtSpot for each table's liveboards
   - Generate CSV with relationships
   - Insert data into database

4. **Result Storage**
   - CSV saved to `thoughtspot_analysis_results/` directory
   - Data inserted into `TS_TABLE_LIVEBOARD_MAPPING` table
   - Job status updated to COMPLETED

5. **Result Retrieval**
   - User queries job status
   - Download CSV or query database
   - View summary statistics

### Database Integration

**Bulk Insert Strategy**:
- Configurable batch size (default: 500 rows)
- Automatic fallback to individual inserts on batch failure
- Comprehensive error handling and logging
- Transaction management for data consistency

**Table Management**:
- Auto-create table if not exists
- Truncate before new analysis (fresh data)
- Timestamps for tracking

---

## 🔍 Key Features

### 1. **Parallel Processing**
- Configurable worker threads (1-10)
- Efficient batch processing
- Timeout handling

### 2. **Error Handling**
- Comprehensive try-catch blocks
- Detailed error logging
- Graceful degradation
- Retry logic for ThoughtSpot API

### 3. **Job Management**
- Unique job IDs (UUID)
- Status tracking (PENDING → RUNNING → COMPLETED/FAILED)
- Progress monitoring
- Job history with pagination

### 4. **Database Integration**
- Automatic table creation
- Bulk insert optimization
- Data validation
- Comprehensive logging

### 5. **Security**
- JWT authentication for protected endpoints
- Public endpoints for testing
- Input validation with Pydantic
- SQL injection prevention

### 6. **Monitoring**
- Structured logging
- Progress tracking
- Performance metrics
- Error tracking

---

## 🧪 Testing

### Test Analysis Endpoint

```python
import requests

# Start analysis
response = requests.post(
    "http://localhost:8000/api/v1/thoughtspot-analysis/public/analyze",
    json={
        "sf_environment": "prod",
        "max_workers": 5,
        "include_views": True,
        "force_prod_urls": True
    }
)

job_id = response.json()["job_id"]
print(f"Job ID: {job_id}")

# Check status
import time
while True:
    status_response = requests.get(
        f"http://localhost:8000/api/v1/thoughtspot-analysis/public/status/{job_id}"
    )
    status = status_response.json()["status"]
    print(f"Status: {status}")
    
    if status in ["COMPLETED", "FAILED"]:
        break
    
    time.sleep(10)

# Get results
results = requests.get(
    f"http://localhost:8000/api/v1/thoughtspot-analysis/public/results/{job_id}"
)
print(results.json())
```

---

## 📝 Notes

### Dependencies
- Requires `dc-canvas-service` module for ThoughtSpot integration
- Uses existing Snowflake connection infrastructure
- Leverages common security and database modules

### Performance
- Analysis time depends on number of tables (typically 5-20 minutes for 150+ tables)
- Parallel processing significantly improves performance
- Database insertion is optimized with bulk inserts

### Limitations
- ThoughtSpot API rate limits may affect large analyses
- Requires valid ThoughtSpot credentials
- Network connectivity to ThoughtSpot cloud required

---

## 🎉 Summary

The ThoughtSpot API integration provides:
- ✅ Complete REST API following existing patterns
- ✅ Background job processing with status tracking
- ✅ CSV result generation
- ✅ Database integration with bulk inserts
- ✅ Comprehensive error handling and logging
- ✅ Both protected and public endpoints
- ✅ Swagger/OpenAPI documentation
- ✅ Production-ready code with best practices

The API is now ready to use and follows the same patterns as SP Analysis and Prefect Analysis APIs!
