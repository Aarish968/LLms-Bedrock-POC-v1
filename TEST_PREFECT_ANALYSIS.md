# Testing Prefect Analysis API

## Quick Test Commands

### 1. Start the API Server
```bash
cd column-lineage-api
python run.py
```

### 2. Test with Default Settings (Should find ~99 repos)
```bash
curl -X POST "http://localhost:8000/api/v1/prefect-analysis/analyze" \
  -H "Content-Type: application/json" \
  -d '{
    "sf_environment": "prod",
    "max_workers": 4
  }'
```

Expected response:
```json
{
  "job_id": "uuid-here",
  "status": "pending",
  "message": "Prefect repository analysis started...",
  "started_at": "2026-01-14T...",
  "results_url": "/api/v1/prefect-analysis/results/{job_id}"
}
```

### 3. Check Job Status
```bash
curl "http://localhost:8000/api/v1/prefect-analysis/status/{job_id}"
```

### 4. Monitor Logs
Watch the console output for:
- ✅ "Performing content-based discovery (this may take longer)..."
- ✅ "Found 20 repos by naming convention, checking 74 remaining repos for Prefect patterns in parallel"
- ✅ "Using X parallel workers for Prefect pattern checking"
- ✅ "Parallel Prefect pattern check completed: Y repositories contain Prefect flows"
- ✅ "Total repositories selected for cloning: ~99"
- ✅ "Parallel clone completed: X new, Y updated, Z existing, W failed"
- ✅ "Verified N repositories actually contain Prefect patterns"
- ❌ NO "NoneType object is not subscriptable" errors

### 5. Test Fast Mode (Should find ~20 repos)
```bash
curl -X POST "http://localhost:8000/api/v1/prefect-analysis/analyze" \
  -H "Content-Type: application/json" \
  -d '{
    "sf_environment": "prod",
    "max_workers": 4,
    "skip_discovery": true
  }'
```

Expected: Only ~20 repositories (naming patterns only)

## What to Verify

### Discovery Phase
- [ ] Content-based discovery runs by default
- [ ] Parallel checking of remaining repositories
- [ ] ~99 total repositories discovered (not just 20)
- [ ] No "string" repository errors

### Cloning Phase
- [ ] Parallel cloning with multiple workers
- [ ] Smart update of existing repositories (git pull)
- [ ] No NoneType errors after cloning completes
- [ ] All 99 repositories cloned/updated successfully

### Verification Phase
- [ ] Cloned repositories verified for Prefect patterns
- [ ] Repository info gathered for all repos
- [ ] No crashes during repository analysis

### Analysis Phase
- [ ] Table-column analysis runs on all cloned repos
- [ ] CSV output file created
- [ ] Database insertion succeeds (if configured)
- [ ] Job status updates to "completed"

## Expected Timeline

- **Discovery**: 3-5 minutes (parallel checking of ~74 repos)
- **Cloning**: 2-3 minutes (parallel cloning of ~99 repos)
- **Verification**: 1-2 minutes (checking cloned repos)
- **Analysis**: 5-10 minutes (table-column reference analysis)
- **Total**: ~15-20 minutes for complete analysis

## Troubleshooting

### If only 20 repos are found:
- Check that `skip_discovery` is `false` in the request
- Look for "Performing content-based discovery" in logs
- Verify parallel checking is running

### If NoneType error occurs:
- Check the exact line number in the error
- Verify `clone_results` structure in logs
- Look for "Clone results type:" and "Details type:" messages

### If cloning is slow:
- Increase `max_workers` (up to 8 for cloning)
- Check network connectivity to AWS CodeCommit
- Verify AWS credentials are valid

### If repositories aren't verified:
- Check that cloned repos exist in target directory
- Verify git repositories are valid
- Look for "Checking X cloned repositories for Prefect patterns" message
