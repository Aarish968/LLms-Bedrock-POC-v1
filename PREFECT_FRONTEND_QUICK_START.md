# Prefect Analysis Frontend - Quick Start Guide

## What You Get

A new **"Start Prefect Analysis"** button that discovers and analyzes ~99 Prefect repositories with smart polling that won't slow down your backend.

## Visual Overview

```
┌─────────────────────────────────────────────────────────────┐
│  Dashboard                                                   │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  [Start View Lineage]  [Start Action Lineage]               │
│  [Start SP Lineage]    [Start Prefect Analysis] ← NEW!      │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

## How It Works

### 1. Click Button
```
User clicks "Start Prefect Analysis"
         ↓
Dialog opens with options
```

### 2. Configure Options
```
┌─────────────────────────────────────────┐
│  Start Prefect Repository Analysis      │
├─────────────────────────────────────────┤
│  Environment: [Prod ▼]                  │
│  Max Workers: [4]                       │
│  Target Dir:  [prefect_repos]           │
│                                         │
│  ☑ Content-Based Discovery              │
│     (Finds ~99 repos, takes ~20 min)    │
│                                         │
│  ☐ Skip Naming Pattern Check            │
│  ☐ Clone All Repositories               │
│                                         │
│  [Cancel]  [Start Analysis]             │
└─────────────────────────────────────────┘
```

### 3. Monitor Progress
```
┌─────────────────────────────────────────────────────────┐
│  Prefect Analysis Jobs              [↻] [New Analysis]  │
├─────────────────────────────────────────────────────────┤
│  ℹ Auto-refreshing every 30 seconds while running       │
├─────────────────────────────────────────────────────────┤
│  Job: abc123...                                         │
│  Status: [CLONING] ⏱ ~18m                               │
│  Progress: ████████░░░░░░░░ 30/99 repos                │
│  Started: 2 minutes ago                                 │
│  Last updated: 30 seconds ago                           │
└─────────────────────────────────────────────────────────┘
```

### 4. Download Results
```
Status changes to [COMPLETED]
         ↓
[⬇] Download button appears
         ↓
Click to download CSV with table-column references
```

## Smart Polling Strategy

### Adaptive Intervals
```
Job Status    Polling Interval    Why?
─────────────────────────────────────────
PENDING       10 seconds          Quick startup
CLONING       30 seconds          Long process
ANALYZING     60 seconds          Very long
COMPLETED     Stop                Done!
```

### Tab Visibility
```
Tab Visible:   Polling active ✓
Tab Hidden:    Polling paused ⏸
               (Saves backend resources!)
```

### Load Comparison
```
Old (5s polling):  240 requests in 20 minutes
New (smart):       ~30 requests in 20 minutes
Reduction:         87.5% fewer requests! 🎉
```

## Quick Commands

### Start the Frontend
```bash
cd column-lineage-frontend
npm install
npm run dev
```

### Access the App
```
http://localhost:5173
```

### Test the Integration
1. Click "Start Prefect Analysis"
2. Keep default settings
3. Click "Start Analysis"
4. Watch the progress in "Prefect Analysis Jobs" tab
5. Download results when complete

## Configuration Options

### Comprehensive Mode (Default)
- **Repos Found**: ~99
- **Time**: ~20 minutes
- **Method**: Naming patterns + content discovery
- **Best For**: Complete analysis

### Fast Mode
- **Repos Found**: ~20
- **Time**: ~11 minutes
- **Method**: Naming patterns only
- **Best For**: Quick checks

### To Enable Fast Mode
Toggle OFF: "Content-Based Discovery"

## Monitoring Tips

### Check Progress
- Look at progress bar: "30/99 repos"
- Check estimated time: "~18m"
- See elapsed time: "2 minutes ago"

### Manual Refresh
- Click refresh icon (↻) anytime
- See last updated timestamp
- No need to wait for auto-refresh

### Tab Management
- Keep tab visible for auto-updates
- Tab hidden? Polling pauses automatically
- Returns when tab visible again

## Troubleshooting

### Button Disabled?
- Another Prefect job is running
- Wait for it to complete or cancel it

### No Jobs Showing?
- Click refresh icon
- Check browser console for errors
- Verify backend is running

### Slow Updates?
- Check if tab is visible
- Look for "Auto-refresh paused" message
- Click manual refresh

### Download Not Working?
- Ensure job status is "COMPLETED"
- Check browser download settings
- Verify backend has results file

## API Endpoints Used

```
POST   /api/v1/prefect-analysis/analyze
GET    /api/v1/prefect-analysis/status/{job_id}
GET    /api/v1/prefect-analysis/jobs
GET    /api/v1/prefect-analysis/results/{job_id}
GET    /api/v1/prefect-analysis/results/{job_id}/download
DELETE /api/v1/prefect-analysis/jobs/{job_id}
```

## Files to Check

If something doesn't work:

1. **Types**: `src/types/prefectAnalysis.ts`
2. **Service**: `src/api/prefectAnalysisService.ts`
3. **Hook**: `src/hooks/usePrefectAnalysis.ts`
4. **Dialog**: `src/components/PrefectAnalysis/PrefectAnalysisDialog.tsx`
5. **Jobs**: `src/components/PrefectAnalysis/PrefectAnalysisJobs.tsx`
6. **Dashboard**: `src/pages/DashboardPage.tsx`

## Browser Console

Watch for these logs:
```
🚀 API Request: POST /api/v1/prefect-analysis/analyze
✅ API Response: { job_id: "...", status: "pending" }
Setting up Prefect analysis polling with 10000ms interval
Polling Prefect analysis jobs...
```

## Success Indicators

✅ Button appears on dashboard
✅ Dialog opens and closes
✅ Analysis starts successfully
✅ Jobs list updates automatically
✅ Progress bar shows during cloning
✅ Estimated time counts down
✅ Download works when complete
✅ Polling pauses when tab hidden
✅ Manual refresh works

## That's It!

You now have a fully functional Prefect Analysis integration with smart polling that won't slow down your backend. Enjoy! 🚀
