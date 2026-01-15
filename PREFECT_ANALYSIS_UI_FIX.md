# Prefect Analysis UI Flow Fix

## Problem
The "Start Prefect Analysis" button had different UI behavior compared to the first three analysis buttons (Column Lineage, Repository Analysis, and SP Analysis). The dialog and job management flow were inconsistent.

## Solution
Updated the Prefect Analysis components to follow the exact same UI pattern as the other three buttons.

## Changes Made

### 1. Updated `usePrefectAnalysis.ts` Hook
- **Added global state management** similar to `useRepositoryAnalysis.ts`
- **Added `startAnalysis()` method** that:
  - Immediately creates a job entry in the state
  - Returns the response for parent component notification
  - Triggers automatic refresh after 1 second
- **Added `addJobToState()` method** for manual job state updates
- **Improved state synchronization** across all hook instances using global listeners

### 2. Updated `PrefectAnalysisDialog.tsx` Component
- **Restructured to match other dialogs** (Repository, SP Analysis)
- **Added proper state management**:
  - `currentJobId` - tracks the current job
  - `jobStatus` - stores job details
  - `isStarting` - loading state during analysis start
  - `isRedirecting` - shows redirect message
- **Implemented same workflow**:
  1. Dialog opens with "Start Analysis" button
  2. On click, job starts and is immediately added to state
  3. Shows redirect message
  4. Auto-closes after 1 second
  5. Parent component switches to Prefect Analysis Jobs tab
- **Added Debug Info component** matching other dialogs
- **Added "New Analysis" button** for completed jobs

### 3. Updated `PrefectAnalysisJobs.tsx` Component
- **Removed spinner and time estimate** from status column (no more ~20m indicator)
- **Changed PENDING status to filled orange button** with white text (#ff9800 background)
- **Added View Results (eye icon) button** for completed jobs
- **Added Results Dialog** showing:
  - Job details (ID, status, references count)
  - Unique tables, repos, and functions
  - Output file information and size
  - Sample references preview
  - Download CSV button
- **Simplified status display** to show only the status chip
- Status colors:
  - PENDING: Orange filled button (warning)
  - CLONING: Blue (info)
  - ANALYZING: Blue (info)
  - COMPLETED: Green (success)
  - FAILED: Red (error)
  - CANCELLED: Gray (default)
- **Actions column now shows**:
  - Eye icon (View Results) for completed jobs
  - Download icon for completed jobs with output files
  - Cancel icon (red X) for running jobs

## UI Flow (Now Identical to Other Buttons)

### Before Click:
- Dialog shows description
- "Start Analysis" button enabled (if no running job)
- Warning shown if another job is running

### After Click:
1. Button shows "Starting..."
2. Job is created and immediately appears in jobs list
3. Dialog shows "Analysis started successfully! Redirecting..."
4. Dialog auto-closes after 1 second
5. UI switches to "Analysis Jobs" tab → "Prefect Analysis Jobs" sub-tab
6. Job appears with status "PENDING"
7. Status automatically updates to "CLONING" → "ANALYZING" → "COMPLETED"

## Key Features Maintained
- Smart polling with adaptive intervals
- Tab visibility detection
- Estimated completion times
- Progress tracking for cloning phase
- Download results when completed
- Cancel running jobs

## Testing Checklist
- [ ] Click "Start Prefect Analysis" button
- [ ] Verify dialog opens with "Start Analysis" button
- [ ] Click "Start Analysis"
- [ ] Verify job appears immediately in the list
- [ ] Verify status shows "PENDING"
- [ ] Verify dialog auto-closes and redirects to Prefect Analysis Jobs tab
- [ ] Verify status updates automatically (PENDING → CLONING → ANALYZING → COMPLETED)
- [ ] Verify "New Analysis" button appears after completion
- [ ] Verify download button works for completed jobs

## Files Modified
1. `column-lineage-frontend/src/hooks/usePrefectAnalysis.ts`
2. `column-lineage-frontend/src/components/PrefectAnalysis/PrefectAnalysisDialog.tsx`
3. `column-lineage-frontend/src/components/PrefectAnalysis/PrefectAnalysisJobs.tsx`

## Result
The Prefect Analysis button now has the exact same UI behavior and user flow as the other three analysis buttons, while maintaining its unique backend logic for cloning CodeCommit repositories and analyzing Prefect code.
