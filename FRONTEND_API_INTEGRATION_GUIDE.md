# Frontend API Integration Guide

## Overview

Your frontend is a **React + TypeScript + Vite** application that integrates with the backend APIs using **Axios** for HTTP requests and **AWS Amplify** for authentication.

## Architecture Pattern

### 1. API Client Layer (`src/api/client.ts`)
- Centralized Axios instance with interceptors
- Automatic JWT token injection from AWS Cognito
- Request/response logging in development
- Error handling and retry logic
- Timeout configuration (300 seconds)

### 2. Service Layer (`src/api/*Service.ts`)
- Type-safe API service classes
- One service per domain (SP Analysis, Repository Analysis, etc.)
- Consistent method naming and error handling
- Support for both authenticated and public endpoints

### 3. Type Definitions (`src/types/*.ts`)
- TypeScript interfaces for requests/responses
- Ensures type safety across the application
- Matches backend Pydantic models

### 4. Custom Hooks (`src/hooks/*.ts`)
- React hooks for state management
- Polling for job status updates
- Loading and error states
- Reusable business logic

### 5. UI Components (`src/components/*`)
- Dialog components for starting analysis
- Job list components for monitoring
- Results display components

### 6. Pages (`src/pages/*.tsx`)
- Dashboard page integrating all components
- Tab-based navigation
- Coordinated state management

## Existing Integrations

### 1. SP Analysis (Stored Procedures)

**API Service**: `src/api/spAnalysisService.ts`
```typescript
export class SPAnalysisService {
  static async startAnalysis(request: SPAnalysisRequest): Promise<SPAnalysisResponse>
  static async getJobStatus(jobId: string): Promise<SPAnalysisJob>
  static async getResults(jobId: string): Promise<SPResultsResponse>
  static async downloadResults(jobId: string, filename?: string): Promise<void>
  static async listJobs(limit: number, offset: number): Promise<SPAnalysisJob[]>
  static async cancelJob(jobId: string): Promise<{ message: string }>
}
```

**Backend Endpoints**:
- `POST /api/v1/sp-analysis/analyze` - Start analysis
- `GET /api/v1/sp-analysis/status/{job_id}` - Get status
- `GET /api/v1/sp-analysis/results/{job_id}` - Get results
- `GET /api/v1/sp-analysis/jobs` - List jobs
- `DELETE /api/v1/sp-analysis/jobs/{job_id}` - Cancel job

**UI Components**:
- `SPAnalysisDialog` - Start analysis dialog
- `SPAnalysisJobs` - Job list and monitoring

**Custom Hook**: `useSPAnalysis`
- Manages job state
- Polls for status updates
- Handles loading/error states

### 2. Repository Analysis

**API Service**: `src/api/repositoryAnalysisService.ts`
```typescript
export class RepositoryAnalysisService {
  static async startAnalysis(request: RepositoryAnalysisRequest): Promise<RepositoryAnalysisResponse>
  static async getJobStatus(jobId: string): Promise<RepositoryAnalysisJob>
  static async getResults(jobId: string): Promise<RepositoryAnalysisResults>
  static async listJobs(limit: number, offset: number): Promise<RepositoryAnalysisJob[]>
  static async cancelJob(jobId: string): Promise<{ message: string }>
}
```

**Backend Endpoints**:
- `POST /api/v1/repo-analysis/analyze` - Start analysis
- `GET /api/v1/repo-analysis/status/{job_id}` - Get status
- `GET /api/v1/repo-analysis/results/{job_id}` - Get results
- `GET /api/v1/repo-analysis/jobs` - List jobs

**UI Components**:
- `RepositoryAnalysisDialog` - Start analysis dialog
- `RepositoryAnalysisJobs` - Job list and monitoring

**Custom Hook**: `useRepositoryAnalysis`

## How to Add Prefect Analysis Integration

Based on the existing pattern, here's what you need to create:

### Step 1: Create Type Definitions

**File**: `column-lineage-frontend/src/types/prefectAnalysis.ts`

```typescript
export enum PrefectAnalysisStatus {
  PENDING = 'pending',
  CLONING = 'cloning',
  ANALYZING = 'analyzing',
  COMPLETED = 'completed',
  FAILED = 'failed',
  CANCELLED = 'cancelled',
}

export interface PrefectAnalysisRequest {
  sf_environment?: string;
  max_workers?: number;
  target_directory?: string;
  skip_naming_check?: boolean;
  skip_discovery?: boolean;
  clone_all_repos?: boolean;
  specific_repos?: string[];
  async_processing?: boolean;
}

export interface PrefectAnalysisResponse {
  job_id: string;
  status: PrefectAnalysisStatus;
  message: string;
  started_at: string;
  results_url?: string;
}

export interface PrefectAnalysisJob {
  job_id: string;
  status: PrefectAnalysisStatus;
  message: string;
  started_at: string;
  completed_at?: string;
  error_message?: string;
  sf_environment: string;
  max_workers: number;
  target_directory: string;
  total_repos_found: number;
  repos_cloned: number;
  total_references: number;
  unique_tables: number;
  unique_repos: number;
  output_file?: string;
  request_params: Record<string, any>;
}

export interface PrefectRepositoryInfo {
  repo_name: string;
  clone_status: string;
  prefect_files_found: string[];
  python_files_count: number;
  has_flows: boolean;
  has_tasks: boolean;
}

export interface PrefectDiscoveryResults {
  total_repos_checked: number;
  prefect_repos_found: number;
  repositories: PrefectRepositoryInfo[];
  discovery_time_seconds: number;
}

export interface TableColumnReference {
  repo_name: string;
  function_name: string;
  table_name: string;
  column_name: string;
  file_name: string;
}

export interface PrefectAnalysisResults {
  job_id: string;
  status: PrefectAnalysisStatus;
  total_references: number;
  unique_tables: number;
  unique_repos: number;
  unique_functions: number;
  output_file: string;
  file_size: number;
  created_at: string;
  modified_at: string;
  summary: Record<string, any>;
  sample_references: TableColumnReference[];
}
```

### Step 2: Create API Service

**File**: `column-lineage-frontend/src/api/prefectAnalysisService.ts`

```typescript
/**
 * Prefect Analysis Service
 * API service for Prefect repository analysis operations
 */

import { api } from './client';
import {
  PrefectAnalysisRequest,
  PrefectAnalysisResponse,
  PrefectAnalysisJob,
  PrefectAnalysisResults,
} from '../types/prefectAnalysis';

const BASE_PATH = '/api/v1/prefect-analysis';

export class PrefectAnalysisService {
  /**
   * Start a new Prefect repository analysis
   */
  static async startAnalysis(
    request: PrefectAnalysisRequest = { 
      sf_environment: 'prod',
      max_workers: 4,
      skip_discovery: false,
      async_processing: true 
    }
  ): Promise<PrefectAnalysisResponse> {
    const response = await api.post<PrefectAnalysisResponse>(
      `${BASE_PATH}/analyze`,
      request
    );
    return response.data;
  }

  /**
   * Get analysis job status
   */
  static async getJobStatus(jobId: string): Promise<PrefectAnalysisJob> {
    const response = await api.get<PrefectAnalysisJob>(
      `${BASE_PATH}/status/${jobId}`
    );
    return response.data;
  }

  /**
   * Get analysis results
   */
  static async getResults(jobId: string): Promise<PrefectAnalysisResults> {
    const response = await api.get<PrefectAnalysisResults>(
      `${BASE_PATH}/results/${jobId}`
    );
    return response.data;
  }

  /**
   * Download analysis results CSV file
   */
  static async downloadResults(jobId: string, filename?: string): Promise<void> {
    return api.download(
      `${BASE_PATH}/results/${jobId}/download`,
      filename || `prefect_analysis_${jobId}.csv`
    );
  }

  /**
   * List all analysis jobs
   */
  static async listJobs(
    limit: number = 50,
    offset: number = 0
  ): Promise<PrefectAnalysisJob[]> {
    const response = await api.get<{ jobs: PrefectAnalysisJob[] }>(
      `${BASE_PATH}/jobs`,
      {
        params: { limit, offset },
      }
    );
    return response.data.jobs;
  }

  /**
   * Cancel an analysis job
   */
  static async cancelJob(jobId: string): Promise<{ message: string }> {
    const response = await api.delete<{ message: string }>(
      `${BASE_PATH}/jobs/${jobId}`
    );
    return response.data;
  }
}

export default PrefectAnalysisService;
```

### Step 3: Update API Index

**File**: `column-lineage-frontend/src/api/index.ts`

```typescript
// Add this line
export * from './prefectAnalysisService';
```

### Step 4: Create Custom Hook

**File**: `column-lineage-frontend/src/hooks/usePrefectAnalysis.ts`

```typescript
import { useState, useEffect, useCallback } from 'react';
import { PrefectAnalysisService } from '../api/prefectAnalysisService';
import {
  PrefectAnalysisJob,
  PrefectAnalysisStatus,
} from '../types/prefectAnalysis';

export const usePrefectAnalysis = () => {
  const [jobs, setJobs] = useState<PrefectAnalysisJob[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasRunningJob, setHasRunningJob] = useState(false);

  // Fetch jobs
  const fetchJobs = useCallback(async () => {
    try {
      setLoading(true);
      const fetchedJobs = await PrefectAnalysisService.listJobs(50, 0);
      setJobs(fetchedJobs);
      
      // Check if any job is running
      const running = fetchedJobs.some(
        job => job.status === PrefectAnalysisStatus.PENDING ||
               job.status === PrefectAnalysisStatus.CLONING ||
               job.status === PrefectAnalysisStatus.ANALYZING
      );
      setHasRunningJob(running);
      
      setError(null);
    } catch (err: any) {
      setError(err.message || 'Failed to fetch jobs');
      console.error('Error fetching Prefect analysis jobs:', err);
    } finally {
      setLoading(false);
    }
  }, []);

  // Poll for updates when there are running jobs
  useEffect(() => {
    fetchJobs();

    if (hasRunningJob) {
      const interval = setInterval(fetchJobs, 5000); // Poll every 5 seconds
      return () => clearInterval(interval);
    }
  }, [hasRunningJob, fetchJobs]);

  // Add job to state
  const addJobToState = useCallback((job: PrefectAnalysisJob) => {
    setJobs(prevJobs => [job, ...prevJobs]);
    setHasRunningJob(true);
  }, []);

  // Cancel job
  const cancelJob = useCallback(async (jobId: string) => {
    try {
      await PrefectAnalysisService.cancelJob(jobId);
      await fetchJobs();
    } catch (err: any) {
      setError(err.message || 'Failed to cancel job');
      throw err;
    }
  }, [fetchJobs]);

  return {
    jobs,
    loading,
    error,
    hasRunningJob,
    fetchJobs,
    addJobToState,
    cancelJob,
  };
};

export default usePrefectAnalysis;
```

### Step 5: Create UI Components

**File**: `column-lineage-frontend/src/components/PrefectAnalysis/PrefectAnalysisDialog.tsx`

```typescript
import React, { useState } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  TextField,
  FormControlLabel,
  Switch,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  Box,
  Typography,
  Alert,
} from '@mui/material';
import { PrefectAnalysisService } from '../../api/prefectAnalysisService';
import { PrefectAnalysisRequest, PrefectAnalysisResponse } from '../../types/prefectAnalysis';

interface PrefectAnalysisDialogProps {
  open: boolean;
  onClose: () => void;
  onAnalysisStarted: (response: PrefectAnalysisResponse) => void;
}

export const PrefectAnalysisDialog: React.FC<PrefectAnalysisDialogProps> = ({
  open,
  onClose,
  onAnalysisStarted,
}) => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [formData, setFormData] = useState<PrefectAnalysisRequest>({
    sf_environment: 'prod',
    max_workers: 4,
    target_directory: 'prefect_repos',
    skip_discovery: false,
    skip_naming_check: false,
    clone_all_repos: false,
    async_processing: true,
  });

  const handleSubmit = async () => {
    try {
      setLoading(true);
      setError(null);
      const response = await PrefectAnalysisService.startAnalysis(formData);
      onAnalysisStarted(response);
      onClose();
    } catch (err: any) {
      setError(err.message || 'Failed to start analysis');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Dialog open={open} onClose={onClose} maxWidth="sm" fullWidth>
      <DialogTitle>Start Prefect Repository Analysis</DialogTitle>
      <DialogContent>
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, mt: 2 }}>
          {error && <Alert severity="error">{error}</Alert>}
          
          <FormControl fullWidth>
            <InputLabel>Snowflake Environment</InputLabel>
            <Select
              value={formData.sf_environment}
              onChange={(e) => setFormData({ ...formData, sf_environment: e.target.value })}
            >
              <MenuItem value="dev">Development</MenuItem>
              <MenuItem value="stage">Staging</MenuItem>
              <MenuItem value="prod">Production</MenuItem>
            </Select>
          </FormControl>

          <TextField
            label="Max Workers"
            type="number"
            value={formData.max_workers}
            onChange={(e) => setFormData({ ...formData, max_workers: parseInt(e.target.value) })}
            inputProps={{ min: 1, max: 10 }}
            fullWidth
          />

          <TextField
            label="Target Directory"
            value={formData.target_directory}
            onChange={(e) => setFormData({ ...formData, target_directory: e.target.value })}
            fullWidth
          />

          <FormControlLabel
            control={
              <Switch
                checked={!formData.skip_discovery}
                onChange={(e) => setFormData({ ...formData, skip_discovery: !e.target.checked })}
              />
            }
            label="Enable Content-Based Discovery (finds ~99 repos, slower)"
          />

          <FormControlLabel
            control={
              <Switch
                checked={formData.skip_naming_check}
                onChange={(e) => setFormData({ ...formData, skip_naming_check: e.target.checked })}
              />
            }
            label="Skip Naming Pattern Check"
          />

          <FormControlLabel
            control={
              <Switch
                checked={formData.clone_all_repos}
                onChange={(e) => setFormData({ ...formData, clone_all_repos: e.target.checked })}
              />
            }
            label="Clone All Repositories (comprehensive)"
          />

          <Typography variant="caption" color="text.secondary">
            {formData.skip_discovery 
              ? "Fast mode: Only ~20 repos (naming patterns only)"
              : "Comprehensive mode: ~99 repos (naming + content discovery)"}
          </Typography>
        </Box>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button onClick={handleSubmit} variant="contained" disabled={loading}>
          {loading ? 'Starting...' : 'Start Analysis'}
        </Button>
      </DialogActions>
    </Dialog>
  );
};
```

**File**: `column-lineage-frontend/src/components/PrefectAnalysis/PrefectAnalysisJobs.tsx`

```typescript
import React from 'react';
import {
  Box,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Chip,
  IconButton,
  Button,
  Typography,
  CircularProgress,
} from '@mui/material';
import { Download, Cancel, Refresh } from '@mui/icons-material';
import { usePrefectAnalysis } from '../../hooks/usePrefectAnalysis';
import { PrefectAnalysisService } from '../../api/prefectAnalysisService';
import { PrefectAnalysisStatus } from '../../types/prefectAnalysis';

interface PrefectAnalysisJobsProps {
  onNewAnalysis: () => void;
}

export const PrefectAnalysisJobs: React.FC<PrefectAnalysisJobsProps> = ({ onNewAnalysis }) => {
  const { jobs, loading, error, hasRunningJob, fetchJobs, cancelJob } = usePrefectAnalysis();

  const getStatusColor = (status: PrefectAnalysisStatus) => {
    switch (status) {
      case PrefectAnalysisStatus.COMPLETED:
        return 'success';
      case PrefectAnalysisStatus.FAILED:
        return 'error';
      case PrefectAnalysisStatus.CANCELLED:
        return 'default';
      default:
        return 'primary';
    }
  };

  const handleDownload = async (jobId: string) => {
    try {
      await PrefectAnalysisService.downloadResults(jobId);
    } catch (err) {
      console.error('Download failed:', err);
    }
  };

  const handleCancel = async (jobId: string) => {
    if (window.confirm('Are you sure you want to cancel this job?')) {
      try {
        await cancelJob(jobId);
      } catch (err) {
        console.error('Cancel failed:', err);
      }
    }
  };

  return (
    <Box>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 2 }}>
        <Typography variant="h6">Prefect Analysis Jobs</Typography>
        <Box>
          <IconButton onClick={fetchJobs} disabled={loading}>
            <Refresh />
          </IconButton>
          <Button variant="contained" onClick={onNewAnalysis} disabled={hasRunningJob}>
            New Analysis
          </Button>
        </Box>
      </Box>

      {error && <Typography color="error">{error}</Typography>}

      <TableContainer component={Paper}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Job ID</TableCell>
              <TableCell>Status</TableCell>
              <TableCell>Environment</TableCell>
              <TableCell>Repos Found</TableCell>
              <TableCell>Repos Cloned</TableCell>
              <TableCell>References</TableCell>
              <TableCell>Started</TableCell>
              <TableCell>Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {loading && jobs.length === 0 ? (
              <TableRow>
                <TableCell colSpan={8} align="center">
                  <CircularProgress />
                </TableCell>
              </TableRow>
            ) : jobs.length === 0 ? (
              <TableRow>
                <TableCell colSpan={8} align="center">
                  No jobs found
                </TableCell>
              </TableRow>
            ) : (
              jobs.map((job) => (
                <TableRow key={job.job_id}>
                  <TableCell>{job.job_id.substring(0, 8)}...</TableCell>
                  <TableCell>
                    <Chip label={job.status} color={getStatusColor(job.status)} size="small" />
                  </TableCell>
                  <TableCell>{job.sf_environment}</TableCell>
                  <TableCell>{job.total_repos_found}</TableCell>
                  <TableCell>{job.repos_cloned}</TableCell>
                  <TableCell>{job.total_references}</TableCell>
                  <TableCell>{new Date(job.started_at).toLocaleString()}</TableCell>
                  <TableCell>
                    {job.status === PrefectAnalysisStatus.COMPLETED && (
                      <IconButton onClick={() => handleDownload(job.job_id)} size="small">
                        <Download />
                      </IconButton>
                    )}
                    {(job.status === PrefectAnalysisStatus.PENDING ||
                      job.status === PrefectAnalysisStatus.CLONING ||
                      job.status === PrefectAnalysisStatus.ANALYZING) && (
                      <IconButton onClick={() => handleCancel(job.job_id)} size="small">
                        <Cancel />
                      </IconButton>
                    )}
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  );
};
```

**File**: `column-lineage-frontend/src/components/PrefectAnalysis/index.ts`

```typescript
export { PrefectAnalysisDialog } from './PrefectAnalysisDialog';
export { PrefectAnalysisJobs } from './PrefectAnalysisJobs';
```

### Step 6: Integrate into Dashboard

**File**: `column-lineage-frontend/src/pages/DashboardPage.tsx`

Add these imports:
```typescript
import { PrefectAnalysisDialog, PrefectAnalysisJobs } from '../components/PrefectAnalysis'
import usePrefectAnalysis from '../hooks/usePrefectAnalysis'
import { PrefectAnalysisResponse } from '../types/prefectAnalysis'
```

Add state:
```typescript
const [prefectAnalysisDialogOpen, setPrefectAnalysisDialogOpen] = useState(false)
const { hasRunningJob: hasRunningPrefectJob } = usePrefectAnalysis()
```

Add handlers:
```typescript
const handleStartPrefectAnalysis = () => {
  if (hasRunningPrefectJob) {
    // Show warning but still allow opening dialog
  }
  setPrefectAnalysisDialogOpen(true)
}

const handlePrefectAnalysisStarted = (_response: PrefectAnalysisResponse) => {
  setCurrentTab(1) // Switch to Analysis Jobs tab
  setAnalysisJobsSubTab(3) // Switch to Prefect Analysis sub-tab
  setPrefectAnalysisDialogOpen(false)
}

const handleClosePrefectAnalysisDialog = () => {
  setPrefectAnalysisDialogOpen(false)
}
```

Add button in UI:
```typescript
<Button
  variant="contained"
  color="secondary"
  size="large"
  startIcon={<PlayArrow />}
  onClick={handleStartPrefectAnalysis}
  disabled={hasRunningPrefectJob}
  title={hasRunningPrefectJob ? 'Please wait for current Prefect analysis to complete' : 'Start Prefect repository analysis'}
>
  Prefect Analysis
</Button>
```

Add tab:
```typescript
{analysisJobsSubTab === 3 && (
  <PrefectAnalysisJobs 
    onNewAnalysis={handleStartPrefectAnalysis}
  />
)}
```

Add dialog:
```typescript
<PrefectAnalysisDialog
  open={prefectAnalysisDialogOpen}
  onClose={handleClosePrefectAnalysisDialog}
  onAnalysisStarted={handlePrefectAnalysisStarted}
/>
```

## Summary

Your frontend follows a clean, layered architecture:

1. **API Client** → Handles HTTP requests, auth, errors
2. **Services** → Type-safe API methods
3. **Types** → TypeScript interfaces
4. **Hooks** → State management and polling
5. **Components** → Reusable UI elements
6. **Pages** → Application views

To add Prefect Analysis, you need to:
1. Create type definitions
2. Create API service (following SP/Repo pattern)
3. Create custom hook for state management
4. Create UI components (dialog + jobs list)
5. Integrate into dashboard page

The pattern is consistent across all analysis types, making it easy to add new features!
