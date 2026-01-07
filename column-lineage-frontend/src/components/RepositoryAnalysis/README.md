# Repository Analysis Components

This directory contains components for repository analysis functionality.

## Components

### RepositoryAnalysisDialog
- Dialog component for starting repository analysis
- Handles API calls to start analysis jobs
- Shows job status and confirmation

### RepositoryAnalysisJobs
- Displays list of repository analysis jobs
- Shows job status, progress, and results
- Allows canceling running jobs and viewing completed results
- Auto-refreshes job status every 10 seconds

## Features

- **Start Analysis**: Initiate repository structure and dependency analysis
- **Job Management**: View, monitor, and cancel analysis jobs
- **Results Viewing**: View detailed analysis results for completed jobs
- **Real-time Updates**: Auto-refresh job status and progress
- **Error Handling**: Comprehensive error handling and user feedback

## Integration

The components are integrated into the main Dashboard through:
- New "Start to Repo Analyze" button in the Column Lineage tab
- Repository Analysis Jobs tab in the Analysis Jobs section
- Shared job management interface with column lineage analysis

## API Integration

Uses the `RepositoryAnalysisService` to communicate with the backend API endpoints:
- `/api/v1/repository-analysis/analyze` - Start analysis
- `/api/v1/repository-analysis/status/{job_id}` - Get job status
- `/api/v1/repository-analysis/jobs` - List all jobs
- `/api/v1/repository-analysis/jobs/{job_id}` - Cancel job
- `/api/v1/repository-analysis/results/{job_id}` - Get results

## Types

TypeScript types are defined in `src/types/repositoryAnalysis.ts` for type safety and better development experience.