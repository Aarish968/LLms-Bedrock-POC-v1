/**
 * Repository Analysis Types
 * TypeScript definitions for repository analysis API
 */

export enum AnalysisStatus {
  PENDING = 'pending',
  CLONING = 'cloning',
  RUNNING = 'running',
  COMPLETED = 'completed',
  FAILED = 'failed',
  CANCELLED = 'cancelled',
}

export interface RepositoryAnalysisRequest {
  async_processing?: boolean;
}

export interface RepositoryAnalysisResponse {
  job_id: string;
  status: AnalysisStatus;
  message: string;
  output_file?: string;
  started_at: string;
}

export interface RepositoryAnalysisJob {
  job_id: string;
  status: AnalysisStatus;
  message: string;
  output_file?: string;
  started_at: string;
  completed_at?: string;
  error_message?: string;
  frontend_repo_name?: string;
  backend_repo_name?: string;
}

export interface RepositoryAnalysisResults {
  job_id: string;
  status: AnalysisStatus;
  output_file: string;
  file_size: number;
  created_at: string;
  modified_at: string;
  message: string;
}

export interface RepositoryAnalysisJobsResponse {
  jobs: RepositoryAnalysisJob[];
  total: number;
}