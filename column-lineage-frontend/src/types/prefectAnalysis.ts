/**
 * Prefect Analysis Type Definitions
 */

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

// Helper function to get estimated completion time
export const getEstimatedCompletionTime = (
  status: PrefectAnalysisStatus,
  startedAt: string,
  skipDiscovery: boolean = false
): { minutes: number; message: string } => {
  const elapsed = Math.floor((Date.now() - new Date(startedAt).getTime()) / 60000);
  
  if (skipDiscovery) {
    // Fast mode: ~11 minutes total
    switch (status) {
      case PrefectAnalysisStatus.PENDING:
        return { minutes: 11, message: 'Starting analysis...' };
      case PrefectAnalysisStatus.CLONING:
        return { minutes: Math.max(1, 10 - elapsed), message: 'Cloning ~20 repositories...' };
      case PrefectAnalysisStatus.ANALYZING:
        return { minutes: Math.max(1, 11 - elapsed), message: 'Analyzing table-column references...' };
      default:
        return { minutes: 0, message: '' };
    }
  } else {
    // Comprehensive mode: ~20 minutes total
    switch (status) {
      case PrefectAnalysisStatus.PENDING:
        return { minutes: 20, message: 'Starting analysis...' };
      case PrefectAnalysisStatus.CLONING:
        return { minutes: Math.max(1, 8 - elapsed), message: 'Discovering and cloning ~99 repositories...' };
      case PrefectAnalysisStatus.ANALYZING:
        return { minutes: Math.max(1, 20 - elapsed), message: 'Analyzing table-column references...' };
      default:
        return { minutes: 0, message: '' };
    }
  }
};

// Helper function to get polling interval based on status
export const getPollingInterval = (status: PrefectAnalysisStatus): number | null => {
  switch (status) {
    case PrefectAnalysisStatus.PENDING:
      return 10000; // 10 seconds - quick startup
    case PrefectAnalysisStatus.CLONING:
      return 30000; // 30 seconds - long cloning process
    case PrefectAnalysisStatus.ANALYZING:
      return 60000; // 60 seconds - very long analysis
    case PrefectAnalysisStatus.COMPLETED:
    case PrefectAnalysisStatus.FAILED:
    case PrefectAnalysisStatus.CANCELLED:
      return null; // Stop polling
    default:
      return null;
  }
};
