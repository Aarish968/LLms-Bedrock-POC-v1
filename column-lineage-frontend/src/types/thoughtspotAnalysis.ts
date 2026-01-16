/**
 * ThoughtSpot Analysis Type Definitions
 */

export enum TSAnalysisStatus {
  PENDING = 'PENDING',
  RUNNING = 'RUNNING',
  COMPLETED = 'COMPLETED',
  FAILED = 'FAILED',
  CANCELLED = 'CANCELLED',
}

export interface TSAnalysisRequest {
  sf_environment?: string;
  table_pattern?: string | null;
  max_workers?: number;
  include_views?: boolean;
  force_prod_urls?: boolean;
}

export interface TSAnalysisResponse {
  job_id: string;
  status: TSAnalysisStatus;
  message: string;
  started_at: string;
  results_url?: string;
}

export interface TSAnalysisJob {
  job_id: string;
  status: TSAnalysisStatus;
  message?: string;
  started_at: string;
  completed_at?: string;
  error_message?: string;
  sf_environment: string;
  table_pattern?: string | null;
  max_workers: number;
  include_views: boolean;
  force_prod_urls: boolean;
  total_tables: number;
  processed_tables: number;
  total_relationships: number;
  result_file?: string;
  request_params: Record<string, any>;
}

export interface TableLiveboardRelationship {
  table_name: string;
  liveboard_name: string;
  guid: string;
  schema: string;
  type: string;
}

export interface TSAnalysisResults {
  job_id: string;
  status: TSAnalysisStatus;
  total_tables: number;
  total_relationships: number;
  unique_liveboards: number;
  result_file?: string;
  download_url?: string;
  summary: Record<string, any>;
}

// Helper function to get estimated completion time
export const getEstimatedCompletionTime = (
  status: TSAnalysisStatus,
  startedAt: string,
  totalTables: number = 150
): { minutes: number; message: string } => {
  const elapsed = Math.floor((Date.now() - new Date(startedAt).getTime()) / 60000);
  
  // Estimate: ~2-3 seconds per table
  const estimatedMinutes = Math.ceil((totalTables * 2.5) / 60);
  
  switch (status) {
    case TSAnalysisStatus.PENDING:
      return { minutes: estimatedMinutes, message: 'Starting analysis...' };
    case TSAnalysisStatus.RUNNING:
      return { 
        minutes: Math.max(1, estimatedMinutes - elapsed), 
        message: `Analyzing ${totalTables} tables for ThoughtSpot liveboards...` 
      };
    default:
      return { minutes: 0, message: '' };
  }
};

// Helper function to get polling interval based on status
export const getPollingInterval = (status: TSAnalysisStatus): number | null => {
  switch (status) {
    case TSAnalysisStatus.PENDING:
      return 10000; // 10 seconds - quick startup
    case TSAnalysisStatus.RUNNING:
      return 30000; // 30 seconds - analysis in progress
    case TSAnalysisStatus.COMPLETED:
    case TSAnalysisStatus.FAILED:
    case TSAnalysisStatus.CANCELLED:
      return null; // Stop polling
    default:
      return null;
  }
};
