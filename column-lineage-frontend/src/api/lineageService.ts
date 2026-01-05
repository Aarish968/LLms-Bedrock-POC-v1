import { api, apiUtils, AxiosError } from './client';

// Types for lineage analysis API
export interface LineageAnalysisRequest {
  view_names?: string[];
  database_filter?: string;  // Made optional for now
  schema_filter?: string;    // Made optional for now
  async_processing?: boolean;
  include_metadata?: boolean;
}

export interface LineageAnalysisResponse {
  job_id: string;
  status: 'PENDING' | 'RUNNING' | 'COMPLETED' | 'FAILED' | 'CANCELLED';
  message: string;
  results_url: string;
}

export interface LineageAnalysisJob {
  job_id: string;
  status: 'PENDING' | 'RUNNING' | 'COMPLETED' | 'FAILED' | 'CANCELLED';
  created_at: string;
  updated_at: string;
  total_views: number;
  processed_views: number;
  results_count: number;
  error_message?: string;
  request_params: Record<string, any>;
}

export interface ViewInfo {
  schema_name: string;
  view_name: string;
  database_name: string;
  column_count: number;
  created_date?: string;
  last_modified?: string;
}

export interface LineageResult {
  view_name: string;
  view_column: string;
  source_table: string;
  source_column: string;
  confidence_score: number;
  column_type?: string;
  expression_type?: string;
  metadata?: Record<string, any>;
}

export interface LineageResultsResponse {
  job_id: string;
  status: string;
  total_results: number;
  results: LineageResult[];
  summary?: Record<string, any>;
}

export const lineageService = {
  /**
   * Start column lineage analysis
   */
  async startAnalysis(request: LineageAnalysisRequest): Promise<LineageAnalysisResponse> {
    try {
      const response = await api.post<LineageAnalysisResponse>('/api/v1/lineage/analyze', request);
      return response.data;
    } catch (error) {
      const axiosError = error as AxiosError;
      
      if (apiUtils.isNetworkError(axiosError)) {
        throw new Error('Network error - please check your connection');
      }
      
      if (apiUtils.isTimeoutError(axiosError)) {
        throw new Error('Request timeout - please try again');
      }
      
      if (apiUtils.getErrorStatus(axiosError) === 401) {
        throw new Error('Authentication required - please log in');
      }
      
      if (apiUtils.getErrorStatus(axiosError) === 403) {
        throw new Error('Access forbidden - insufficient permissions');
      }
      
      if (apiUtils.isServerError(axiosError)) {
        throw new Error('Server error - please try again later');
      }
      
      const errorMessage = apiUtils.getErrorMessage(axiosError);
      throw new Error(errorMessage || 'Failed to start lineage analysis');
    }
  },

  /**
   * Get job status
   */
  async getJobStatus(jobId: string): Promise<LineageAnalysisJob> {
    try {
      const response = await api.get<LineageAnalysisJob>(`/api/v1/lineage/status/${jobId}`);
      return response.data;
    } catch (error) {
      const axiosError = error as AxiosError;
      
      if (apiUtils.getErrorStatus(axiosError) === 404) {
        throw new Error('Job not found');
      }
      
      const errorMessage = apiUtils.getErrorMessage(axiosError);
      throw new Error(errorMessage || 'Failed to get job status');
    }
  },

  /**
   * Get analysis results
   */
  async getResults(jobId: string, limit?: number, offset: number = 0): Promise<LineageResultsResponse> {
    try {
      const response = await api.get<LineageResultsResponse>(`/api/v1/lineage/results/${jobId}`, {
        params: {
          limit,
          offset,
        },
      });
      return response.data;
    } catch (error) {
      const axiosError = error as AxiosError;
      
      if (apiUtils.getErrorStatus(axiosError) === 404) {
        throw new Error('Job not found');
      }
      
      if (apiUtils.getErrorStatus(axiosError) === 400) {
        throw new Error('Job is not completed yet');
      }
      
      const errorMessage = apiUtils.getErrorMessage(axiosError);
      throw new Error(errorMessage || 'Failed to get analysis results');
    }
  },

  /**
   * List available views
   */
  async getAvailableViews(
    databaseFilter: string, 
    schemaFilter: string, 
    limit: number = 100, 
    offset: number = 0
  ): Promise<ViewInfo[]> {
    try {
      // Use public endpoint with mandatory filters
      const response = await api.get<ViewInfo[]>('/api/v1/lineage/public/views', {
        params: {
          database_filter: databaseFilter,
          schema_filter: schemaFilter,
          limit,
          offset,
        },
      });
      return response.data;
    } catch (error) {
      const axiosError = error as AxiosError;
      
      if (apiUtils.getErrorStatus(axiosError) === 401) {
        throw new Error('Authentication required - please log in');
      }
      
      const errorMessage = apiUtils.getErrorMessage(axiosError);
      throw new Error(errorMessage || 'Failed to get available views');
    }
  },

  /**
   * Cancel a running job
   */
  async cancelJob(jobId: string): Promise<void> {
    try {
      await api.delete(`/api/v1/lineage/jobs/${jobId}`);
    } catch (error) {
      const axiosError = error as AxiosError;
      
      if (apiUtils.getErrorStatus(axiosError) === 404) {
        throw new Error('Job not found');
      }
      
      if (apiUtils.getErrorStatus(axiosError) === 400) {
        throw new Error('Cannot cancel job - it may already be completed');
      }
      
      const errorMessage = apiUtils.getErrorMessage(axiosError);
      throw new Error(errorMessage || 'Failed to cancel job');
    }
  },

  /**
   * List all jobs
   */
  async listJobs(statusFilter?: string, limit: number = 50, offset: number = 0): Promise<LineageAnalysisJob[]> {
    try {
      const response = await api.get<LineageAnalysisJob[]>('/api/v1/lineage/jobs', {
        params: {
          status_filter: statusFilter,
          limit,
          offset,
        },
      });
      return response.data;
    } catch (error) {
      const axiosError = error as AxiosError;
      
      const errorMessage = apiUtils.getErrorMessage(axiosError);
      throw new Error(errorMessage || 'Failed to list jobs');
    }
  },
};