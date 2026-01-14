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
      skip_discovery: false, // Comprehensive mode by default
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
    const response = await api.get<PrefectAnalysisJob[]>(
      `${BASE_PATH}/jobs`,
      {
        params: { limit, offset },
      }
    );
    return response.data;
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
