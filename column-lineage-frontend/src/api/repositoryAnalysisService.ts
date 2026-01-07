/**
 * Repository Analysis Service
 * API service for repository analysis operations
 */

import { api } from './client';
import {
  RepositoryAnalysisRequest,
  RepositoryAnalysisResponse,
  RepositoryAnalysisJob,
  RepositoryAnalysisResults,
} from '../types/repositoryAnalysis';

const BASE_PATH = '/api/v1/repo-analysis';

export class RepositoryAnalysisService {
  /**
   * Start a new repository analysis
   */
  static async startAnalysis(
    request: RepositoryAnalysisRequest = { async_processing: true }
  ): Promise<RepositoryAnalysisResponse> {
    const response = await api.post<RepositoryAnalysisResponse>(
      `${BASE_PATH}/analyze`,
      request
    );
    return response.data;
  }

  /**
   * Get analysis job status
   */
  static async getJobStatus(jobId: string): Promise<RepositoryAnalysisJob> {
    const response = await api.get<RepositoryAnalysisJob>(
      `${BASE_PATH}/status/${jobId}`
    );
    return response.data;
  }

  /**
   * List all analysis jobs
   */
  static async listJobs(
    limit: number = 50,
    offset: number = 0
  ): Promise<RepositoryAnalysisJob[]> {
    const response = await api.get<RepositoryAnalysisJob[]>(
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

  /**
   * Get analysis results
   */
  static async getResults(jobId: string): Promise<RepositoryAnalysisResults> {
    const response = await api.get<RepositoryAnalysisResults>(
      `${BASE_PATH}/results/${jobId}`
    );
    return response.data;
  }

  /**
   * Start analysis using public endpoint (for testing)
   */
  static async startAnalysisPublic(
    request: RepositoryAnalysisRequest = { async_processing: true }
  ): Promise<RepositoryAnalysisResponse> {
    const response = await api.post<RepositoryAnalysisResponse>(
      `${BASE_PATH}/public/analyze`,
      request
    );
    return response.data;
  }

  /**
   * Get job status using public endpoint (for testing)
   */
  static async getJobStatusPublic(jobId: string): Promise<RepositoryAnalysisJob> {
    const response = await api.get<RepositoryAnalysisJob>(
      `${BASE_PATH}/public/status/${jobId}`
    );
    return response.data;
  }

  /**
   * List jobs using public endpoint (for testing)
   */
  static async listJobsPublic(
    limit: number = 50,
    offset: number = 0
  ): Promise<RepositoryAnalysisJob[]> {
    const response = await api.get<RepositoryAnalysisJob[]>(
      `${BASE_PATH}/public/jobs`,
      {
        params: { limit, offset },
      }
    );
    return response.data;
  }
}

export default RepositoryAnalysisService;