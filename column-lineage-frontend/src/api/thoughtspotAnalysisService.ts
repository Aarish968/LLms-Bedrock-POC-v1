/**
 * ThoughtSpot Analysis Service
 * API service for ThoughtSpot liveboard analysis operations
 */

import { api } from './client';
import {
  TSAnalysisRequest,
  TSAnalysisResponse,
  TSAnalysisJob,
  TSAnalysisResults,
} from '../types/thoughtspotAnalysis';

const BASE_PATH = '/api/v1/thoughtspot-analysis';

export class ThoughtSpotAnalysisService {
  /**
   * Start a new ThoughtSpot liveboard analysis
   */
  static async startAnalysis(
    request: TSAnalysisRequest = { 
      sf_environment: 'prod',
      max_workers: 5,
      include_views: true,
      force_prod_urls: true,
      table_pattern: null
    }
  ): Promise<TSAnalysisResponse> {
    const response = await api.post<TSAnalysisResponse>(
      `${BASE_PATH}/analyze`,
      request
    );
    return response.data;
  }

  /**
   * Get analysis job status
   */
  static async getJobStatus(jobId: string): Promise<TSAnalysisJob> {
    const response = await api.get<TSAnalysisJob>(
      `${BASE_PATH}/status/${jobId}`
    );
    return response.data;
  }

  /**
   * Get analysis results
   */
  static async getResults(jobId: string): Promise<TSAnalysisResults> {
    const response = await api.get<TSAnalysisResults>(
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
      filename || `thoughtspot_analysis_${jobId}.csv`
    );
  }

  /**
   * List all analysis jobs
   */
  static async listJobs(
    limit: number = 50,
    offset: number = 0
  ): Promise<TSAnalysisJob[]> {
    const response = await api.get<{ jobs: TSAnalysisJob[] }>(
      `${BASE_PATH}/jobs`,
      {
        params: { limit, offset },
      }
    );
    return response.data.jobs;
  }

  /**
   * Delete an analysis job
   */
  static async deleteJob(jobId: string): Promise<{ message: string }> {
    const response = await api.delete<{ message: string }>(
      `${BASE_PATH}/jobs/${jobId}`
    );
    return response.data;
  }
}

export default ThoughtSpotAnalysisService;
