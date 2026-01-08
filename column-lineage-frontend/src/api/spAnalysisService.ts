/**
 * Stored Procedure Analysis Service
 * API service for stored procedure analysis operations
 */

import { api } from './client';
import {
  SPAnalysisRequest,
  SPAnalysisResponse,
  SPAnalysisJob,
  SPResultsResponse,
  SingleProcedureRequest,
  StoredProcedureAnalysis,
  ProcedureListResponse,
} from '../types/spAnalysis';

const BASE_PATH = '/api/v1/sp-analysis';

export class SPAnalysisService {
  /**
   * Start a new stored procedure analysis
   */
  static async startAnalysis(
    request: SPAnalysisRequest = { 
      sf_environment: 'prod',
      max_workers: 4,
      resume_from_partial: true 
    }
  ): Promise<SPAnalysisResponse> {
    const response = await api.post<SPAnalysisResponse>(
      `${BASE_PATH}/analyze`,
      request
    );
    return response.data;
  }

  /**
   * Get analysis job status
   */
  static async getJobStatus(jobId: string): Promise<SPAnalysisJob> {
    const response = await api.get<SPAnalysisJob>(
      `${BASE_PATH}/status/${jobId}`
    );
    return response.data;
  }

  /**
   * Get analysis results
   */
  static async getResults(jobId: string): Promise<SPResultsResponse> {
    const response = await api.get<SPResultsResponse>(
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
      filename || `sp_analysis_${jobId}.csv`
    );
  }

  /**
   * Analyze a single stored procedure
   */
  static async analyzeSingleProcedure(
    request: SingleProcedureRequest
  ): Promise<StoredProcedureAnalysis> {
    const response = await api.post<StoredProcedureAnalysis>(
      `${BASE_PATH}/analyze/single`,
      request
    );
    return response.data;
  }

  /**
   * List all stored procedures from Snowflake
   */
  static async listProcedures(
    sf_environment: string = 'prod'
  ): Promise<ProcedureListResponse> {
    const response = await api.get<ProcedureListResponse>(
      `${BASE_PATH}/procedures`,
      {
        params: { sf_environment },
      }
    );
    return response.data;
  }

  /**
   * List all analysis jobs
   */
  static async listJobs(
    limit: number = 50,
    offset: number = 0
  ): Promise<SPAnalysisJob[]> {
    const response = await api.get<{ jobs: SPAnalysisJob[] }>(
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

  // Public endpoints (for testing without authentication)

  /**
   * Start analysis using public endpoint (for testing)
   */
  static async startAnalysisPublic(
    request: SPAnalysisRequest = { 
      sf_environment: 'prod',
      max_workers: 4,
      resume_from_partial: true 
    }
  ): Promise<SPAnalysisResponse> {
    const response = await api.post<SPAnalysisResponse>(
      `${BASE_PATH}/public/analyze`,
      request
    );
    return response.data;
  }

  /**
   * Get job status using public endpoint (for testing)
   */
  static async getJobStatusPublic(jobId: string): Promise<SPAnalysisJob> {
    const response = await api.get<SPAnalysisJob>(
      `${BASE_PATH}/public/status/${jobId}`
    );
    return response.data;
  }

  /**
   * Get results using public endpoint (for testing)
   */
  static async getResultsPublic(jobId: string): Promise<SPResultsResponse> {
    const response = await api.get<SPResultsResponse>(
      `${BASE_PATH}/public/results/${jobId}`
    );
    return response.data;
  }
}

export default SPAnalysisService;