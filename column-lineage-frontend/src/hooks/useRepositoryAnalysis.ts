/**
 * Repository Analysis Hook
 * Custom hook for managing repository analysis state and operations
 */

import { useState, useCallback } from 'react';
import { RepositoryAnalysisService } from '../api/repositoryAnalysisService';
import {
  RepositoryAnalysisJob,
  RepositoryAnalysisResponse,
  AnalysisStatus,
} from '../types/repositoryAnalysis';

interface UseRepositoryAnalysisReturn {
  jobs: RepositoryAnalysisJob[];
  isLoading: boolean;
  error: string | null;
  startAnalysis: () => Promise<RepositoryAnalysisResponse | null>;
  refreshJobs: () => Promise<void>;
  getJobStatus: (jobId: string) => Promise<RepositoryAnalysisJob | null>;
  cancelJob: (jobId: string) => Promise<boolean>;
}

export const useRepositoryAnalysis = (): UseRepositoryAnalysisReturn => {
  const [jobs, setJobs] = useState<RepositoryAnalysisJob[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const startAnalysis = useCallback(async (): Promise<RepositoryAnalysisResponse | null> => {
    setIsLoading(true);
    setError(null);

    try {
      const response = await RepositoryAnalysisService.startAnalysis({
        async_processing: true,
      });

      // Add the new job to the jobs list
      const newJob: RepositoryAnalysisJob = {
        job_id: response.job_id,
        status: response.status,
        message: response.message,
        output_file: response.output_file,
        started_at: response.started_at,
      };

      setJobs(prevJobs => [newJob, ...prevJobs]);
      return response;

    } catch (err: any) {
      const errorMessage = err.response?.data?.detail || err.message || 'Failed to start analysis';
      setError(errorMessage);
      console.error('Failed to start repository analysis:', err);
      return null;

    } finally {
      setIsLoading(false);
    }
  }, []);

  const refreshJobs = useCallback(async (): Promise<void> => {
    setIsLoading(true);
    setError(null);

    try {
      const jobsList = await RepositoryAnalysisService.listJobs(50, 0);
      setJobs(jobsList);

    } catch (err: any) {
      const errorMessage = err.response?.data?.detail || err.message || 'Failed to fetch jobs';
      setError(errorMessage);
      console.error('Failed to fetch repository analysis jobs:', err);

    } finally {
      setIsLoading(false);
    }
  }, []);

  const getJobStatus = useCallback(async (jobId: string): Promise<RepositoryAnalysisJob | null> => {
    try {
      const job = await RepositoryAnalysisService.getJobStatus(jobId);
      
      // Update the job in the jobs list
      setJobs(prevJobs => 
        prevJobs.map(prevJob => 
          prevJob.job_id === jobId ? job : prevJob
        )
      );

      return job;

    } catch (err: any) {
      console.error(`Failed to get job status for ${jobId}:`, err);
      return null;
    }
  }, []);

  const cancelJob = useCallback(async (jobId: string): Promise<boolean> => {
    try {
      await RepositoryAnalysisService.cancelJob(jobId);
      
      // Update the job status in the jobs list
      setJobs(prevJobs => 
        prevJobs.map(job => 
          job.job_id === jobId 
            ? { ...job, status: AnalysisStatus.CANCELLED, message: 'Job cancelled' }
            : job
        )
      );

      return true;

    } catch (err: any) {
      const errorMessage = err.response?.data?.detail || err.message || 'Failed to cancel job';
      setError(errorMessage);
      console.error(`Failed to cancel job ${jobId}:`, err);
      return false;
    }
  }, []);

  return {
    jobs,
    isLoading,
    error,
    startAnalysis,
    refreshJobs,
    getJobStatus,
    cancelJob,
  };
};

export default useRepositoryAnalysis;