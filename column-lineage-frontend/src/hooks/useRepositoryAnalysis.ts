/**
 * Repository Analysis Hook
 * Custom hook for managing repository analysis state and operations
 */

import { useState, useCallback, useEffect } from 'react';
import { RepositoryAnalysisService } from '../api/repositoryAnalysisService';
import {
  RepositoryAnalysisJob,
  RepositoryAnalysisResponse,
  AnalysisStatus,
} from '../types/repositoryAnalysis';

// Global state to share between hook instances
let globalJobs: RepositoryAnalysisJob[] = [];
let globalJobsListeners: Set<(jobs: RepositoryAnalysisJob[]) => void> = new Set();

const notifyJobsListeners = (jobs: RepositoryAnalysisJob[]) => {
  globalJobs = jobs;
  globalJobsListeners.forEach(listener => listener(jobs));
};

interface UseRepositoryAnalysisReturn {
  jobs: RepositoryAnalysisJob[];
  isLoading: boolean;
  error: string | null;
  hasRunningJob: boolean;
  startAnalysis: () => Promise<RepositoryAnalysisResponse | null>;
  refreshJobs: () => Promise<void>;
  getJobStatus: (jobId: string) => Promise<RepositoryAnalysisJob | null>;
  cancelJob: (jobId: string) => Promise<boolean>;
  addJobToState: (job: RepositoryAnalysisJob) => void;
}

export const useRepositoryAnalysis = (): UseRepositoryAnalysisReturn => {
  const [jobs, setJobs] = useState<RepositoryAnalysisJob[]>(globalJobs);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Subscribe to global jobs changes
  useEffect(() => {
    const listener = (newJobs: RepositoryAnalysisJob[]) => {
      setJobs(newJobs);
    };
    
    globalJobsListeners.add(listener);
    
    return () => {
      globalJobsListeners.delete(listener);
    };
  }, []);

  // Check if there's any running job
  const hasRunningJob = jobs.some(job => 
    job.status === AnalysisStatus.PENDING || 
    job.status === AnalysisStatus.CLONING || 
    job.status === AnalysisStatus.RUNNING
  );

  // Define refreshJobs first
  const refreshJobs = useCallback(async (): Promise<void> => {
    setIsLoading(true);
    setError(null);

    try {
      const jobsList = await RepositoryAnalysisService.listJobs(50, 0);
      notifyJobsListeners(jobsList);

    } catch (err: any) {
      const errorMessage = err.response?.data?.detail || err.message || 'Failed to fetch jobs';
      setError(errorMessage);
      console.error('Failed to fetch repository analysis jobs:', err);

    } finally {
      setIsLoading(false);
    }
  }, []);

  const startAnalysis = useCallback(async (): Promise<RepositoryAnalysisResponse | null> => {
    setIsLoading(true);
    setError(null);

    try {
      const response = await RepositoryAnalysisService.startAnalysis({
        async_processing: true,
      });

      // Immediately add the new job to global state for instant UI feedback
      const newJob: RepositoryAnalysisJob = {
        job_id: response.job_id,
        status: response.status,
        message: response.message,
        output_file: response.output_file,
        started_at: response.started_at,
      };

      const updatedJobs = [newJob, ...globalJobs];
      notifyJobsListeners(updatedJobs);
      
      // Force a refresh after a short delay to ensure consistency
      setTimeout(() => {
        refreshJobs();
      }, 1000);
      
      return response;

    } catch (err: any) {
      const errorMessage = err.response?.data?.detail || err.message || 'Failed to start analysis';
      setError(errorMessage);
      console.error('Failed to start repository analysis:', err);
      return null;

    } finally {
      setIsLoading(false);
    }
  }, [refreshJobs]);

  const getJobStatus = useCallback(async (jobId: string): Promise<RepositoryAnalysisJob | null> => {
    try {
      const job = await RepositoryAnalysisService.getJobStatus(jobId);
      
      // Update the job in global state
      const updatedJobs = globalJobs.map(prevJob => 
        prevJob.job_id === jobId ? job : prevJob
      );
      notifyJobsListeners(updatedJobs);

      return job;

    } catch (err: any) {
      console.error(`Failed to get job status for ${jobId}:`, err);
      return null;
    }
  }, []);

  const cancelJob = useCallback(async (jobId: string): Promise<boolean> => {
    try {
      await RepositoryAnalysisService.cancelJob(jobId);
      
      // Update the job status in global state
      const updatedJobs = globalJobs.map(job => 
        job.job_id === jobId 
          ? { ...job, status: AnalysisStatus.CANCELLED, message: 'Job cancelled' }
          : job
      );
      notifyJobsListeners(updatedJobs);

      return true;

    } catch (err: any) {
      const errorMessage = err.response?.data?.detail || err.message || 'Failed to cancel job';
      setError(errorMessage);
      console.error(`Failed to cancel job ${jobId}:`, err);
      return false;
    }
  }, []);

  const addJobToState = useCallback((job: RepositoryAnalysisJob) => {
    // Check if job already exists to avoid duplicates
    const existingJobIndex = globalJobs.findIndex(existingJob => existingJob.job_id === job.job_id);
    let updatedJobs: RepositoryAnalysisJob[];
    
    if (existingJobIndex >= 0) {
      updatedJobs = [...globalJobs];
      updatedJobs[existingJobIndex] = job;
    } else {
      updatedJobs = [job, ...globalJobs];
    }
    
    notifyJobsListeners(updatedJobs);
  }, []);

  return {
    jobs,
    isLoading,
    error,
    hasRunningJob,
    startAnalysis,
    refreshJobs,
    getJobStatus,
    cancelJob,
    addJobToState,
  };
};

export default useRepositoryAnalysis;