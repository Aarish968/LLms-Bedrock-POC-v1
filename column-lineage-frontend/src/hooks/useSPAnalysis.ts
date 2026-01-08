/**
 * Stored Procedure Analysis Hook
 * Custom hook for managing stored procedure analysis state and operations
 */

import { useState, useCallback, useEffect } from 'react';
import { SPAnalysisService } from '../api/spAnalysisService';
import {
  SPAnalysisJob,
  SPAnalysisResponse,
  SPJobStatus,
  SPAnalysisRequest,
} from '../types/spAnalysis';

// Global state to share between hook instances
let globalJobs: SPAnalysisJob[] = [];
let globalJobsListeners: Set<(jobs: SPAnalysisJob[]) => void> = new Set();

const notifyJobsListeners = (jobs: SPAnalysisJob[]) => {
  globalJobs = jobs;
  globalJobsListeners.forEach(listener => listener(jobs));
};

interface UseSPAnalysisReturn {
  jobs: SPAnalysisJob[];
  isLoading: boolean;
  error: string | null;
  hasRunningJob: boolean;
  startAnalysis: (request?: SPAnalysisRequest) => Promise<SPAnalysisResponse | null>;
  refreshJobs: () => Promise<void>;
  getJobStatus: (jobId: string) => Promise<SPAnalysisJob | null>;
  cancelJob: (jobId: string) => Promise<boolean>;
  addJobToState: (job: SPAnalysisJob) => void;
}

export const useSPAnalysis = (): UseSPAnalysisReturn => {
  const [jobs, setJobs] = useState<SPAnalysisJob[]>(globalJobs);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Subscribe to global jobs changes
  useEffect(() => {
    const listener = (newJobs: SPAnalysisJob[]) => {
      setJobs(newJobs);
    };
    
    globalJobsListeners.add(listener);
    
    return () => {
      globalJobsListeners.delete(listener);
    };
  }, []);

  // Check if there's any running job
  const hasRunningJob = jobs.some(job => 
    job.status === SPJobStatus.PENDING || 
    job.status === SPJobStatus.RUNNING
  );

  // Define refreshJobs first
  const refreshJobs = useCallback(async (): Promise<void> => {
    setIsLoading(true);
    setError(null);

    try {
      const jobsList = await SPAnalysisService.listJobs(50, 0);
      notifyJobsListeners(jobsList);

    } catch (err: any) {
      const errorMessage = err.response?.data?.detail || err.message || 'Failed to fetch SP analysis jobs';
      setError(errorMessage);
      console.error('Failed to fetch SP analysis jobs:', err);

    } finally {
      setIsLoading(false);
    }
  }, []);

  const startAnalysis = useCallback(async (request?: SPAnalysisRequest): Promise<SPAnalysisResponse | null> => {
    setIsLoading(true);
    setError(null);

    try {
      const analysisRequest: SPAnalysisRequest = {
        sf_environment: 'prod',
        max_workers: 4,
        resume_from_partial: true,
        ...request,
      };

      const response = await SPAnalysisService.startAnalysis(analysisRequest);

      // Immediately add the new job to global state for instant UI feedback
      const newJob: SPAnalysisJob = {
        job_id: response.job_id,
        status: response.status,
        sf_environment: analysisRequest.sf_environment || 'prod',
        max_workers: analysisRequest.max_workers || 4,
        total_procedures: 0,
        completed_procedures: 0,
        failed_procedures: 0,
        started_at: response.started_at,
        request_params: analysisRequest,
      };

      const updatedJobs = [newJob, ...globalJobs];
      notifyJobsListeners(updatedJobs);
      
      // Force a refresh after a short delay to ensure consistency
      setTimeout(() => {
        refreshJobs();
      }, 1000);
      
      return response;

    } catch (err: any) {
      const errorMessage = err.response?.data?.detail || err.message || 'Failed to start SP analysis';
      setError(errorMessage);
      console.error('Failed to start SP analysis:', err);
      return null;

    } finally {
      setIsLoading(false);
    }
  }, [refreshJobs]);

  const getJobStatus = useCallback(async (jobId: string): Promise<SPAnalysisJob | null> => {
    try {
      const job = await SPAnalysisService.getJobStatus(jobId);
      
      // Update the job in global state
      const updatedJobs = globalJobs.map(prevJob => 
        prevJob.job_id === jobId ? job : prevJob
      );
      notifyJobsListeners(updatedJobs);

      return job;

    } catch (err: any) {
      console.error(`Failed to get SP job status for ${jobId}:`, err);
      return null;
    }
  }, []);

  const cancelJob = useCallback(async (jobId: string): Promise<boolean> => {
    try {
      await SPAnalysisService.cancelJob(jobId);
      
      // Update the job status in global state
      const updatedJobs = globalJobs.map(job => 
        job.job_id === jobId 
          ? { ...job, status: SPJobStatus.CANCELLED, error_message: 'Job cancelled' }
          : job
      );
      notifyJobsListeners(updatedJobs);

      return true;

    } catch (err: any) {
      const errorMessage = err.response?.data?.detail || err.message || 'Failed to cancel SP analysis job';
      setError(errorMessage);
      console.error(`Failed to cancel SP job ${jobId}:`, err);
      return false;
    }
  }, []);

  const addJobToState = useCallback((job: SPAnalysisJob) => {
    // Check if job already exists to avoid duplicates
    const existingJobIndex = globalJobs.findIndex(existingJob => existingJob.job_id === job.job_id);
    let updatedJobs: SPAnalysisJob[];
    
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

export default useSPAnalysis;