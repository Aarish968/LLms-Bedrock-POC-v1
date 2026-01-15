/**
 * Custom hook for Prefect Analysis
 * Implements smart polling strategy to minimize backend load
 */

import { useState, useEffect, useCallback, useRef } from 'react';
import { PrefectAnalysisService } from '../api/prefectAnalysisService';
import {
  PrefectAnalysisJob,
  PrefectAnalysisStatus,
  PrefectAnalysisRequest,
  PrefectAnalysisResponse,
  getPollingInterval,
} from '../types/prefectAnalysis';

// Global state to share between hook instances
let globalJobs: PrefectAnalysisJob[] = [];
let globalJobsListeners: Set<(jobs: PrefectAnalysisJob[]) => void> = new Set();

const notifyJobsListeners = (jobs: PrefectAnalysisJob[]) => {
  globalJobs = jobs;
  globalJobsListeners.forEach(listener => listener(jobs));
};

export const usePrefectAnalysis = () => {
  const [jobs, setJobs] = useState<PrefectAnalysisJob[]>(globalJobs);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasRunningJob, setHasRunningJob] = useState(false);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  
  // Track if tab is visible using Page Visibility API
  const [isTabVisible, setIsTabVisible] = useState(!document.hidden);
  const pollingIntervalRef = useRef<NodeJS.Timeout | null>(null);

  // Subscribe to global jobs changes
  useEffect(() => {
    const listener = (newJobs: PrefectAnalysisJob[]) => {
      setJobs(newJobs);
    };
    
    globalJobsListeners.add(listener);
    
    return () => {
      globalJobsListeners.delete(listener);
    };
  }, []);

  // Handle visibility change
  useEffect(() => {
    const handleVisibilityChange = () => {
      setIsTabVisible(!document.hidden);
    };

    document.addEventListener('visibilitychange', handleVisibilityChange);
    return () => {
      document.removeEventListener('visibilitychange', handleVisibilityChange);
    };
  }, []);

  // Fetch jobs
  const fetchJobs = useCallback(async (showLoading = true) => {
    try {
      if (showLoading) {
        setLoading(true);
      }
      
      const fetchedJobs = await PrefectAnalysisService.listJobs(50, 0);
      notifyJobsListeners(fetchedJobs);
      setLastUpdated(new Date());
      
      // Check if any job is running
      const running = fetchedJobs.some(
        job => job.status === PrefectAnalysisStatus.PENDING ||
               job.status === PrefectAnalysisStatus.CLONING ||
               job.status === PrefectAnalysisStatus.ANALYZING
      );
      setHasRunningJob(running);
      
      setError(null);
    } catch (err: any) {
      setError(err.message || 'Failed to fetch jobs');
      console.error('Error fetching Prefect analysis jobs:', err);
    } finally {
      if (showLoading) {
        setLoading(false);
      }
    }
  }, []);

  // Smart polling with adaptive intervals
  useEffect(() => {
    // Clear any existing interval
    if (pollingIntervalRef.current) {
      clearInterval(pollingIntervalRef.current);
      pollingIntervalRef.current = null;
    }

    // Only poll if tab is visible and there are running jobs
    if (!isTabVisible || !hasRunningJob) {
      return;
    }

    // Find the running job with the most appropriate polling interval
    const runningJobs = jobs.filter(
      job => job.status === PrefectAnalysisStatus.PENDING ||
             job.status === PrefectAnalysisStatus.CLONING ||
             job.status === PrefectAnalysisStatus.ANALYZING
    );

    if (runningJobs.length === 0) {
      return;
    }

    // Get the shortest polling interval from all running jobs
    const intervals = runningJobs
      .map(job => getPollingInterval(job.status))
      .filter((interval): interval is number => interval !== null);

    if (intervals.length === 0) {
      return;
    }

    const shortestInterval = Math.min(...intervals);

    // Set up polling with adaptive interval
    console.log(`Setting up Prefect analysis polling with ${shortestInterval}ms interval`);
    
    pollingIntervalRef.current = setInterval(() => {
      console.log('Polling Prefect analysis jobs...');
      fetchJobs(false); // Don't show loading spinner on background polls
    }, shortestInterval);

    // Cleanup
    return () => {
      if (pollingIntervalRef.current) {
        clearInterval(pollingIntervalRef.current);
        pollingIntervalRef.current = null;
      }
    };
  }, [isTabVisible, hasRunningJob, jobs, fetchJobs]);

  // Initial fetch
  useEffect(() => {
    fetchJobs();
  }, [fetchJobs]);

  // Start analysis
  const startAnalysis = useCallback(async (request?: PrefectAnalysisRequest): Promise<PrefectAnalysisResponse | null> => {
    setLoading(true);
    setError(null);

    try {
      const defaultRequest: PrefectAnalysisRequest = {
        sf_environment: 'prod',
        max_workers: 4,
        target_directory: 'prefect_repos',
        skip_discovery: false,
        skip_naming_check: false,
        clone_all_repos: false,
        async_processing: true,
      };

      const response = await PrefectAnalysisService.startAnalysis(request || defaultRequest);

      // Immediately add the new job to global state for instant UI feedback
      const newJob: PrefectAnalysisJob = {
        job_id: response.job_id,
        status: response.status,
        message: response.message,
        started_at: response.started_at,
        sf_environment: defaultRequest.sf_environment || 'prod',
        max_workers: defaultRequest.max_workers || 4,
        target_directory: defaultRequest.target_directory || 'prefect_repos',
        total_repos_found: 0,
        repos_cloned: 0,
        total_references: 0,
        unique_tables: 0,
        unique_repos: 0,
        request_params: request || defaultRequest,
      };

      const updatedJobs = [newJob, ...globalJobs];
      notifyJobsListeners(updatedJobs);
      setHasRunningJob(true);
      
      // Force a refresh after a short delay to ensure consistency
      setTimeout(() => {
        fetchJobs(false);
      }, 1000);
      
      return response;

    } catch (err: any) {
      const errorMessage = err.response?.data?.detail || err.message || 'Failed to start analysis';
      setError(errorMessage);
      console.error('Failed to start Prefect analysis:', err);
      return null;

    } finally {
      setLoading(false);
    }
  }, [fetchJobs]);

  // Add job to state (when new job is created)
  const addJobToState = useCallback((job: PrefectAnalysisJob) => {
    // Check if job already exists to avoid duplicates
    const existingJobIndex = globalJobs.findIndex(existingJob => existingJob.job_id === job.job_id);
    let updatedJobs: PrefectAnalysisJob[];
    
    if (existingJobIndex >= 0) {
      updatedJobs = [...globalJobs];
      updatedJobs[existingJobIndex] = job;
    } else {
      updatedJobs = [job, ...globalJobs];
    }
    
    notifyJobsListeners(updatedJobs);
    setHasRunningJob(true);
  }, []);

  // Cancel job
  const cancelJob = useCallback(async (jobId: string) => {
    try {
      await PrefectAnalysisService.cancelJob(jobId);
      await fetchJobs();
    } catch (err: any) {
      setError(err.message || 'Failed to cancel job');
      throw err;
    }
  }, [fetchJobs]);

  // Manual refresh
  const refresh = useCallback(() => {
    fetchJobs(true);
  }, [fetchJobs]);

  return {
    jobs,
    loading,
    error,
    hasRunningJob,
    lastUpdated,
    isTabVisible,
    fetchJobs,
    startAnalysis,
    addJobToState,
    cancelJob,
    refresh,
  };
};

export default usePrefectAnalysis;
