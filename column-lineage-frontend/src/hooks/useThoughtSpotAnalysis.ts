/**
 * Custom hook for ThoughtSpot Analysis
 * Implements smart polling strategy to minimize backend load
 */

import { useState, useEffect, useCallback, useRef } from 'react';
import { ThoughtSpotAnalysisService } from '../api/thoughtspotAnalysisService';
import {
  TSAnalysisJob,
  TSAnalysisStatus,
  TSAnalysisRequest,
  TSAnalysisResponse,
  getPollingInterval,
} from '../types/thoughtspotAnalysis';

// Global state to share between hook instances
let globalJobs: TSAnalysisJob[] = [];
let globalJobsListeners: Set<(jobs: TSAnalysisJob[]) => void> = new Set();

const notifyJobsListeners = (jobs: TSAnalysisJob[]) => {
  globalJobs = jobs;
  globalJobsListeners.forEach(listener => listener(jobs));
};

export const useThoughtSpotAnalysis = () => {
  const [jobs, setJobs] = useState<TSAnalysisJob[]>(globalJobs);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasRunningJob, setHasRunningJob] = useState(false);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  
  // Track if tab is visible using Page Visibility API
  const [isTabVisible, setIsTabVisible] = useState(!document.hidden);
  const pollingIntervalRef = useRef<NodeJS.Timeout | null>(null);

  // Subscribe to global jobs changes
  useEffect(() => {
    const listener = (newJobs: TSAnalysisJob[]) => {
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
      
      const fetchedJobs = await ThoughtSpotAnalysisService.listJobs(50, 0);
      notifyJobsListeners(fetchedJobs);
      setLastUpdated(new Date());
      
      // Check if any job is running
      const running = fetchedJobs.some(
        job => job.status === TSAnalysisStatus.PENDING ||
               job.status === TSAnalysisStatus.RUNNING
      );
      setHasRunningJob(running);
      
      setError(null);
    } catch (err: any) {
      setError(err.message || 'Failed to fetch jobs');
      console.error('Error fetching ThoughtSpot analysis jobs:', err);
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
      job => job.status === TSAnalysisStatus.PENDING ||
             job.status === TSAnalysisStatus.RUNNING
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
    console.log(`Setting up ThoughtSpot analysis polling with ${shortestInterval}ms interval`);
    
    pollingIntervalRef.current = setInterval(() => {
      console.log('Polling ThoughtSpot analysis jobs...');
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
  const startAnalysis = useCallback(async (request?: TSAnalysisRequest): Promise<TSAnalysisResponse | null> => {
    setLoading(true);
    setError(null);

    try {
      const defaultRequest: TSAnalysisRequest = {
        sf_environment: 'prod',
        max_workers: 5,
        include_views: true,
        force_prod_urls: true,
        table_pattern: null,
      };

      const response = await ThoughtSpotAnalysisService.startAnalysis(request || defaultRequest);

      // Immediately add the new job to global state for instant UI feedback
      const newJob: TSAnalysisJob = {
        job_id: response.job_id,
        status: response.status,
        message: response.message,
        started_at: response.started_at,
        sf_environment: defaultRequest.sf_environment || 'prod',
        max_workers: defaultRequest.max_workers || 5,
        include_views: defaultRequest.include_views !== undefined ? defaultRequest.include_views : true,
        force_prod_urls: defaultRequest.force_prod_urls !== undefined ? defaultRequest.force_prod_urls : true,
        table_pattern: defaultRequest.table_pattern || null,
        total_tables: 0,
        processed_tables: 0,
        total_relationships: 0,
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
      console.error('Failed to start ThoughtSpot analysis:', err);
      return null;

    } finally {
      setLoading(false);
    }
  }, [fetchJobs]);

  // Add job to state (when new job is created)
  const addJobToState = useCallback((job: TSAnalysisJob) => {
    // Check if job already exists to avoid duplicates
    const existingJobIndex = globalJobs.findIndex(existingJob => existingJob.job_id === job.job_id);
    let updatedJobs: TSAnalysisJob[];
    
    if (existingJobIndex >= 0) {
      updatedJobs = [...globalJobs];
      updatedJobs[existingJobIndex] = job;
    } else {
      updatedJobs = [job, ...globalJobs];
    }
    
    notifyJobsListeners(updatedJobs);
    setHasRunningJob(true);
  }, []);

  // Delete job
  const deleteJob = useCallback(async (jobId: string) => {
    try {
      await ThoughtSpotAnalysisService.deleteJob(jobId);
      await fetchJobs();
    } catch (err: any) {
      setError(err.message || 'Failed to delete job');
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
    deleteJob,
    refresh,
  };
};

export default useThoughtSpotAnalysis;
