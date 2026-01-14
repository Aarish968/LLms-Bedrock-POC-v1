/**
 * Custom hook for Prefect Analysis
 * Implements smart polling strategy to minimize backend load
 */

import { useState, useEffect, useCallback, useRef } from 'react';
import { PrefectAnalysisService } from '../api/prefectAnalysisService';
import {
  PrefectAnalysisJob,
  PrefectAnalysisStatus,
  getPollingInterval,
} from '../types/prefectAnalysis';

export const usePrefectAnalysis = () => {
  const [jobs, setJobs] = useState<PrefectAnalysisJob[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasRunningJob, setHasRunningJob] = useState(false);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  
  // Track if tab is visible using Page Visibility API
  const [isTabVisible, setIsTabVisible] = useState(!document.hidden);
  const pollingIntervalRef = useRef<NodeJS.Timeout | null>(null);

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
      setJobs(fetchedJobs);
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

  // Add job to state (when new job is created)
  const addJobToState = useCallback((job: PrefectAnalysisJob) => {
    setJobs(prevJobs => [job, ...prevJobs]);
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
    addJobToState,
    cancelJob,
    refresh,
  };
};

export default usePrefectAnalysis;
