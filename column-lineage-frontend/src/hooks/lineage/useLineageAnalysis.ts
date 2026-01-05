import { useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { lineageService, LineageAnalysisRequest, LineageAnalysisResponse, LineageAnalysisJob } from '@/api/lineageService';

export const useStartLineageAnalysis = () => {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: (request: LineageAnalysisRequest) => lineageService.startAnalysis(request),
    onSuccess: (data: LineageAnalysisResponse) => {
      toast.success(`Analysis started successfully! Job ID: ${data.job_id.slice(0, 8)}...`);
      
      // Immediately add the job to the jobs list cache with PENDING status
      queryClient.setQueryData(['lineage-jobs'], (oldJobs: LineageAnalysisJob[] | undefined) => {
        const newJob: LineageAnalysisJob = {
          job_id: data.job_id,
          status: 'PENDING',
          created_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
          total_views: 0,
          processed_views: 0,
          results_count: 0,
          request_params: {},
        };
        
        return oldJobs ? [newJob, ...oldJobs] : [newJob];
      });
      
      // Invalidate jobs query to trigger refetch
      queryClient.invalidateQueries({ queryKey: ['lineage-jobs'] });
    },
    onError: (error: Error) => {
      toast.error(`Failed to start analysis: ${error.message}`);
    },
  });
};

export const useJobStatus = (jobId: string | null, enabled: boolean = true) => {
  const queryClient = useQueryClient();
  
  return useQuery({
    queryKey: ['lineage-job-status', jobId],
    queryFn: () => {
      if (!jobId) {
        throw new Error('Job ID is required');
      }
      return lineageService.getJobStatus(jobId);
    },
    enabled: enabled && !!jobId && jobId !== 'null',
    refetchInterval: (data) => {
      // Refetch every 3 seconds if job is still running
      if (data?.status === 'PENDING' || data?.status === 'RUNNING') {
        return 3000;
      }
      return false;
    },
    onSuccess: (data) => {
      // Update the jobs list cache when status changes
      queryClient.setQueryData(['lineage-jobs'], (oldJobs: LineageAnalysisJob[] | undefined) => {
        if (!oldJobs) return oldJobs;
        
        return oldJobs.map(job => 
          job.job_id === jobId ? { ...job, ...data } : job
        );
      });
    },
  });
};

export const useLineageResults = (jobId: string | null, enabled: boolean = false) => {
  return useQuery({
    queryKey: ['lineage-results', jobId],
    queryFn: () => {
      if (!jobId) {
        throw new Error('Job ID is required');
      }
      return lineageService.getResults(jobId);
    },
    enabled: enabled && !!jobId && jobId !== 'null',
  });
};

export const useLineageJobs = () => {
  return useQuery({
    queryKey: ['lineage-jobs'],
    queryFn: () => lineageService.listJobs(),
    refetchInterval: 15000, // Refetch every 15 seconds (less frequent since individual jobs update more often)
  });
};

// Custom hook for managing the complete lineage analysis workflow
export const useLineageWorkflow = () => {
  const [currentJobId, setCurrentJobId] = useState<string | null>(null);
  const [showResults, setShowResults] = useState(false);

  const startAnalysisMutation = useStartLineageAnalysis();
  const jobStatusQuery = useJobStatus(currentJobId, !!currentJobId);
  const resultsQuery = useLineageResults(currentJobId, showResults);

  const startAnalysis = async (request: LineageAnalysisRequest) => {
    // Prevent multiple simultaneous analysis
    if (startAnalysisMutation.isPending) {
      toast.warning('Analysis is already starting. Please wait...');
      return;
    }

    // Check if there's already a running job
    if (currentJobId && (jobStatusQuery.data?.status === 'PENDING' || jobStatusQuery.data?.status === 'RUNNING')) {
      toast.warning('Analysis is already in progress. Please wait for it to complete.');
      return;
    }

    try {
      const response = await startAnalysisMutation.mutateAsync({
        ...request,
        async_processing: true, // Always use async processing
      });
      setCurrentJobId(response.job_id);
      setShowResults(false);
    } catch (error) {
      console.error('Failed to start analysis:', error);
    }
  };

  const viewResults = () => {
    if (currentJobId && jobStatusQuery.data?.status === 'COMPLETED') {
      setShowResults(true);
    }
  };

  const resetWorkflow = () => {
    // Only allow reset if no job is currently running
    if (jobStatusQuery.data?.status === 'PENDING' || jobStatusQuery.data?.status === 'RUNNING') {
      toast.warning('Cannot start new analysis while current job is running.');
      return;
    }
    
    setCurrentJobId(null);
    setShowResults(false);
  };

  return {
    // State
    currentJobId,
    showResults,
    
    // Queries
    jobStatus: jobStatusQuery.data,
    jobStatusLoading: jobStatusQuery.isLoading,
    results: resultsQuery.data,
    resultsLoading: resultsQuery.isLoading,
    
    // Actions
    startAnalysis,
    viewResults,
    resetWorkflow,
    
    // Status checks
    isAnalysisRunning: startAnalysisMutation.isPending,
    isJobRunning: jobStatusQuery.data?.status === 'PENDING' || jobStatusQuery.data?.status === 'RUNNING',
    isJobCompleted: jobStatusQuery.data?.status === 'COMPLETED',
    isJobFailed: jobStatusQuery.data?.status === 'FAILED',
    canStartNewAnalysis: !startAnalysisMutation.isPending && 
                        !(jobStatusQuery.data?.status === 'PENDING' || jobStatusQuery.data?.status === 'RUNNING'),
  };
};