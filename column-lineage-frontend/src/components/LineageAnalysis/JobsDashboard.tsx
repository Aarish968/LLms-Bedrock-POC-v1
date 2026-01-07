import React, { useState } from 'react';
import {
  Box,
  Typography,
  Button,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Alert,
  Tabs,
  Tab,
} from '@mui/material';
import { Add, Refresh, Analytics, Code } from '@mui/icons-material';
import { useLineageJobs, useJobStatus, useLineageResults } from '@/hooks/lineage/useLineageAnalysis';
import JobStatusCard from './JobStatusCard';
import ResultsViewer from './ResultsViewer';
import { RepositoryAnalysisJobs } from '../RepositoryAnalysis';

interface JobsDashboardProps {
  onNewAnalysis: () => void;
  onNewRepoAnalysis?: () => void;
}

const JobsDashboard: React.FC<JobsDashboardProps> = ({ 
  onNewAnalysis, 
  onNewRepoAnalysis 
}) => {
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null);
  const [showResults, setShowResults] = useState(false);
  const [currentTab, setCurrentTab] = useState(0);

  const { data: jobs, isLoading: jobsLoading, refetch: refetchJobs } = useLineageJobs();
  const { data: selectedJobStatus, refetch: refetchJobStatus } = useJobStatus(
    selectedJobId, 
    !!selectedJobId
  );
  const { data: resultsData, isLoading: resultsLoading } = useLineageResults(
    selectedJobId,
    showResults
  );

  const handleViewResults = (jobId: string) => {
    setSelectedJobId(jobId);
    setShowResults(true);
  };

  const handleRefreshJob = (jobId: string) => {
    setSelectedJobId(jobId);
    refetchJobStatus();
  };

  const handleCloseResults = () => {
    setShowResults(false);
    setSelectedJobId(null);
  };

  const handleRefreshAll = () => {
    refetchJobs();
  };

  const handleTabChange = (_event: React.SyntheticEvent, newValue: number) => {
    setCurrentTab(newValue);
  };

  // Check if there's any running job to disable new analysis
  const hasRunningJob = jobs?.some(job => 
    job.status === 'PENDING' || job.status === 'RUNNING'
  );

  const handleNewAnalysis = () => {
    if (hasRunningJob) {
      // Show warning but still allow opening dialog
      console.warn('There is already a running job');
    }
    onNewAnalysis();
  };

  return (
    <Box>
      {/* Jobs Tabs */}
      <Box sx={{ borderBottom: 1, borderColor: 'divider', mb: 3 }}>
        <Tabs value={currentTab} onChange={handleTabChange}>
          <Tab 
            icon={<Analytics />} 
            label="Column Lineage Jobs" 
            iconPosition="start"
          />
          <Tab 
            icon={<Code />} 
            label="Repository Analysis Jobs" 
            iconPosition="start"
          />
        </Tabs>
      </Box>

      {/* Column Lineage Jobs Tab */}
      {currentTab === 0 && (
        <Box>
          <Box display="flex" alignItems="center" justifyContent="between" sx={{ mb: 3 }}>
            <Typography variant="h6" component="h2">
              Column Lineage Analysis
            </Typography>
            <Box display="flex" gap={1}>
              <Button
                variant="outlined"
                startIcon={<Refresh />}
                onClick={handleRefreshAll}
              >
                Refresh
              </Button>
              <Button
                variant="contained"
                startIcon={<Add />}
                onClick={handleNewAnalysis}
                disabled={hasRunningJob}
                title={hasRunningJob ? 'Please wait for current analysis to complete' : 'Start new analysis'}
              >
                New Analysis
              </Button>
            </Box>
          </Box>

          {hasRunningJob && (
            <Alert severity="info" sx={{ mb: 2 }}>
              Analysis in progress. New analysis will be available once current job completes.
            </Alert>
          )}

          {jobsLoading ? (
            <Box display="flex" justifyContent="center" p={3}>
              <Typography>Loading jobs...</Typography>
            </Box>
          ) : !jobs || jobs.length === 0 ? (
            <Alert severity="info">
              No analysis jobs found. Start your first analysis!
            </Alert>
          ) : (
            <Box>
              {jobs.map((job) => (
                <JobStatusCard
                  key={job.job_id}
                  jobId={job.job_id}
                  status={job.status}
                  totalViews={job.total_views}
                  processedViews={job.processed_views}
                  resultsCount={job.results_count}
                  createdAt={job.created_at}
                  startedAt={job.started_at}
                  completedAt={job.completed_at}
                  successfulViews={job.successful_views}
                  failedViews={job.failed_views}
                  requestParams={job.request_params}
                  errorMessage={job.error_message}
                  onViewResults={() => handleViewResults(job.job_id)}
                  onRefresh={() => handleRefreshJob(job.job_id)}
                />
              ))}
            </Box>
          )}
        </Box>
      )}

      {/* Repository Analysis Jobs Tab */}
      {currentTab === 1 && (
        <RepositoryAnalysisJobs onNewAnalysis={onNewRepoAnalysis} />
      )}

      {/* Results Dialog */}
      <Dialog
        open={showResults}
        onClose={handleCloseResults}
        maxWidth="lg"
        fullWidth
      >
        <DialogTitle>
          Analysis Results - Job {selectedJobId?.slice(0, 8)}...
        </DialogTitle>
        <DialogContent>
          {resultsLoading ? (
            <Box display="flex" justifyContent="center" p={3}>
              <Typography>Loading results...</Typography>
            </Box>
          ) : resultsData ? (
            <ResultsViewer
              results={resultsData.results}
              summary={resultsData.summary}
              totalResults={resultsData.total_results}
            />
          ) : (
            <Alert severity="error">
              Failed to load results
            </Alert>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={handleCloseResults}>
            Close
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default JobsDashboard;