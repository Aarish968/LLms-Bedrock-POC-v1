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
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Chip,
  IconButton,
  Tooltip,
  CircularProgress,
  Card,
  CardContent,
} from '@mui/material';
import { 
  Add, 
  Refresh, 
  Visibility, 
  Cancel, 
  Work,
  CheckCircle,
  Error as ErrorIcon,
  Schedule,
  ContentCopy,
} from '@mui/icons-material';
import { useLineageJobs, useJobStatus, useLineageResults } from '@/hooks/lineage/useLineageAnalysis';
import ResultsViewer from './ResultsViewer';

interface JobsDashboardProps {
  onNewAnalysis: () => void;
}

const JobsDashboard: React.FC<JobsDashboardProps> = ({ onNewAnalysis }) => {
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null);
  const [showResults, setShowResults] = useState(false);
  const [showJobData, setShowJobData] = useState(false);
  const [selectedJobForData, setSelectedJobForData] = useState<any>(null);

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

  const handleViewJobData = (job: any) => {
    setSelectedJobForData(job);
    setShowJobData(true);
  };

  const handleRefreshJob = (jobId: string) => {
    setSelectedJobId(jobId);
    refetchJobStatus();
  };

  const handleCloseResults = () => {
    setShowResults(false);
    setSelectedJobId(null);
  };

  const handleCloseJobData = () => {
    setShowJobData(false);
    setSelectedJobForData(null);
  };

  const handleRefreshAll = () => {
    refetchJobs();
  };

  // Check if there's any running job to disable new analysis
  const hasRunningJob = jobs?.some(job => 
    job.status === 'PENDING' || job.status === 'RUNNING'
  );

  const handleNewAnalysis = () => {
    if (hasRunningJob) {
      // Show warning but still allow opening dialog
    }
    onNewAnalysis();
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'PENDING':
        return 'warning';
      case 'RUNNING':
        return 'info';
      case 'COMPLETED':
        return 'success';
      case 'FAILED':
        return 'error';
      case 'CANCELLED':
        return 'default';
      default:
        return 'default';
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'PENDING':
        return <Schedule />;
      case 'RUNNING':
        return <CircularProgress size={16} />;
      case 'COMPLETED':
        return <CheckCircle />;
      case 'FAILED':
        return <ErrorIcon />;
      case 'CANCELLED':
        return <Cancel />;
      default:
        return undefined;
    }
  };

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleString();
  };

  const getJobDataAsJson = (job: any) => {
    // Filter out unwanted fields from request_params
    const filteredRequestParams = { ...job.request_params };
    if (filteredRequestParams) {
      delete filteredRequestParams.view_names;
      delete filteredRequestParams.include_system_views;
      delete filteredRequestParams.max_views;
      delete filteredRequestParams.async_processing;
      delete filteredRequestParams.include_metadata;
    }

    return {
      job_id: job.job_id,
      status: job.status,
      created_at: job.created_at,
      started_at: job.started_at || null,
      completed_at: job.completed_at || null,
      total_views: job.total_views,
      processed_views: job.processed_views,
      successful_views: job.successful_views,
      failed_views: job.failed_views,
      error_message: job.error_message || null,
      results_count: job.results_count,
      request_params: filteredRequestParams || {}
    };
  };

  const copyToClipboard = () => {
    if (selectedJobForData) {
      const jsonData = JSON.stringify(getJobDataAsJson(selectedJobForData), null, 2);
      navigator.clipboard.writeText(jsonData);
    }
  };

  if (jobsLoading) {
    return (
      <Box display="flex" justifyContent="center" p={3}>
        <Typography>Loading jobs...</Typography>
      </Box>
    );
  }

  return (
    <Box>
      <Box display="flex" alignItems="center" justifyContent="space-between" sx={{ mb: 3 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Work color="primary" />
          <Typography variant="h6">
            Column Lineage Jobs
          </Typography>
        </Box>
        <Box display="flex" gap={1}>
          <Button
            variant="outlined"
            startIcon={<Refresh />}
            onClick={handleRefreshAll}
            disabled={jobsLoading}
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

      {!jobs || jobs.length === 0 ? (
        <Card>
          <CardContent sx={{ textAlign: 'center', py: 4 }}>
            <Work sx={{ fontSize: 48, color: 'text.secondary', mb: 2 }} />
            <Typography variant="h6" color="text.secondary" gutterBottom>
              No Column Lineage Jobs
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
              Use the "New Analysis" button to start your first column lineage analysis.
            </Typography>
          </CardContent>
        </Card>
      ) : (
        <Box>
          <TableContainer component={Paper}>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>Job ID</TableCell>
                  <TableCell>Status</TableCell>
                  <TableCell>Started</TableCell>
                  <TableCell>Completed</TableCell>
                  <TableCell>Progress</TableCell>
                  <TableCell>Results</TableCell>
                  <TableCell align="right">Actions</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {jobs.map((job) => (
                  <TableRow key={job.job_id}>
                    <TableCell>
                      <Typography variant="body2" fontFamily="monospace">
                        {job.job_id.slice(0, 8)}...
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Chip
                        {...(getStatusIcon(job.status) && { icon: getStatusIcon(job.status) })}
                        label={job.status.toUpperCase()}
                        color={getStatusColor(job.status)}
                        size="small"
                      />
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2">
                        {formatDate(job.created_at)}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2">
                        {job.completed_at ? formatDate(job.completed_at) : '-'}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2">
                        {job.processed_views} / {job.total_views} views
                      </Typography>
                      {job.total_views > 0 && (
                        <Typography variant="caption" color="text.secondary" display="block">
                          {Math.round((job.processed_views / job.total_views) * 100)}% complete
                        </Typography>
                      )}
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2">
                        {job.results_count || 0} lineages
                      </Typography>
                      {job.status === 'COMPLETED' && (
                        <Typography variant="caption" color="text.secondary" display="block">
                          Success: {job.successful_views || 0} | Failed: {job.failed_views || 0}
                        </Typography>
                      )}
                    </TableCell>
                    <TableCell align="right">
                      <Box sx={{ display: 'flex', gap: 0.5, justifyContent: 'flex-end' }}>
                        {job.status === 'COMPLETED' && (
                          <Tooltip title="View Job Data">
                            <IconButton
                              size="small"
                              onClick={() => handleViewJobData(job)}
                            >
                              <Visibility />
                            </IconButton>
                          </Tooltip>
                        )}
                      </Box>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </Box>
      )}

      {/* Job Data Dialog */}
      <Dialog
        open={showJobData}
        onClose={handleCloseJobData}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>
          Job Data - {selectedJobForData?.job_id.slice(0, 8)}...
        </DialogTitle>
        <DialogContent>
          <Paper
            elevation={0}
            sx={{
              p: 2,
              backgroundColor: 'grey.50',
              border: '1px solid',
              borderColor: 'grey.300',
              borderRadius: 1,
              fontFamily: 'monospace',
              fontSize: '0.875rem',
              overflow: 'auto',
              maxHeight: '400px'
            }}
          >
            <pre style={{ margin: 0, whiteSpace: 'pre-wrap' }}>
              {selectedJobForData ? JSON.stringify(getJobDataAsJson(selectedJobForData), null, 2) : ''}
            </pre>
          </Paper>
        </DialogContent>
        <DialogActions>
          <Button
            startIcon={<ContentCopy />}
            onClick={copyToClipboard}
            variant="outlined"
          >
            Copy JSON
          </Button>
          <Button onClick={handleCloseJobData}>
            Close
          </Button>
        </DialogActions>
      </Dialog>

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
              <CircularProgress />
              <Typography sx={{ ml: 2 }}>Loading results...</Typography>
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