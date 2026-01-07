import React, { useState, useEffect } from 'react';
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
  CheckCircle,
  Error as ErrorIcon,
  Schedule,
  Analytics,
  Warning,
} from '@mui/icons-material';
import { useLineageJobs, useJobStatus, useLineageResults } from '@/hooks/lineage/useLineageAnalysis';
import ResultsViewer from './ResultsViewer';

interface JobsDashboardProps {
  onNewAnalysis: () => void;
}

const JobsDashboard: React.FC<JobsDashboardProps> = ({ onNewAnalysis }) => {
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null);
  const [showResults, setShowResults] = useState(false);

  const { data: jobs, isLoading: jobsLoading, refetch: refetchJobs } = useLineageJobs();
  const { data: selectedJobStatus, refetch: refetchJobStatus } = useJobStatus(
    selectedJobId, 
    !!selectedJobId
  );
  const { data: resultsData, isLoading: resultsLoading } = useLineageResults(
    selectedJobId,
    showResults
  );

  // Auto-refresh jobs every 10 seconds
  useEffect(() => {
    refetchJobs();
    const interval = setInterval(refetchJobs, 10000);
    return () => clearInterval(interval);
  }, [refetchJobs]);

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
        return <CircularProgress size={16} sx={{ color: 'inherit' }} />;
      case 'RUNNING':
        return <CircularProgress size={16} sx={{ color: 'inherit' }} />;
      case 'COMPLETED':
        return <CheckCircle />;
      case 'FAILED':
        return <Warning />;
      case 'CANCELLED':
        return <Cancel />;
      default:
        return undefined;
    }
  };

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

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleString();
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
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Analytics color="primary" />
          <Typography variant="h6">
            Column Lineage Jobs
          </Typography>
        </Box>
        <Box sx={{ display: 'flex', gap: 1 }}>
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

      {!jobs || jobs.length === 0 ? (
        <Card>
          <CardContent sx={{ textAlign: 'center', py: 4 }}>
            <Analytics sx={{ fontSize: 48, color: 'text.secondary', mb: 2 }} />
            <Typography variant="h6" color="text.secondary" gutterBottom>
              No Column Lineage Jobs
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
              Start your first column lineage analysis to see jobs here.
            </Typography>
            <Button
              variant="contained"
              startIcon={<Add />}
              onClick={handleNewAnalysis}
              disabled={hasRunningJob}
              title={hasRunningJob ? 'Please wait for current analysis to complete' : 'Start new analysis'}
            >
              Start Analysis
            </Button>
          </CardContent>
        </Card>
      ) : (
        <Box>
          {hasRunningJob && (
            <Alert severity="info" sx={{ mb: 2 }}>
              Column lineage analysis in progress. New analysis will be available once current job completes.
            </Alert>
          )}
          
          <TableContainer component={Paper}>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>Job ID</TableCell>
                  <TableCell>Status</TableCell>
                  <TableCell>Started</TableCell>
                  <TableCell>Completed</TableCell>
                  <TableCell>Progress</TableCell>
                  <TableCell>Message</TableCell>
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
                        {job.created_at ? formatDate(job.created_at) : '-'}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2">
                        {job.completed_at ? formatDate(job.completed_at) : '-'}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2">
                        {job.total_views > 0 
                          ? `${job.processed_views} / ${job.total_views} views`
                          : '-'
                        }
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2" sx={{ maxWidth: 200 }}>
                        {job.status === 'COMPLETED' 
                          ? `Analysis completed! Found ${job.results_count} lineage relationships.`
                          : job.error_message || 'Processing...'
                        }
                      </Typography>
                    </TableCell>
                    <TableCell align="right">
                      <Box sx={{ display: 'flex', gap: 0.5 }}>
                        {job.status === 'COMPLETED' && (
                          <Tooltip title="View Results">
                            <IconButton
                              size="small"
                              onClick={() => handleViewResults(job.job_id)}
                            >
                              <Visibility />
                            </IconButton>
                          </Tooltip>
                        )}
                        
                        {(job.status === 'PENDING' || job.status === 'RUNNING') && (
                          <Tooltip title="Refresh Status">
                            <IconButton
                              size="small"
                              onClick={() => handleRefreshJob(job.job_id)}
                            >
                              <Refresh />
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

      {/* Results Dialog */}
      <Dialog
        open={showResults}
        onClose={handleCloseResults}
        maxWidth="lg"
        fullWidth
      >
        <DialogTitle>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Analytics color="primary" />
            <Typography variant="h6">
              Analysis Results - Job {selectedJobId?.slice(0, 8)}...
            </Typography>
          </Box>
        </DialogTitle>
        <DialogContent>
          {resultsLoading ? (
            <Box display="flex" justifyContent="center" p={3}>
              <CircularProgress />
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