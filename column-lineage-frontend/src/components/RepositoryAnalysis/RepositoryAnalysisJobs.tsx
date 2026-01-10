/**
 * Repository Analysis Jobs Component
 * Displays and manages repository analysis jobs
 */

import React, { useState, useEffect } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Chip,
  Button,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  IconButton,
  Tooltip,
  CircularProgress,
  Alert,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
} from '@mui/material';
import {
  Refresh,
  Cancel,
  Visibility,
  Code,
  CheckCircle,
  Error as ErrorIcon,
  Schedule,
  PlayArrow,
} from '@mui/icons-material';

import { useRepositoryAnalysis } from '../../hooks/useRepositoryAnalysis';
import {
  RepositoryAnalysisJob,
  AnalysisStatus,
  RepositoryAnalysisResults,
} from '../../types/repositoryAnalysis';
import { RepositoryAnalysisService } from '../../api/repositoryAnalysisService';

interface RepositoryAnalysisJobsProps {
  onNewAnalysis?: () => void;
}

const RepositoryAnalysisJobs: React.FC<RepositoryAnalysisJobsProps> = ({ onNewAnalysis }) => {
  const { jobs, isLoading, error, hasRunningJob, refreshJobs, cancelJob } = useRepositoryAnalysis();
  const [jobResults, setJobResults] = useState<RepositoryAnalysisResults | null>(null);
  const [resultsDialogOpen, setResultsDialogOpen] = useState(false);
  const [loadingResults, setLoadingResults] = useState(false);

  // Smart polling - only poll when there are running jobs
  useEffect(() => {
    refreshJobs(); // Initial load
    
    let interval: NodeJS.Timeout | null = null;
    
    // Only start polling if there are running jobs
    if (hasRunningJob) {
      interval = setInterval(() => {
        refreshJobs();
      }, 5000);
    }
    
    return () => {
      if (interval) {
        clearInterval(interval);
      }
    };
  }, [refreshJobs, hasRunningJob]); // Re-run when hasRunningJob changes

  const getStatusColor = (status: AnalysisStatus) => {
    switch (status) {
      case AnalysisStatus.PENDING:
        return 'warning';
      case AnalysisStatus.CLONING:
      case AnalysisStatus.RUNNING:
        return 'info';
      case AnalysisStatus.COMPLETED:
        return 'success';
      case AnalysisStatus.FAILED:
        return 'error';
      case AnalysisStatus.CANCELLED:
        return 'default';
      default:
        return 'default';
    }
  };

  const getStatusIcon = (status: AnalysisStatus) => {
    switch (status) {
      case AnalysisStatus.PENDING:
        return <Schedule />;
      case AnalysisStatus.CLONING:
      case AnalysisStatus.RUNNING:
        return <CircularProgress size={16} />;
      case AnalysisStatus.COMPLETED:
        return <CheckCircle />;
      case AnalysisStatus.FAILED:
        return <ErrorIcon />;
      case AnalysisStatus.CANCELLED:
        return <Cancel />;
      default:
        return undefined;
    }
  };

  const handleCancelJob = async (jobId: string) => {
    const success = await cancelJob(jobId);
    if (success) {
      await refreshJobs();
    }
  };

  const handleViewResults = async (job: RepositoryAnalysisJob) => {
    if (job.status !== AnalysisStatus.COMPLETED) {
      return;
    }

    setLoadingResults(true);
    setResultsDialogOpen(true);

    try {
      const results = await RepositoryAnalysisService.getResults(job.job_id);
      setJobResults(results);
    } catch (err: any) {
      setJobResults(null);
    } finally {
      setLoadingResults(false);
    }
  };

  const handleCloseResultsDialog = () => {
    setResultsDialogOpen(false);
    setJobResults(null);
  };

  const handleManualRefresh = () => {
    refreshJobs();
  };

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleString();
  };

  const formatFileSize = (bytes: number) => {
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    if (bytes === 0) return '0 Bytes';
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return Math.round(bytes / Math.pow(1024, i) * 100) / 100 + ' ' + sizes[i];
  };

  if (error) {
    return (
      <Alert severity="error" sx={{ mb: 2 }}>
        {error}
      </Alert>
    );
  }

  return (
    <Box>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Code color="primary" />
          <Typography variant="h6">
            Repository Analysis Jobs
          </Typography>
        </Box>
        <Box sx={{ display: 'flex', gap: 1 }}>
          <Button
            variant="outlined"
            startIcon={<Refresh />}
            onClick={handleManualRefresh}
            disabled={isLoading}
          >
            Refresh
          </Button>
          {onNewAnalysis && (
            <Button
              variant="contained"
              startIcon={<PlayArrow />}
              onClick={onNewAnalysis}
            >
              New Analysis
            </Button>
          )}
        </Box>
      </Box>

      {jobs.length === 0 ? (
        <Card>
          <CardContent sx={{ textAlign: 'center', py: 4 }}>
            <Code sx={{ fontSize: 48, color: 'text.secondary', mb: 2 }} />
            <Typography variant="h6" color="text.secondary" gutterBottom>
              No Repository Analysis Jobs
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
              Use the "Start Action To Endpoint Lineage" button from the Column Lineage tab to start your first analysis.
            </Typography>
            {/* {onNewAnalysis && (
              <Button
                variant="contained"
                startIcon={<PlayArrow />}
                onClick={onNewAnalysis}
              >
                Start Analysis
              </Button>
            )} */}
          </CardContent>
        </Card>
      ) : (
        <Box>
          {hasRunningJob && (
            <Alert severity="info" sx={{ mb: 2 }}>
              Repository analysis in progress. New analysis will be available once current job completes.
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
                        {formatDate(job.started_at)}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2">
                        {job.completed_at ? formatDate(job.completed_at) : '-'}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2" sx={{ maxWidth: 200 }}>
                        {job.error_message || job.message}
                      </Typography>
                    </TableCell>
                    <TableCell align="right">
                      <Box sx={{ display: 'flex', gap: 0.5 }}>
                        {job.status === AnalysisStatus.COMPLETED && (
                          <Tooltip title="View Results">
                            <IconButton
                              size="small"
                              onClick={() => handleViewResults(job)}
                            >
                              <Visibility />
                            </IconButton>
                          </Tooltip>
                        )}
                        
                        {(job.status === AnalysisStatus.PENDING ||
                          job.status === AnalysisStatus.CLONING ||
                          job.status === AnalysisStatus.RUNNING) && (
                          <Tooltip title="Cancel Job">
                            <IconButton
                              size="small"
                              onClick={() => handleCancelJob(job.job_id)}
                              color="error"
                            >
                              <Cancel />
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
        open={resultsDialogOpen}
        onClose={handleCloseResultsDialog}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Code color="primary" />
            <Typography variant="h6">
              Analysis Results
            </Typography>
          </Box>
        </DialogTitle>
        
        <DialogContent>
          {loadingResults ? (
            <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
              <CircularProgress />
            </Box>
          ) : jobResults ? (
            <Box sx={{ py: 1 }}>
              <Typography variant="body1" gutterBottom>
                <strong>Job ID:</strong> {jobResults.job_id}
              </Typography>
              <Typography variant="body1" gutterBottom>
                <strong>Status:</strong>{' '}
                <Chip
                  {...(getStatusColor(jobResults.status) && { color: getStatusColor(jobResults.status) })}
                  label={jobResults.status.toUpperCase()}
                  size="small"
                />
              </Typography>
              <Typography variant="body1" gutterBottom>
                <strong>Output File:</strong> {jobResults.output_file}
              </Typography>
              <Typography variant="body1" gutterBottom>
                <strong>File Size:</strong> {formatFileSize(jobResults.file_size)}
              </Typography>
              <Typography variant="body1" gutterBottom>
                <strong>Created:</strong> {jobResults.created_at}
              </Typography>
              <Typography variant="body1" gutterBottom>
                <strong>Modified:</strong> {jobResults.modified_at}
              </Typography>
              <Typography variant="body1" gutterBottom>
                <strong>Message:</strong> {jobResults.message}
              </Typography>
            </Box>
          ) : (
            <Alert severity="error">
              Failed to load analysis results.
            </Alert>
          )}
        </DialogContent>
        
        <DialogActions>
          <Button onClick={handleCloseResultsDialog}>
            Close
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default RepositoryAnalysisJobs;