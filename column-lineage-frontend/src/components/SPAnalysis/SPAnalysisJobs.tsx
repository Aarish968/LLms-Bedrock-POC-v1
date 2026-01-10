/**
 * Stored Procedure Analysis Jobs Component
 * Displays and manages SP analysis jobs
 */

import React, { useEffect, useState } from 'react';
import {
  Box,
  Typography,
  Card,
  CardContent,
  Button,
  Chip,
  LinearProgress,
  Alert,
  IconButton,
  Tooltip,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  CircularProgress,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
} from '@mui/material';
import {
  Refresh,
  Download,
  Cancel,
  PlayArrow,
  CheckCircle,
  Error as ErrorIcon,
  Storage,
  Visibility,
} from '@mui/icons-material';

import useSPAnalysis from '../../hooks/useSPAnalysis';
import { SPAnalysisService } from '../../api/spAnalysisService';
import {
  SPAnalysisJob,
  SPJobStatus,
  SPResultsResponse,
} from '../../types/spAnalysis';

interface SPAnalysisJobsProps {
  onNewAnalysis?: () => void;
}

const SPAnalysisJobs: React.FC<SPAnalysisJobsProps> = ({ onNewAnalysis }) => {
  const {
    jobs,
    isLoading,
    error,
    refreshJobs,
    cancelJob,
  } = useSPAnalysis();

  const [selectedJob, setSelectedJob] = useState<SPAnalysisJob | null>(null);
  const [jobResults, setJobResults] = useState<SPResultsResponse | null>(null);
  const [resultsDialogOpen, setResultsDialogOpen] = useState(false);
  const [loadingResults, setLoadingResults] = useState(false);

  // Auto-refresh jobs every 5 seconds
  useEffect(() => {
    const interval = setInterval(() => {
      refreshJobs();
    }, 5000);

    return () => clearInterval(interval);
  }, [refreshJobs]);

  // Initial load
  useEffect(() => {
    refreshJobs();
  }, [refreshJobs]);

  const handleCancelJob = async (jobId: string) => {
    const success = await cancelJob(jobId);
    if (success) {
      refreshJobs();
    }
  };

  // const handleDownloadResults = async (jobId: string) => {
  //   try {
  //     await SPAnalysisService.downloadResults(jobId);
  //   } catch (err) {
  //     console.error('Failed to download results:', err);
  //   }
  // };

  const handleViewResults = async (job: SPAnalysisJob) => {
    if (job.status !== SPJobStatus.COMPLETED) return;

    setSelectedJob(job);
    setLoadingResults(true);
    setResultsDialogOpen(true);

    try {
      const results = await SPAnalysisService.getResults(job.job_id);
      setJobResults(results);
    } catch (err) {
      console.error('Failed to load results:', err);
      setJobResults(null);
    } finally {
      setLoadingResults(false);
    }
  };

  const handleCloseResultsDialog = () => {
    setResultsDialogOpen(false);
    setSelectedJob(null);
    setJobResults(null);
  };

  const getStatusIcon = (status: SPJobStatus) => {
    switch (status) {
      case SPJobStatus.PENDING:
      case SPJobStatus.RUNNING:
        return <CircularProgress size={16} />;
      case SPJobStatus.COMPLETED:
        return <CheckCircle color="success" />;
      case SPJobStatus.FAILED:
        return <ErrorIcon color="error" />;
      case SPJobStatus.CANCELLED:
        return <Cancel color="disabled" />;
      default:
        return null;
    }
  };

  const getStatusColor = (status: SPJobStatus) => {
    switch (status) {
      case SPJobStatus.PENDING:
        return 'warning';
      case SPJobStatus.RUNNING:
        return 'info';
      case SPJobStatus.COMPLETED:
        return 'success';
      case SPJobStatus.FAILED:
        return 'error';
      case SPJobStatus.CANCELLED:
        return 'default';
      default:
        return 'default';
    }
  };

  const getProgressValue = (job: SPAnalysisJob) => {
    if (job.total_procedures === 0) return 0;
    return (job.completed_procedures / job.total_procedures) * 100;
  };

  const formatDuration = (startTime: string, endTime?: string) => {
    const start = new Date(startTime);
    const end = endTime ? new Date(endTime) : new Date();
    const duration = Math.floor((end.getTime() - start.getTime()) / 1000);
    
    if (duration < 60) return `${duration}s`;
    if (duration < 3600) return `${Math.floor(duration / 60)}m ${duration % 60}s`;
    return `${Math.floor(duration / 3600)}h ${Math.floor((duration % 3600) / 60)}m`;
  };

  return (
    <Box>
      {/* Header */}
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Storage color="primary" />
          <Typography variant="h6">
            Stored Procedure Analysis Jobs
          </Typography>
        </Box>
        <Box sx={{ display: 'flex', gap: 1 }}>
          <Button
            variant="outlined"
            startIcon={<Refresh />}
            onClick={refreshJobs}
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

      {/* Error Alert */}
      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      {/* Loading State */}
      {isLoading && jobs.length === 0 && (
        <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
          <CircularProgress />
        </Box>
      )}

      {/* Empty State */}
      {!isLoading && jobs.length === 0 && (
        <Card>
          <CardContent sx={{ textAlign: 'center', py: 4 }}>
            <Storage sx={{ fontSize: 48, color: 'text.secondary', mb: 2 }} />
            <Typography variant="h6" color="text.secondary" gutterBottom>
              No SP Analysis Jobs Found
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
              Start your first stored procedure analysis to see jobs here.
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
      )}

      {/* Jobs Table */}
      {jobs.length > 0 && (
        <TableContainer component={Paper}>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>Status</TableCell>
                <TableCell>Job ID</TableCell>
                <TableCell>Environment</TableCell>
                <TableCell>Progress</TableCell>
                <TableCell>Duration</TableCell>
                <TableCell>Started</TableCell>
                <TableCell align="right">Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {jobs.map((job) => (
                <TableRow key={job.job_id} hover>
                  <TableCell>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      {getStatusIcon(job.status)}
                      <Chip
                        label={job.status}
                        color={getStatusColor(job.status)}
                        size="small"
                      />
                    </Box>
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2" fontFamily="monospace">
                      {job.job_id.slice(0, 8)}...
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Chip label={job.sf_environment} size="small" variant="outlined" />
                  </TableCell>
                  <TableCell>
                    <Box sx={{ minWidth: 120 }}>
                      {job.status === SPJobStatus.RUNNING && job.total_procedures > 0 ? (
                        <Box>
                          <LinearProgress
                            variant="determinate"
                            value={getProgressValue(job)}
                            sx={{ mb: 0.5 }}
                          />
                          <Typography variant="caption" color="text.secondary">
                            {job.completed_procedures}/{job.total_procedures}
                          </Typography>
                        </Box>
                      ) : job.status === SPJobStatus.COMPLETED ? (
                        <Typography variant="body2" color="success.main">
                          {job.total_procedures} procedures
                        </Typography>
                      ) : job.status === SPJobStatus.FAILED ? (
                        <Typography variant="body2" color="error.main">
                          Failed
                        </Typography>
                      ) : (
                        <Typography variant="body2" color="text.secondary">
                          {job.status === SPJobStatus.PENDING ? 'Starting...' : '-'}
                        </Typography>
                      )}
                    </Box>
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2">
                      {formatDuration(job.started_at, job.completed_at)}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2">
                      {new Date(job.started_at).toLocaleString()}
                    </Typography>
                  </TableCell>
                  <TableCell align="right">
                    <Box sx={{ display: 'flex', gap: 0.5 }}>
                      {job.status === SPJobStatus.COMPLETED && (
                        <>
                          <Tooltip title="View Results">
                            <IconButton
                              size="small"
                              onClick={() => handleViewResults(job)}
                            >
                              <Visibility />
                            </IconButton>
                          </Tooltip>
                          {/* <Tooltip title="Download CSV">
                            <IconButton
                              size="small"
                              onClick={() => handleDownloadResults(job.job_id)}
                            >
                              <Download />
                            </IconButton>
                          </Tooltip> */}
                        </>
                      )}
                      {/* {(job.status === SPJobStatus.PENDING || job.status === SPJobStatus.RUNNING) && (
                        <Tooltip title="Cancel Job">
                          <IconButton
                            size="small"
                            onClick={() => handleCancelJob(job.job_id)}
                            color="error"
                          >
                            <Cancel />
                          </IconButton>
                        </Tooltip>
                      )} */}
                    </Box>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
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
            <Storage color="primary" />
            <Typography variant="h6">
              Analysis Results
            </Typography>
            {selectedJob && (
              <Chip
                label={`Job: ${selectedJob.job_id.slice(0, 8)}...`}
                size="small"
                variant="outlined"
              />
            )}
          </Box>
        </DialogTitle>
        <DialogContent>
          {loadingResults ? (
            <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
              <CircularProgress />
            </Box>
          ) : jobResults ? (
            <Box>
              <Typography variant="h6" gutterBottom>
                Summary
              </Typography>
              <Box sx={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: 2, mb: 3 }}>
                <Card variant="outlined">
                  <CardContent>
                    <Typography variant="h4" color="primary">
                      {jobResults.total_procedures}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      Procedures Analyzed
                    </Typography>
                  </CardContent>
                </Card>
                <Card variant="outlined">
                  <CardContent>
                    <Typography variant="h4" color="secondary">
                      {jobResults.total_relationships}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      Relationships Found
                    </Typography>
                  </CardContent>
                </Card>
                <Card variant="outlined">
                  <CardContent>
                    <Typography variant="h4" color="success.main">
                      {jobResults.unique_tables}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      Unique Tables
                    </Typography>
                  </CardContent>
                </Card>
              </Box>
              
              {jobResults.summary.relationship_types && (
                <Box>
                  <Typography variant="h6" gutterBottom>
                    Relationship Types
                  </Typography>
                  <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
                    {Object.entries(jobResults.summary.relationship_types).map(([type, count]) => (
                      <Chip
                        key={type}
                        label={`${type}: ${count}`}
                        size="small"
                        variant="outlined"
                      />
                    ))}
                  </Box>
                </Box>
              )}
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
          {/* {jobResults && selectedJob && (
            <Button
              variant="contained"
              startIcon={<Download />}
              onClick={() => handleDownloadResults(selectedJob.job_id)}
            >
              Download CSV
            </Button>
          )} */}
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default SPAnalysisJobs;